"""ShareCRM (纷享销客) IM Gateway adapter for Hermes Agent.

对接 ShareCRM IM Gateway 的 SSE 长连接 + HTTP 开放接口：
https://open.fxiaoke.com/im-gateway/docs/bot-api.md

设计要点：
- 入站走 SSE（事件含 connected / message / reset），出站走 qixin/message/send。
- 出站接口只支持 text，但企信会渲染文本里的 Markdown，故不做 markdown 剥离。
- 本插件同时支持新旧 Hermes 运行时：``_compat`` 负责 profile-scoped 配置读取。
"""

from __future__ import annotations

import asyncio
import dataclasses
import ipaddress
import json
import logging
import mimetypes
import os
import random
import tempfile
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.config import Platform

try:  # Hermes >= 2026-09（模块拆分后）
    from gateway.platforms.event import MessageEvent, MessageType
except Exception:  # 旧布局
    from gateway.platforms.base import MessageEvent, MessageType

try:
    from gateway.platforms.base import cache_image_from_bytes
except Exception:  # pragma: no cover - 极端精简运行时
    cache_image_from_bytes = None

try:
    from gateway.platforms.helpers import MessageDeduplicator
except Exception:  # pragma: no cover - 旧运行时兜底
    class MessageDeduplicator:  # type: ignore[no-redef]
        """最小去重实现（官方 helper 不可用时的兜底）。"""

        def __init__(self, max_size: int = 2000, ttl_seconds: float = 300):
            self._seen: Dict[str, float] = {}
            self._max_size, self._ttl = max_size, ttl_seconds

        def is_duplicate(self, msg_id: str) -> bool:
            if not msg_id:
                return False
            now = time.time()
            if msg_id in self._seen and now - self._seen[msg_id] < self._ttl:
                return True
            self._seen[msg_id] = now
            if len(self._seen) > self._max_size:
                cutoff = now - self._ttl
                self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
            return False

try:
    from ._compat import (
        apply_yaml_bridge,
        extra_or_secret as _extra_or_secret,
        get_secret as _get_secret,
        seed_extra_from_env,
        send_error,
    )
except ImportError:  # 被当作顶层模块直接 import（测试/调试）
    from _compat import (  # type: ignore
        apply_yaml_bridge,
        extra_or_secret as _extra_or_secret,
        get_secret as _get_secret,
        seed_extra_from_env,
        send_error,
    )

try:
    import aiohttp
except ImportError:
    aiohttp = None

DEFAULT_BASE_URL = "https://open.fxiaoke.com"
DEFAULT_SSE_VERSION = "1.4.0"
TOKEN_BUFFER_SECONDS = 300
TOKEN_MAX_RETRIES = 3  # 首次之外最多重试 3 次（1s/2s/4s + jitter，见文档 §12.2）
TOKEN_RETRY_JITTER = 0.2
NON_RETRYABLE_AUTH_CODES = {40001, 40002, 40004, 40005}
ACK_TEXT = "👀已收到，稍后回您！"
ERR_TOKEN_INVALID = 40100
ERR_TOKEN_EXPIRED = 40101
ERR_BOT_NOT_CONNECTED = 50001
ERR_INTERNAL = 50000
MAX_INBOUND_IMAGES = 8
MAX_INBOUND_IMAGE_BYTES = 10 * 1024 * 1024
HISTORY_LIMIT = 12
HISTORY_MAX_CHARS = 4000
HISTORY_HEADER = "[Recent channel messages]"
BOT_RECONNECT_WAIT_SECONDS = 10.0
# 会话名缓存上限（chat_id -> {name, type}）：只用于显示，超出丢最早条目
CHAT_NAME_CACHE_MAX = 500


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _guess_image_mime(name: str) -> str:
    return mimetypes.guess_type(name or "image.png")[0] or "image/png"


async def _safe_json(resp) -> Optional[dict]:
    """解析响应 JSON；非 JSON（例如 5xx 返回的 HTML）返回 None 而不是抛异常。"""
    try:
        return await resp.json(content_type=None)
    except Exception:
        return None


async def _request_token(
    session, base_url: str, app_id: str, app_secret: str
) -> Tuple[Optional[str], int, str, bool]:
    """POST /auth/token，带指数退避重试。

    返回 ``(token, expires_in, error, retryable)``。仅对网络错误、5xx、408/429 与
    业务侧临时码（非 40001/40002/40004/40005）重试；账号/参数错误立即失败。
    """
    url = f"{base_url}/im-gateway/auth/token"
    payload = {"appId": app_id, "appSecret": app_secret}
    last_error, retryable = "auth failed", True
    for attempt in range(TOKEN_MAX_RETRIES + 1):
        if attempt:
            delay = (2 ** (attempt - 1)) * (1 + random.uniform(0, TOKEN_RETRY_JITTER))
            await asyncio.sleep(delay)
        try:
            async with session.post(url, json=payload) as resp:
                status = resp.status
                data = await _safe_json(resp)
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError if aiohttp else Exception, asyncio.TimeoutError) as exc:
            last_error, retryable = str(exc), True
            logger.warning("ShareCRM: auth network error (attempt %d/%d): %s", attempt + 1, TOKEN_MAX_RETRIES + 1, exc)
            continue
        if data is None:
            last_error = f"HTTP {status}"
            retryable = status >= 500 or status in (408, 429)
            logger.warning("ShareCRM: auth non-JSON response HTTP %s (attempt %d/%d)", status, attempt + 1, TOKEN_MAX_RETRIES + 1)
            if not retryable:
                return None, 0, last_error, False
            continue
        code = data.get("code")
        if code == 0:
            td = data.get("data") or {}
            return str(td.get("accessToken") or ""), _coerce_int(td.get("expiresIn"), 7200), "", False
        last_error = str(data.get("msg") or f"code {code}")
        if code in NON_RETRYABLE_AUTH_CODES:
            return None, 0, last_error, False
        logger.warning("ShareCRM: auth transient code=%s msg=%s (attempt %d/%d)", code, last_error, attempt + 1, TOKEN_MAX_RETRIES + 1)
    return None, 0, last_error, retryable


async def _post_text(
    session, base_url: str, token: str, chat_id: str, text: str, reply_message_id: Any = None,
) -> Tuple[bool, Optional[str], int, str]:
    """POST /qixin/message/send。

    返回 ``(ok, message_id, code, error)``；``code`` 为业务码（0 成功，-1 网络/非 JSON）。
    """
    url = f"{base_url}/im-gateway/qixin/message/send"
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_message_id not in (None, "", 0):
        try:
            payload["reply_message_id"] = int(reply_message_id)
        except (ValueError, TypeError):
            payload["reply_message_id"] = reply_message_id
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            status = resp.status
            data = await _safe_json(resp)
    except asyncio.CancelledError:
        raise
    except (aiohttp.ClientError if aiohttp else Exception, asyncio.TimeoutError) as exc:
        return False, None, -1, str(exc)
    if data is None:
        return False, None, -1, f"HTTP {status}"
    code = data.get("code", -1)
    if code == 0:
        return True, str((data.get("data") or {}).get("message_id") or ""), 0, ""
    return False, None, _coerce_int(code, -1), str(data.get("msg") or f"Error {code}")


class ShareCRMAdapter(BasePlatformAdapter):

    SUPPORTS_MESSAGE_EDITING = False

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("sharecrm"))
        extra = getattr(config, "extra", {}) or {}

        self.app_id = str(_extra_or_secret(extra, "app_id", "SHARECRM_APP_ID") or "")
        self.app_secret = str(_extra_or_secret(extra, "app_secret", "SHARECRM_APP_SECRET") or "")
        self.base_url = str(
            _extra_or_secret(extra, "base_url", "SHARECRM_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL
        ).rstrip("/")
        self.sse_version = str(
            _extra_or_secret(extra, "sse_version", "SHARECRM_SSE_VERSION", DEFAULT_SSE_VERSION) or DEFAULT_SSE_VERSION
        )
        self.max_message_length = _coerce_int(
            _extra_or_secret(extra, "max_message_length", "SHARECRM_MAX_MESSAGE_LENGTH", 4096), 4096
        )
        # base 的分片逻辑读类属性 MAX_MESSAGE_LENGTH；实例属性可覆盖以支持配置。
        self.MAX_MESSAGE_LENGTH = self.max_message_length
        self.include_history = bool(_extra_or_secret(extra, "include_history", "SHARECRM_INCLUDE_HISTORY", True))
        # 群聊 @机器人 的显示名（逗号分隔），用于剥掉命令前缀；留空则按通用 "@token" 兜底
        self.mention_names = [
            n.strip().lstrip("@＠")
            for n in str(_extra_or_secret(extra, "mention_names", "SHARECRM_MENTION_NAMES", "") or "").split(",")
            if n.strip().lstrip("@＠")
        ]

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._bot_full_id: Optional[str] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._last_event_id: Optional[str] = None
        self._client_session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        # SSE 是否在线：供 50001（Bot 未在线）时等待链路恢复后重试
        self._sse_online = asyncio.Event()
        # 服务端 connected 事件下发的重连建议 / 连接寿命
        self._server_retry_ms: Optional[int] = None
        self._max_lifetime_ms: Optional[int] = None
        self._dedup = MessageDeduplicator()
        # 官方 cache helper 不可用时写下的临时图片，disconnect 时清理
        self._temp_files: set = set()
        # 会话可读名缓存（chat_id -> {"name","type"}）：sharecrm 的 chat_id 是不透明
        # uuid，会话列表/handoff 需要可读名；只存显示信息，有界。
        self._chat_meta: Dict[str, Dict[str, Any]] = {}
        # history 增量水位：chat_id -> 本插件最近一条出站消息的时间戳(ms)。
        # 只注入「自己上次发言之后」的群聊历史，避免每轮重发整窗口导致 transcript 重复累积。
        self._last_self_ts: Dict[str, float] = {}

    @property
    def name(self) -> str:
        return "ShareCRM"

    # ── connect / disconnect ────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False, **kwargs) -> bool:
        if not aiohttp:
            self._set_fatal_error("missing_deps", "pip install aiohttp", retryable=False)
            return False
        if not self.app_id or not self.app_secret:
            self._set_fatal_error("config_missing", "SHARECRM_APP_ID and SHARECRM_APP_SECRET required", retryable=False)
            return False

        connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
        self._client_session = aiohttp.ClientSession(
            connector=connector, timeout=aiohttp.ClientTimeout(total=60, connect=15)
        )

        if not await self._refresh_token():
            await self._client_session.close()
            self._client_session = None
            return False

        self._stop_event.clear()
        self._connected_event.clear()
        self._sse_online.clear()
        self._sse_task = asyncio.create_task(self._sse_loop())

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("ShareCRM: SSE connection timed out")
            await self.disconnect()
            self._set_fatal_error("sse_timeout", "No connected event", retryable=True)
            return False

        self._mark_connected()
        logger.info("ShareCRM: connected as %s", self._bot_full_id)
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        self._stop_event.set()
        self._sse_online.clear()
        if self._sse_task and not self._sse_task.done():
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
        if self._client_session and not self._client_session.closed:
            await self._client_session.close()
        self._sse_task = None
        self._client_session = None
        self._access_token = None
        self._token_expires_at = 0.0
        self._bot_full_id = None
        self._cleanup_temp_files()

    def _cleanup_temp_files(self) -> None:
        for path in list(self._temp_files):
            try:
                os.remove(path)
            except OSError:
                pass
        self._temp_files.clear()

    # ── token ────────────────────────────────────────────────────────────

    async def _refresh_token(self) -> bool:
        if not self._client_session:
            return False
        token, expires_in, error, retryable = await _request_token(
            self._client_session, self.base_url, self.app_id, self.app_secret
        )
        if token:
            self._access_token = token
            self._token_expires_at = time.time() + max(expires_in - TOKEN_BUFFER_SECONDS, 60)
            return True
        self._set_fatal_error("auth_failed", error or "Auth failed", retryable=retryable)
        logger.error("ShareCRM: auth failed — %s", error)
        return False

    async def _ensure_token(self) -> bool:
        if not self._access_token or time.time() >= self._token_expires_at:
            return await self._refresh_token()
        return True

    # ── SSE ──────────────────────────────────────────────────────────────

    def _server_retry_seconds(self, default: float) -> float:
        """重连等待：优先遵循服务端下发的 retry（毫秒），否则用 fallback。"""
        if self._server_retry_ms:
            return max(0.2, min(self._server_retry_ms / 1000.0, 60.0))
        return default

    async def _sse_loop(self) -> None:
        fallback = 1.0
        while not self._stop_event.is_set():
            if not await self._ensure_token():
                await self._wait(5.0)
                continue
            url = f"{self.base_url}/im-gateway/bot/events?token={self._access_token}&version={self.sse_version}"
            headers = {"Accept": "text/event-stream"}
            if self._last_event_id:
                headers["Last-Event-ID"] = self._last_event_id
            reason = "eof"
            self._sse_online.clear()
            # SSE 是长连接：不能用会按总时长计时的 total 超时，否则会被客户端
            # 按时长掐断。用 sock_read 兜底空闲（服务端有心跳，正常不会触发）。
            sse_timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=120)
            try:
                async with self._client_session.get(url, headers=headers, timeout=sse_timeout) as resp:
                    if resp.status == 401:
                        # 连接即鉴权：401 先刷新 token 再重连（文档 §12.3）
                        self._access_token = None
                        await self._wait(1.0)
                        continue
                    if resp.status != 200:
                        logger.error("ShareCRM: SSE HTTP %s", resp.status)
                        await self._wait(fallback)
                        fallback = min(fallback * 2, 60.0)
                        continue
                    fallback = 1.0
                    reason = await self._read_stream(resp)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError if aiohttp else Exception, asyncio.TimeoutError) as exc:
                logger.warning("ShareCRM: SSE connection error — %s", exc)
                await self._wait(fallback)
                fallback = min(fallback * 2, 60.0)
                continue
            except Exception as exc:
                # 服务端可能在 max_lifetime 到期后正常关闭流，这里按普通断开处理
                logger.debug("ShareCRM: SSE stream ended — %s", exc)
                await self._wait(fallback)
                fallback = min(fallback * 2, 60.0)
                continue
            finally:
                self._sse_online.clear()

            if reason == "stopped":
                break
            if reason == "reset":
                # 游标失效：清空后立即重连，并告警（文档 §6.4）
                self._last_event_id = None
                await self._wait(0.2)
                continue
            # 正常断开（含 max_lifetime 到期）：按服务端 retry 快速重连
            await self._wait(self._server_retry_seconds(1.0))

    async def _read_stream(self, resp) -> str:
        """解析 SSE 流，返回结束原因：``eof`` / ``reset`` / ``stopped``。"""
        ev_type = ev_id = ""
        data_lines: List[str] = []
        async for raw in resp.content:
            if self._stop_event.is_set():
                return "stopped"
            try:
                line = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            except UnicodeDecodeError:
                continue
            line = line.rstrip("\r\n")
            if not line:
                if data_lines:
                    signal = await self._dispatch(ev_type, ev_id, "\n".join(data_lines))
                    if ev_id:
                        self._last_event_id = ev_id
                    ev_type, ev_id, data_lines = "", "", []
                    if signal:
                        return signal
                continue
            if line.startswith(":"):
                continue  # SSE comment 心跳
            if line.startswith("event:"):
                ev_type = line[6:].strip()
            elif line.startswith("id:"):
                ev_id = line[3:].strip()
            elif line.startswith("data:"):
                # SSE 规范：多行 data 以换行拼接；去掉一个前导空格
                data_lines.append(line[5:].lstrip(" "))
            elif line.startswith("retry:"):
                try:
                    self._server_retry_ms = int(line[6:].strip())
                except ValueError:
                    pass
        if data_lines:
            signal = await self._dispatch(ev_type, ev_id, "\n".join(data_lines))
            if ev_id:
                self._last_event_id = ev_id
            if signal:
                return signal
        return "eof"

    async def _dispatch(self, ev_type: str, ev_id: str, data: str) -> Optional[str]:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return None
        t = payload.get("type", ev_type)
        if t == "connected":
            d = payload.get("data") or {}
            self._bot_full_id = d.get("bot_full_id", "")
            retry = d.get("retry")
            if isinstance(retry, (int, float)) and retry > 0:
                self._server_retry_ms = int(retry)
            lifetime = d.get("max_lifetime")
            if isinstance(lifetime, (int, float)) and lifetime > 0:
                self._max_lifetime_ms = int(lifetime)
            self._sse_online.set()
            self._connected_event.set()
            logger.info(
                "ShareCRM: connected bot=%s retry=%sms max_lifetime=%sms",
                self._bot_full_id, self._server_retry_ms, self._max_lifetime_ms,
            )
        elif t == "message":
            await self._handle_message(payload)
        elif t == "reset":
            logger.warning("ShareCRM: SSE reset reason=%s, reconnecting", (payload.get("data") or {}).get("reason") or payload.get("reason"))
            return "reset"
        return None

    # ── inbound ──────────────────────────────────────────────────────────

    async def _handle_message(self, payload: dict) -> None:
        d = payload.get("data") or {}
        if not d:
            return

        message_id = str(d.get("message_id") or "")
        if self._dedup.is_duplicate(message_id):
            logger.debug("ShareCRM: duplicate message %s dropped", message_id)
            return

        chat_id = d.get("chat_id", "")
        raw_chat_type = (d.get("chat_type") or "direct").strip().lower()
        # Hermes 的 pairing / DM 鉴权只在 chat_type == "dm" 时触发；
        # ShareCRM 用 "direct" 表示 1:1 会话。
        if raw_chat_type in {"dm", "direct", "private", "c2c", ""}:
            chat_type = "dm"
        else:
            chat_type = raw_chat_type
        sender = d.get("from") or {}
        raw_id = sender.get("id", "")
        ea = d.get("ea", "")
        user_id = raw_id if raw_id.startswith("E.") else (f"E.{ea}.{raw_id}" if ea and raw_id else raw_id)
        user_name = sender.get("name", raw_id)
        # chat_id 是不透明 uuid，换成可读名再交给 gateway（session 列表/handoff 用）
        chat_name = self._resolve_chat_name(chat_id, chat_type, user_name, user_id)
        self._remember_chat(chat_id, chat_name, chat_type)

        msg = d.get("message") or {}
        caption = (msg.get("content") or "").strip() or (d.get("text") or "")
        # 企信群聊必须 @ 机器人，文本形如 "@二哈 /new"；剥掉开头提及，
        # 否则 /new、/reset 等命令不在 char 0，gateway 识别不到。私聊不处理。
        if chat_type != "dm":
            caption = self._strip_mention(caption)
        staged = await self._stage_images(msg.get("images") or [])
        image_lines = [f"![{name}]({path})" for path, name in staged]
        text = "\n".join(part for part in [caption, *image_lines] if part)
        reply_to_id = d.get("reply_message_id")
        history = d.get("history_messages") or []

        # 历史上下文（官方 channel_context 路径）：
        # - 只在非私聊注入（私聊每条都触发，无需 backfill；对齐 Discord/Slack/Relay）
        # - 只注入「自己上次发言之后」的增量（watermark），避免每轮重发整窗口
        # - 必须在发 ACK 之前取水位，否则 ACK 会把自己刚推进的时间戳当成水位
        channel_context = ""
        if self.include_history and chat_type != "dm":
            since_ms = self._last_self_ts.get(chat_id)
            channel_context = self._format_history(history, since_ms=since_ms)
        reply_text = self._find_reply_text(history, reply_to_id)

        # 拦截 /sethome，用官方 persist_home_channel 落库（不再直写 os.environ）
        if caption.strip() == "/sethome":
            self._set_home_channel(chat_id)
            await self.send(chat_id, f"已将当前会话 {chat_id} 设置为 Home Channel。")
            return

        # 首个私聊静默设为 home，避免 Hermes 注入 "No home channel" 引导
        if chat_type == "dm" and chat_id and not self._home_channel_set():
            self._set_home_channel(chat_id)

        if chat_id and (caption or staged):
            try:
                await self._do_send(chat_id, ACK_TEXT)
            except Exception as exc:
                logger.debug("ShareCRM: ack send failed: %s", exc)

        source = self.build_source(
            chat_id=chat_id, chat_name=chat_name, chat_type=chat_type,
            user_id=user_id, user_name=user_name,
        )
        event_kwargs: Dict[str, Any] = {
            "text": text,
            "message_type": MessageType.PHOTO if staged and not caption else MessageType.TEXT,
            "source": source,
            "message_id": message_id or None,
            "reply_to_message_id": str(reply_to_id) if reply_to_id else None,
            "reply_to_text": reply_text,
            "timestamp": self._event_time(d),
            "channel_context": channel_context or None,
        }
        if staged:
            event_kwargs["media_urls"] = [path for path, _ in staged]
            event_kwargs["media_types"] = [_guess_image_mime(name) for _, name in staged]
        event = self._build_event(event_kwargs)
        await self.handle_message(event)

    @staticmethod
    def _event_time(d: dict) -> datetime:
        ts = d.get("timestamp") or d.get("date")
        if isinstance(ts, (int, float)) and ts > 0:
            try:
                return datetime.fromtimestamp(ts)
            except (OverflowError, OSError, ValueError):
                pass
        return datetime.now()

    @staticmethod
    def _build_event(kwargs: Dict[str, Any]) -> MessageEvent:
        """按当前 MessageEvent 的字段裁剪 kwargs，兼容不同版本的字段差异。"""
        try:
            allowed = {f.name for f in dataclasses.fields(MessageEvent)}
            kwargs = {k: v for k, v in kwargs.items() if k in allowed}
        except TypeError:
            pass
        try:
            return MessageEvent(**kwargs)
        except TypeError:
            # 极端旧运行时：丢弃可选字段再试
            for key in ("channel_context", "media_urls", "media_types", "reply_to_text"):
                kwargs.pop(key, None)
            return MessageEvent(**kwargs)

    @staticmethod
    def _short_sender(value: str) -> str:
        value = (value or "").strip()
        if not value:
            return "?"
        return value.rsplit(".", 1)[-1] if value.startswith("E.") else value

    @staticmethod
    def _is_mention_boundary(ch: str) -> bool:
        """提及 token 的结束边界：空白、结尾或常见标点。"""
        return not ch or ch.isspace() or ch in ",.;:!?，。；：！？、()（）[]【】{}<>\"'“”‘’"

    @classmethod
    def _remove_mention_token(cls, text: str, token: str) -> str:
        """删除所有作为独立 token 出现的 ``@名字``（后接边界/结尾），避免误伤 ``@名字xyz``。"""
        if not text or not token:
            return text
        out: List[str] = []
        i, m, n = 0, len(text), len(token)
        while i < m:
            if text.startswith(token, i) and cls._is_mention_boundary(text[i + n] if i + n < m else ""):
                out.append(" ")
                i += n
                continue
            out.append(text[i])
            i += 1
        return "".join(out)

    @staticmethod
    def _collapse_ws(text: str) -> str:
        """折叠空白：行内多空格/制表符压成单个空格，去首尾空白，保留换行。"""
        return "\n".join(" ".join(line.split()) for line in text.split("\n")).strip()

    def _strip_mention(self, text: str) -> str:
        """去掉群聊里的 @机器人（开头/结尾/中间都处理），让 ``@二哈 /new`` 这类命令生效。

        - 配置了 ``SHARECRM_MENTION_NAMES``：删除所有 ``@<名字>`` 出现处（最长名优先，避免短名误吃长名）
        - 未配置：只剥掉首/尾的通用 ``@token``，不动中间（避免误删 @其他同事）
        - 剥完为空（消息只有 @机器人）时保持原样
        """
        if not text or ("@" not in text and "＠" not in text):
            return text
        if self.mention_names:
            result = text
            for name in sorted(self.mention_names, key=len, reverse=True):
                for prefix in ("@", "＠"):
                    result = self._remove_mention_token(result, prefix + name)
            return self._collapse_ws(result) or text
        # 未配置名字：只处理首尾 token，中间的原样保留
        tokens = text.split()
        n = len(tokens)
        start, end = 0, n
        while start < end and tokens[start].startswith(("@", "＠")) and len(tokens[start]) > 1:
            start += 1
        while end > start and tokens[end - 1].startswith(("@", "＠")) and len(tokens[end - 1]) > 1:
            end -= 1
        if start == 0 and end == n:
            return text
        return self._collapse_ws(" ".join(tokens[start:end])) or text

    def _format_history(self, history: List[Any], *, since_ms: Optional[float] = None) -> str:
        """把 history_messages 整理成官方风格的只读上下文块。

        - 只保留 `message_timestamp` 晚于 ``since_ms``（本插件上次出站时间）的增量
        - 按时间升序，取最近 ``HISTORY_LIMIT`` 条，限总长
        - 渲染为 ``[Recent channel messages]`` + ``[sender] content``
        没有可用增量时返回 ""（channel_context 保持未设置）。
        """
        if not history:
            return ""
        rows: List[Tuple[float, str, str]] = []
        for item in history:
            if not isinstance(item, dict):
                continue
            try:
                ts = float(item.get("message_timestamp"))
            except (TypeError, ValueError):
                continue  # 无时间戳无法做增量/排序，跳过
            if since_ms is not None and ts <= since_ms:
                continue
            content = str(item.get("content") or "").strip()
            if not content:
                continue
            sender_raw = str(item.get("full_sender_id") or item.get("sender_id") or "")
            if sender_raw.startswith("BOT."):
                continue  # 机器人自己嘅回复已在 transcript，勿当历史重复注入
            sender = self._short_sender(sender_raw)
            rows.append((ts, sender, content))
        if not rows:
            return ""
        rows.sort(key=lambda row: row[0])
        lines = [f"[{sender}] {content}" for _ts, sender, content in rows[-HISTORY_LIMIT:]]
        text = "\n".join(lines)
        if len(text) > HISTORY_MAX_CHARS:
            text = text[-HISTORY_MAX_CHARS:]
        return f"{HISTORY_HEADER}\n{text}"

    @staticmethod
    def _find_reply_text(history: List[Any], reply_to_id: Any) -> Optional[str]:
        if not reply_to_id or not history:
            return None
        target = str(reply_to_id)
        for item in history:
            if isinstance(item, dict) and str(item.get("message_id", "")) == target:
                return item.get("content") or None
        return None

    async def _stage_images(self, images: List[Any]) -> List[Tuple[str, str]]:
        staged: List[Tuple[str, str]] = []
        if not self._client_session or not images:
            return staged
        for image in images[:MAX_INBOUND_IMAGES]:
            if not isinstance(image, dict):
                continue
            url = str(image.get("url") or "").strip()
            if not url:
                continue
            if not self._is_public_image_url(url):
                logger.warning("ShareCRM: skip inbound image from non-public host")
                continue
            name = os.path.basename(str(image.get("filename") or "image.png").replace("\\", "/")) or "image.png"
            try:
                async with self._client_session.get(
                    url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True
                ) as resp:
                    if resp.status >= 400:
                        logger.warning("ShareCRM: inbound image download failed status=%s", resp.status)
                        continue
                    data = await resp.read()
            except Exception as exc:
                logger.warning("ShareCRM: inbound image download failed: %s", exc)
                continue
            if not data or len(data) > MAX_INBOUND_IMAGE_BYTES:
                logger.warning("ShareCRM: inbound image empty or too large")
                continue
            path = await self._cache_image(data, name)
            if path:
                staged.append((path, name))
        return staged

    async def _cache_image(self, data: bytes, name: str) -> Optional[str]:
        """优先用官方 cache helper（自带 TTL 清理），不可用时写临时文件并登记清理。"""
        ext = os.path.splitext(name)[1].lower() or ".png"
        if cache_image_from_bytes is not None:
            try:
                return await asyncio.to_thread(cache_image_from_bytes, data, ext)
            except Exception as exc:
                logger.warning("ShareCRM: cache image failed: %s", exc)
                return None
        try:
            fd, path = tempfile.mkstemp(prefix="sharecrm-image-", suffix=ext)
            os.close(fd)
            with open(path, "wb") as fh:
                fh.write(data)
            self._temp_files.add(path)
            return path
        except OSError as exc:
            logger.warning("ShareCRM: temp image write failed: %s", exc)
            return None

    @staticmethod
    def _is_public_image_url(url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http") or parsed.username or parsed.password:
            return False
        host = (parsed.hostname or "").lower()
        if not host or host in {"localhost", "metadata.google.internal"} or host.endswith((".localhost", ".local", ".internal")):
            return False
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
                return False
        except ValueError:
            pass
        return True

    async def _wait(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def _wait_online(self, timeout: float) -> bool:
        """等待 SSE 链路恢复在线（用于 50001 后重试）。"""
        if self._sse_online.is_set():
            return True
        try:
            await asyncio.wait_for(self._sse_online.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # ── send ─────────────────────────────────────────────────────────────

    def format_message(self, content: str) -> str:
        # 企信会渲染文本中的 Markdown，出站直接原样发送，不做剥离。
        return content

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        r = await self._do_send(chat_id, content, reply_to)
        if r.get("success"):
            return SendResult(success=True, message_id=r.get("message_id", ""))
        return SendResult(success=False, error=r.get("error", ""), retryable=r.get("retryable", False))

    async def _do_send(self, chat_id: str, text: str, reply_to: Optional[str] = None,
                       *, allow_retry: bool = True) -> dict:
        if not self._client_session or not await self._ensure_token():
            return {"success": False, "error": "Not connected", "retryable": True}

        ok, message_id, code, error = await _post_text(
            self._client_session, self.base_url, self._access_token or "", chat_id, text, reply_to
        )
        if ok:
            # 记录自己最近一条出站消息时间，作为下次 group history 的增量水位
            self._remember_self_ts(chat_id)
            return {"success": True, "message_id": message_id or ""}

        if code in (ERR_TOKEN_INVALID, ERR_TOKEN_EXPIRED) and allow_retry:
            self._access_token = None
            if await self._refresh_token():
                return await self._do_send(chat_id, text, reply_to, allow_retry=False)
            return {"success": False, "error": error or "Token error", "retryable": True}

        if code == ERR_BOT_NOT_CONNECTED and allow_retry:
            # 50001 表示未投递：等 SSE 恢复后重试一次（文档 §12.4）
            if await self._wait_online(BOT_RECONNECT_WAIT_SECONDS):
                return await self._do_send(chat_id, text, reply_to, allow_retry=False)
            return {"success": False, "error": error or "Bot not connected", "retryable": True}

        retryable = code == -1 or code >= ERR_INTERNAL
        return {"success": False, "error": error or f"Error {code}", "retryable": retryable}

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        pass

    # ── home channel ─────────────────────────────────────────────────────

    def _home_channel_set(self) -> bool:
        home = getattr(self.config, "home_channel", None)
        if home is not None and getattr(home, "chat_id", ""):
            return True
        return bool(str(_get_secret("SHARECRM_HOME_CHANNEL", "") or "").strip())

    def _set_home_channel(self, chat_id: str) -> None:
        """用官方 persist_home_channel 落库，multiplex 安全。"""
        if not chat_id:
            return
        try:
            from gateway.config import HomeChannel, persist_home_channel

            name = (self._chat_meta.get(chat_id) or {}).get("name") or chat_id
            home = HomeChannel(platform=self.platform, chat_id=chat_id, name=name)
            persist_home_channel(home, enabled_if_new=True)
            try:
                self.config.home_channel = home
            except Exception:
                pass
        except Exception as exc:
            logger.debug("ShareCRM: persist home channel failed: %s", exc)

    # ── chat naming ──────────────────────────────────────────────────────

    @staticmethod
    def _short_chat_id(chat_id: str) -> str:
        """从 `{env}:{ea}:{sessionId}:{parent}` 取可读的短会话段。"""
        parts = str(chat_id or "").split(":")
        session = parts[2] if len(parts) >= 3 else ""
        return (session or str(chat_id or ""))[:8]

    def _resolve_chat_name(self, chat_id: str, chat_type: str, user_name: str, user_id: str) -> str:
        """给会话一个可读名。

        ShareCRM 的 chat_id 是不透明 uuid，会话列表/handoff 直接显示会很难认：
        - 私聊：``私聊 <发送者显示名>``，退化到 user_id
        - 群聊：API 不提供群名，用 ``群聊 <短 session 段>``
        """
        if chat_type == "dm":
            name = str(user_name or "").strip()
            if not name or name == chat_id:
                name = str(user_id or chat_id)
            return f"私聊 {name}" if name else chat_id
        short = self._short_chat_id(chat_id)
        return f"群聊 {short}" if short else chat_id

    def _remember_chat(self, chat_id: str, name: str, chat_type: str) -> None:
        """记录 chat_id 的显示名与类型，供 get_chat_info / handoff 使用（有界）。"""
        if not chat_id:
            return
        self._chat_meta[chat_id] = {"name": name or chat_id, "type": chat_type or "dm"}
        overflow = len(self._chat_meta) - CHAT_NAME_CACHE_MAX
        if overflow > 0:
            for key in list(self._chat_meta.keys())[:overflow]:
                self._chat_meta.pop(key, None)

    def _remember_self_ts(self, chat_id: str) -> None:
        """记录本插件在该会话最近一条出站消息的时间戳(ms)，作为 history 增量水位（有界）。"""
        if not chat_id:
            return
        self._last_self_ts[chat_id] = time.time() * 1000.0
        overflow = len(self._last_self_ts) - CHAT_NAME_CACHE_MAX
        if overflow > 0:
            for key in list(self._last_self_ts.keys())[:overflow]:
                self._last_self_ts.pop(key, None)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """返回 {name, type, chat_id}；type 遵循 Hermes 约定（dm/group/channel）。"""
        meta = self._chat_meta.get(chat_id)
        if meta:
            return {
                "name": meta.get("name") or chat_id,
                "type": meta.get("type") or "dm",
                "chat_id": chat_id,
            }
        # 没见过该会话：chat_id 无法区分 dm/group，按最常见的私聊兜底，名字用短段。
        return {"name": self._short_chat_id(chat_id), "type": "dm", "chat_id": chat_id}


# ── plugin hooks ────────────────────────────────────────────────────────


def check_requirements() -> bool:
    """被动依赖探测：aiohttp 可用且凭证已配置。"""
    if aiohttp is None:
        return False
    return bool(
        str(_get_secret("SHARECRM_APP_ID", "") or "").strip()
        and str(_get_secret("SHARECRM_APP_SECRET", "") or "").strip()
    )


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(
        str(_extra_or_secret(extra, "app_id", "SHARECRM_APP_ID") or "").strip()
        and str(_extra_or_secret(extra, "app_secret", "SHARECRM_APP_SECRET") or "").strip()
    )


def is_connected(config) -> bool:
    return validate_config(config)


# env → extra 播种表（注意：必须在 _env_enablement 使用前定义）
_ENV_SEED_SPEC = (
    ("SHARECRM_APP_ID", "app_id", None),
    ("SHARECRM_APP_SECRET", "app_secret", None),
    ("SHARECRM_BASE_URL", "base_url", None),
    ("SHARECRM_SSE_VERSION", "sse_version", None),
    ("SHARECRM_MAX_MESSAGE_LENGTH", "max_message_length", _coerce_int),
    ("SHARECRM_MENTION_NAMES", "mention_names", None),
)

# YAML → env 桥：(yaml_key, ENV_VAR, kind)
_YAML_SPEC = (
    ("app_id", "SHARECRM_APP_ID", "str"),
    ("app_secret", "SHARECRM_APP_SECRET", "str"),
    ("base_url", "SHARECRM_BASE_URL", "str"),
    ("sse_version", "SHARECRM_SSE_VERSION", "str"),
    ("max_message_length", "SHARECRM_MAX_MESSAGE_LENGTH", "str"),
    ("mention_names", "SHARECRM_MENTION_NAMES", "str"),
    ("allowed_users", "SHARECRM_ALLOWED_USERS", "csv"),
    ("allow_all_users", "SHARECRM_ALLOW_ALL_USERS", "lower"),
    ("home_channel", "SHARECRM_HOME_CHANNEL", "str"),
)


def _env_enablement() -> Optional[dict]:
    """env_enablement_fn：构造 adapter 前先用 env 播种 extra（让 status 反映纯 env 配置）。"""
    seed = seed_extra_from_env(_ENV_SEED_SPEC, home_env="SHARECRM_HOME_CHANNEL")
    if not seed.get("app_id") or not seed.get("app_secret"):
        return None
    seed.setdefault("base_url", DEFAULT_BASE_URL)
    return seed


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
    """apply_yaml_config_fn：把 config.yaml 的 sharecrm 段翻译成 env + extra。"""
    return apply_yaml_bridge(platform_cfg or {}, _YAML_SPEC)


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """进程外投递（cron / send_message）：自己取 token、发一条、关闭，不依赖 live adapter。

    签名必须与 ``PlatformEntry.standalone_sender_fn`` 契约一致：
    ``async (pconfig, chat_id, message, *, thread_id=None, media_files=None, force_document=False)``。
    企信出站只支持 text，``media_files`` / ``force_document`` 仅为签名兼容。
    """
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return send_error("ShareCRM standalone send: chat_id is required")
    if any(ch in chat_id for ch in "\r\n"):
        return send_error("ShareCRM standalone send: chat_id contains control characters")

    extra = getattr(pconfig, "extra", {}) or {}
    app_id = str(_extra_or_secret(extra, "app_id", "SHARECRM_APP_ID") or "")
    app_secret = str(_extra_or_secret(extra, "app_secret", "SHARECRM_APP_SECRET") or "")
    base_url = str(
        _extra_or_secret(extra, "base_url", "SHARECRM_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL
    ).rstrip("/")
    if not app_id or not app_secret:
        return send_error("ShareCRM standalone send: SHARECRM_APP_ID / SHARECRM_APP_SECRET not configured")
    if aiohttp is None:
        return send_error("ShareCRM standalone send: aiohttp not installed")

    try:
        connector = aiohttp.TCPConnector(limit=2, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            token, _expires_in, error, _retryable = await _request_token(session, base_url, app_id, app_secret)
            if not token:
                return send_error(f"ShareCRM standalone send: auth failed: {error}")
            ok, message_id, _code, error = await _post_text(session, base_url, token, chat_id, message, None)
            if not ok:
                return send_error(f"ShareCRM standalone send: {error}")
            return {"success": True, "message_id": message_id or ""}
    except Exception as exc:
        logger.debug("ShareCRM standalone send raised", exc_info=True)
        return send_error(f"ShareCRM standalone send failed: {exc}")


_DASHBOARD_ENV = (
    {
        "name": "SHARECRM_BASE_URL",
        "description": f"接口地址，留空则使用 {DEFAULT_BASE_URL}",
        "prompt": f"Base URL ({DEFAULT_BASE_URL})",
        "help": f"默认 {DEFAULT_BASE_URL}",
    },
    {
        "name": "SHARECRM_SSE_VERSION",
        "description": f"SSE 协议版本，默认 {DEFAULT_SSE_VERSION}；<1.4.0 收不到图片/图文",
        "prompt": f"SSE version ({DEFAULT_SSE_VERSION})",
        "help": "建议保持默认 1.4.0。",
    },
    {
        "name": "SHARECRM_MAX_MESSAGE_LENGTH",
        "description": "单条消息最大长度，默认 4096",
        "prompt": "Max message length (4096)",
        "help": "超长回复会被自动分片。",
    },
    {
        "name": "SHARECRM_INCLUDE_HISTORY",
        "description": "是否把群聊 history_messages 作为上下文注入，默认 true",
        "prompt": "Include history (true/false)",
        "help": "false 时不注入历史消息上下文。",
    },
    {
        "name": "SHARECRM_MENTION_NAMES",
        "description": "群聊 @机器人 的显示名，逗号分隔（如 二哈）。用于剥掉命令前缀，让 @二哈 /new 生效",
        "prompt": "Bot mention names",
        "help": "留空则按通用 @token 兜底剥离。",
    },
    {
        "name": "SHARECRM_ALLOWED_USERS",
        "description": "允许的用户 ID，逗号分隔（如 E.82846.1230）。留空则私聊走 pairing",
        "prompt": "Allowed users",
        "help": "完整用户 ID，多个用逗号分隔。未列出的用户私聊会收到配对码。",
    },
    {
        "name": "SHARECRM_ALLOW_ALL_USERS",
        "description": "设为 true 允许所有用户（仅开发测试）",
        "prompt": "Allow all users (true/false)",
        "help": "true 时跳过允许列表和 pairing。生产环境不要开。",
    },
    {
        "name": "SHARECRM_HOME_CHANNEL",
        "description": "定时通知投递目标 chat_id；也可在会话里发 /sethome",
        "prompt": "Home channel",
        "help": "企信 chat_id。不知道可留空，私聊 Bot 发 /sethome。",
    },
)
_DASHBOARD_FORCE_VISIBLE = {item["name"] for item in _DASHBOARD_ENV}


def _register_dashboard_env() -> None:
    """Surface ShareCRM optional env vars on the Dashboard Channels form.

    Hermes only auto-injects optional_env from *bundled* platform plugins, and
    also hides ``*_ALLOW_ALL_USERS`` / ``*_HOME_CHANNEL`` in setup UI. Community
    plugins therefore only showed required credentials unless we inject here.
    """
    for modname in ("hermes_cli.config_defaults", "hermes_cli.config"):
        try:
            mod = __import__(modname, fromlist=["OPTIONAL_ENV_VARS"])
            env = getattr(mod, "OPTIONAL_ENV_VARS", None)
            if not isinstance(env, dict):
                continue
            for item in _DASHBOARD_ENV:
                env.setdefault(
                    item["name"],
                    {
                        "description": item["description"],
                        "prompt": item["prompt"],
                        "help": item["help"],
                        "url": None,
                        "password": False,
                        "category": "messaging",
                    },
                )
        except Exception:
            continue

    def _visible(name: str, orig) -> bool:
        if name in _DASHBOARD_FORCE_VISIBLE:
            return False
        return orig(name)

    for modname in ("hermes_cli.setup_hidden_env", "hermes_cli.web_server"):
        try:
            mod = __import__(modname, fromlist=["is_setup_hidden_env"])
            attr = "is_setup_hidden_env"
            orig = getattr(mod, attr, None)
            if orig is None:
                attr = "_is_setup_hidden_env"
                orig = getattr(mod, attr, None)
            if orig is None or getattr(orig, "_sharecrm_patched", False):
                continue
            wrapped = lambda name, _orig=orig: _visible(name, _orig)
            wrapped._sharecrm_patched = True  # type: ignore[attr-defined]
            setattr(mod, attr, wrapped)
        except Exception:
            continue


def interactive_setup() -> None:
    from hermes_cli.setup import prompt, prompt_yes_no, save_env_value, get_env_value
    from hermes_cli.setup import print_header, print_info, print_warning, print_success

    print_header("纷享销客 ShareCRM")
    if get_env_value("SHARECRM_APP_ID"):
        print_info("已配置。")
        if not prompt_yes_no("重新配置?", False):
            return

    aid = prompt("App ID")
    if not aid:
        return print_warning("已跳过")
    save_env_value("SHARECRM_APP_ID", aid.strip())

    sec = prompt("App Secret", password=True)
    if not sec:
        return print_warning("已跳过")
    save_env_value("SHARECRM_APP_SECRET", sec.strip())

    url = prompt("Base URL", default=get_env_value("SHARECRM_BASE_URL") or DEFAULT_BASE_URL)
    if url:
        save_env_value("SHARECRM_BASE_URL", url.strip())

    print()
    if prompt_yes_no("允许所有用户?", False):
        save_env_value("SHARECRM_ALLOW_ALL_USERS", "true")
    else:
        save_env_value("SHARECRM_ALLOW_ALL_USERS", "false")
        u = prompt("允许的用户 ID (逗号分隔)", default=get_env_value("SHARECRM_ALLOWED_USERS") or "")
        if u:
            save_env_value("SHARECRM_ALLOWED_USERS", u.replace(" ", ""))
    print_success("已保存到 ~/.hermes/.env")


def register(ctx):
    _register_dashboard_env()
    ctx.register_platform(
        name="sharecrm",
        label="纷享销客 ShareCRM",
        adapter_factory=lambda cfg: ShareCRMAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["SHARECRM_APP_ID", "SHARECRM_APP_SECRET"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        standalone_sender_fn=_standalone_send,
        cron_deliver_env_var="SHARECRM_HOME_CHANNEL",
        allowed_users_env="SHARECRM_ALLOWED_USERS",
        allow_all_env="SHARECRM_ALLOW_ALL_USERS",
        max_message_length=4096,
        emoji="💼",
        platform_hint=(
            "You are on 纷享销客 ShareCRM 企信. "
            "Messages are plain text but Markdown IS rendered: use **bold**, *italic*, `code`, "
            "lists, headings, links, and image links normally. "
            "Outbound only supports text, so never claim to send files/voice; share a link instead. "
            "Always reply with the chat_id from the incoming message verbatim. "
            "In group chats you only receive messages that mention you."
        ),
    )
