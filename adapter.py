"""ShareCRM (纷享销客) IM Gateway adapter for Hermes Agent."""

import asyncio
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import PlatformConfig, Platform

try:
    import aiohttp
except ImportError:
    aiohttp = None

DEFAULT_BASE_URL = "https://open.fxiaoke.com"
SSE_VERSION = "1.4.0"
TOKEN_BUFFER_SECONDS = 300
ACK_TEXT = "👀已收到，稍后回您！"
ERR_TOKEN_INVALID = 40100
ERR_TOKEN_EXPIRED = 40101
ERR_BOT_NOT_CONNECTED = 50001


def _strip_markdown(text: str) -> str:
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"```\w*\n?", "", text)
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^[-*_]{3,}\s*$", "---", text, flags=re.MULTILINE)
    return text


class ShareCRMAdapter(BasePlatformAdapter):

    SUPPORTS_MESSAGE_EDITING = False

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("sharecrm"))
        extra = getattr(config, "extra", {}) or {}

        self.app_id = os.getenv("SHARECRM_APP_ID") or extra.get("app_id", "")
        self.app_secret = os.getenv("SHARECRM_APP_SECRET") or extra.get("app_secret", "")
        self.base_url = (os.getenv("SHARECRM_BASE_URL") or extra.get("base_url", DEFAULT_BASE_URL)).rstrip("/")
        self.max_message_length = int(extra.get("max_message_length", 4096))

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._bot_full_id: Optional[str] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._last_event_id: Optional[str] = None
        self._client_session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()

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
        self._client_session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=60, connect=15))

        if not await self._refresh_token():
            return False

        self._stop_event.clear()
        self._connected_event.clear()
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

    # ── token ────────────────────────────────────────────────────────────

    async def _refresh_token(self) -> bool:
        if not self._client_session:
            return False
        try:
            async with self._client_session.post(
                f"{self.base_url}/im-gateway/auth/token",
                json={"appId": self.app_id, "appSecret": self.app_secret},
            ) as resp:
                data = await resp.json()
            if data.get("code") != 0:
                logger.error("ShareCRM: auth failed code=%s", data.get("code"))
                self._set_fatal_error(f"auth_{data.get('code')}", data.get("msg", "Auth failed"), retryable=False)
                return False
            td = data.get("data", {})
            self._access_token = td.get("accessToken", "")
            self._token_expires_at = time.time() + td.get("expiresIn", 7200) - TOKEN_BUFFER_SECONDS
            return True
        except aiohttp.ClientError as e:
            logger.error("ShareCRM: auth error — %s", e)
            self._set_fatal_error("auth_network", str(e), retryable=True)
            return False

    async def _ensure_token(self) -> bool:
        if not self._access_token or time.time() >= self._token_expires_at:
            return await self._refresh_token()
        return True

    # ── SSE ──────────────────────────────────────────────────────────────

    async def _sse_loop(self) -> None:
        delay = 1.0
        while not self._stop_event.is_set():
            if not await self._ensure_token():
                await asyncio.sleep(5)
                continue
            url = f"{self.base_url}/im-gateway/bot/events?token={self._access_token}&version={SSE_VERSION}"
            headers = {"Accept": "text/event-stream"}
            if self._last_event_id:
                headers["Last-Event-ID"] = self._last_event_id
            try:
                async with self._client_session.get(url, headers=headers) as resp:
                    if resp.status == 401:
                        self._access_token = None
                        await asyncio.sleep(1)
                        continue
                    if resp.status != 200:
                        logger.error("ShareCRM: SSE HTTP %s", resp.status)
                        await self._wait(delay)
                        delay = min(delay * 2, 60)
                        continue
                    delay = 1.0
                    await self._read_stream(resp)
            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as e:
                logger.error("ShareCRM: SSE connection error — %s", e)
                await self._wait(delay)
                delay = min(delay * 2, 60)
            except Exception as e:
                # Server may close the SSE stream after max_lifetime (~60s).
                # Treat as normal — just reconnect, don't log as error.
                if not str(e):
                    logger.debug("ShareCRM: SSE stream ended, reconnecting")
                else:
                    logger.error("ShareCRM: SSE unexpected error — %s", e)
                await self._wait(delay)
                delay = min(delay * 2, 60)

    async def _read_stream(self, resp) -> None:
        ev_type = ev_id = data = ""
        async for line_bytes in resp.content:
            if self._stop_event.is_set():
                break
            try:
                line = line_bytes.decode("utf-8").rstrip("\n").rstrip("\r")
            except UnicodeDecodeError:
                continue
            if line.startswith(":") or not line:
                if not line and data:
                    await self._dispatch(ev_type, ev_id, data)
                    if ev_id:
                        self._last_event_id = ev_id
                    ev_type = ev_id = data = ""
                continue
            if line.startswith("event:"):
                ev_type = line[6:].strip()
            elif line.startswith("id:"):
                ev_id = line[3:].strip()
            elif line.startswith("data:"):
                data = line[5:].strip()

    async def _dispatch(self, ev_type: str, ev_id: str, data: str) -> None:
        try:
            p = json.loads(data)
        except json.JSONDecodeError:
            return
        t = p.get("type", ev_type)
        if t == "connected":
            d = p.get("data", {})
            self._bot_full_id = d.get("bot_full_id", "")
            self._connected_event.set()
            logger.info("ShareCRM: connected bot=%s", self._bot_full_id)
        elif t == "message":
            await self._handle_message(p)
        elif t == "reset":
            logger.warning("ShareCRM: SSE reset")
            self._last_event_id = None

    async def _handle_message(self, payload: dict) -> None:
        d = payload.get("data", {})
        if not d:
            return

        chat_id = d.get("chat_id", "")
        raw_chat_type = (d.get("chat_type") or "direct").strip().lower()
        # Hermes pairing / DM auth only fires when chat_type == "dm".
        # ShareCRM IM uses "direct" for 1:1 chats.
        if raw_chat_type in {"dm", "direct", "private", "c2c", ""}:
            chat_type = "dm"
        else:
            chat_type = raw_chat_type
        sender = d.get("from", {})
        raw_id = sender.get("id", "")
        ea = d.get("ea", "")
        user_id = raw_id if raw_id.startswith("E.") else (f"E.{ea}.{raw_id}" if ea and raw_id else raw_id)
        user_name = sender.get("name", raw_id)

        msg = d.get("message") or {}
        caption = (msg.get("content") or "").strip() or (d.get("text") or "")
        staged = await self._stage_images(msg.get("images") or [])
        image_lines = [f"![{name}]({path})" for path, name in staged]
        text = "\n".join(part for part in [caption, *image_lines] if part)
        message_id = d.get("message_id", "")
        reply_to_id = d.get("reply_message_id")

        # Intercept /sethome command to manually set the home channel env var.
        # This ensures the gateway's "No home channel" check passes immediately
        # without relying on .env reload or gateway command dispatch.
        if caption.strip() == "/sethome":
            self._set_home_channel(chat_id)
            await self.send(chat_id, f"已将当前会话 {chat_id} 设置为 Home Channel。")
            return

        # First DM silently becomes home so Hermes does not inject the
        # "No home channel is set / type /sethome" onboarding notice.
        if chat_type == "dm" and chat_id and not self._home_channel_set():
            self._set_home_channel(chat_id)

        if chat_id and (caption or staged):
            try:
                await self._do_send(chat_id, ACK_TEXT)
            except Exception as exc:
                logger.debug("ShareCRM: ack send failed: %s", exc)

        history = d.get("history_messages", [])
        if history:
            ctx = ["[Recent chat context:]"]
            for h in history:
                sid = h.get("full_sender_id", h.get("sender_id", ""))
                c = h.get("content", "")
                if c:
                    ctx.append(f"{sid}: {c}")
            ctx.append("---")
            text = "\n".join(ctx) + "\n" + text

        reply_text = None
        if reply_to_id and history:
            for h in history:
                if str(h.get("message_id", "")) == str(reply_to_id):
                    reply_text = h.get("content")
                    break

        source = self.build_source(
            chat_id=chat_id, chat_name=chat_id, chat_type=chat_type,
            user_id=user_id, user_name=user_name,
        )
        image_type = getattr(MessageType, "IMAGE", MessageType.TEXT)
        event_kwargs: Dict[str, Any] = {
            "text": text,
            "message_type": image_type if staged and not caption else MessageType.TEXT,
            "source": source,
            "message_id": message_id,
            "reply_to_message_id": str(reply_to_id) if reply_to_id else None,
            "reply_to_text": reply_text,
            "timestamp": datetime.now(),
        }
        media_paths = [path for path, _ in staged]
        if media_paths:
            event_kwargs["media_urls"] = media_paths
        try:
            event = MessageEvent(**event_kwargs)
        except TypeError:
            event_kwargs.pop("media_urls", None)
            event_kwargs["message_type"] = MessageType.TEXT
            event = MessageEvent(**event_kwargs)
        await self.handle_message(event)

    async def _stage_images(self, images: List[Any]) -> List[Tuple[str, str]]:
        staged: List[Tuple[str, str]] = []
        if not self._client_session:
            return staged
        for image in (images or [])[:8]:
            url = ((image or {}).get("url") or "").strip()
            if not url:
                continue
            if not self._is_public_image_url(url):
                logger.warning("ShareCRM: skip inbound image from non-public host")
                continue
            name = ((image or {}).get("filename") or "image.png").strip() or "image.png"
            name = os.path.basename(name.replace("\\", "/"))
            try:
                async with self._client_session.get(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as resp:
                    if resp.status >= 400:
                        logger.warning("ShareCRM: inbound image download failed status=%s", resp.status)
                        continue
                    data = await resp.read()
                if not data or len(data) > 10 * 1024 * 1024:
                    logger.warning("ShareCRM: inbound image empty or too large")
                    continue
                suffix = os.path.splitext(name)[1] or ".png"
                fd, path = tempfile.mkstemp(prefix="sharecrm-image-", suffix=suffix)
                os.close(fd)
                with open(path, "wb") as fh:
                    fh.write(data)
                staged.append((path, name))
            except Exception as exc:
                logger.warning("ShareCRM: inbound image download failed: %s", exc)
        return staged

    @staticmethod
    def _is_public_image_url(url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http") or parsed.username or parsed.password:
            return False
        host = (parsed.hostname or "").lower()
        if not host or host in {"localhost", "metadata.google.internal"} or host.endswith(".localhost"):
            return False
        if host.startswith("127.") or host.startswith("10.") or host.startswith("192.168.") or host.startswith("169.254."):
            return False
        return True

    async def _wait(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # ── send ─────────────────────────────────────────────────────────────

    def format_message(self, content: str) -> str:
        return _strip_markdown(content)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        r = await self._do_send(chat_id, content, reply_to)
        if r.get("success"):
            return SendResult(success=True, message_id=r.get("message_id", ""))
        return SendResult(success=False, error=r.get("error", ""), retryable=r.get("retryable", False))

    async def _do_send(self, chat_id: str, text: str, reply_to: Optional[str] = None) -> dict:
        if not self._client_session or not await self._ensure_token():
            return {"success": False, "error": "Not connected", "retryable": True}

        url = f"{self.base_url}/im-gateway/qixin/message/send"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self._access_token}"}
        payload: dict = {"chat_id": chat_id, "text": text}
        if reply_to:
            try:
                payload["reply_message_id"] = int(reply_to)
            except (ValueError, TypeError):
                payload["reply_message_id"] = reply_to

        try:
            async with self._client_session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json()
            code = data.get("code", -1)
            if code == 0:
                return {"success": True, "message_id": data.get("data", {}).get("message_id", "")}
            if code == ERR_BOT_NOT_CONNECTED:
                return {"success": False, "error": "Bot not connected", "retryable": True}
            if code in (ERR_TOKEN_INVALID, ERR_TOKEN_EXPIRED):
                self._access_token = None
                if await self._refresh_token():
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    async with self._client_session.post(url, json=payload, headers=headers) as r2:
                        d2 = await r2.json()
                    if d2.get("code") == 0:
                        return {"success": True, "message_id": d2.get("data", {}).get("message_id", "")}
                return {"success": False, "error": data.get("msg", "Token error"), "retryable": True}
            return {"success": False, "error": data.get("msg", f"Error {code}"), "retryable": code >= 50000}
        except aiohttp.ClientError as e:
            return {"success": False, "error": str(e), "retryable": True}
        except Exception as e:
            return {"success": False, "error": str(e), "retryable": False}

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        pass

    @staticmethod
    def _home_channel_set() -> bool:
        return bool((os.getenv("SHARECRM_HOME_CHANNEL") or "").strip())

    @staticmethod
    def _set_home_channel(chat_id: str) -> None:
        if not chat_id:
            return
        os.environ["SHARECRM_HOME_CHANNEL"] = chat_id
        try:
            from hermes_cli.setup import save_env_value
            save_env_value("SHARECRM_HOME_CHANNEL", chat_id)
        except Exception:
            pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        is_direct = chat_id.count(":") >= 3 and chat_id.split(":")[3] == ""
        return {"name": chat_id, "type": "direct" if is_direct else "group", "chat_id": chat_id}


# ── plugin hooks ────────────────────────────────────────────────────────

def check_requirements() -> bool:
    return bool(os.getenv("SHARECRM_APP_ID") and os.getenv("SHARECRM_APP_SECRET"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(
        (os.getenv("SHARECRM_APP_ID") or extra.get("app_id"))
        and (os.getenv("SHARECRM_APP_SECRET") or extra.get("app_secret"))
    )


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    aid = os.getenv("SHARECRM_APP_ID", "").strip()
    sec = os.getenv("SHARECRM_APP_SECRET", "").strip()
    if not (aid and sec):
        return None
    seed: dict = {"app_id": aid, "app_secret": sec}
    base = os.getenv("SHARECRM_BASE_URL", "").strip() or DEFAULT_BASE_URL
    seed["base_url"] = base
    home = os.getenv("SHARECRM_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": home}
    return seed


_DASHBOARD_ENV = (
    {
        "name": "SHARECRM_BASE_URL",
        "description": f"接口地址，留空则使用 {DEFAULT_BASE_URL}",
        "prompt": f"Base URL ({DEFAULT_BASE_URL})",
        "help": f"默认 {DEFAULT_BASE_URL}",
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
        cron_deliver_env_var="SHARECRM_HOME_CHANNEL",
        allowed_users_env="SHARECRM_ALLOWED_USERS",
        allow_all_env="SHARECRM_ALLOW_ALL_USERS",
        max_message_length=4096,
        emoji="💼",
        platform_hint=(
            "You are on 纷享销客 ShareCRM 企信. "
            "Use plain text only — no markdown. "
            "Always use the chat_id from incoming messages for replies."
        ),
    )
