"""ShareCRM (纷享销客) IM Gateway adapter for Hermes Agent."""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional

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
SSE_VERSION = "1.3.0"
TOKEN_BUFFER_SECONDS = 300
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

    async def connect(self) -> bool:
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
        chat_type = d.get("chat_type", "direct")
        sender = d.get("from", {})
        raw_id = sender.get("id", "")
        ea = d.get("ea", "")
        user_id = raw_id if raw_id.startswith("E.") else (f"E.{ea}.{raw_id}" if ea and raw_id else raw_id)
        user_name = sender.get("name", raw_id)

        text = d.get("message", {}).get("content", "") or d.get("text", "")
        message_id = d.get("message_id", "")
        reply_to_id = d.get("reply_message_id")

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
        event = MessageEvent(
            text=text, message_type=MessageType.TEXT, source=source,
            message_id=message_id,
            reply_to_message_id=str(reply_to_id) if reply_to_id else None,
            reply_to_text=reply_text, timestamp=datetime.now(),
        )
        await self.handle_message(event)

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
    base = os.getenv("SHARECRM_BASE_URL", "").strip()
    if base:
        seed["base_url"] = base
    home = os.getenv("SHARECRM_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": home}
    return seed


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
