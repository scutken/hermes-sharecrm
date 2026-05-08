"""
ShareCRM (纷享销客) Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to the ShareCRM IM Gateway
via SSE for inbound events and HTTP POST for outbound messages.

ShareCRM IM Gateway docs: https://open.fxiaoke.com/im-gateway/docs/bot-api.md

Configuration via config.yaml::

    gateway:
      platforms:
        sharecrm:
          enabled: true
          extra:
            app_id: "your_app_id"
            app_secret: "your_app_secret"
            base_url: "https://open.fxiaoke.com"
            allowed_users: []
            max_message_length: 4096

Or via environment variables:
    SHARECRM_APP_ID, SHARECRM_APP_SECRET, SHARECRM_BASE_URL
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy import: BasePlatformAdapter lives in the main repo.
# ---------------------------------------------------------------------------
from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.session import SessionSource
from gateway.config import PlatformConfig, Platform

try:
    import aiohttp
except ImportError:
    aiohttp = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://open.fxiaoke.com"
SSE_VERSION = "1.3.0"
TOKEN_BUFFER_SECONDS = 300  # Refresh token 5 minutes before expiry

# HTTP status codes that warrant a retry
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}

# ShareCRM error codes
ERR_TOKEN_INVALID = 40100
ERR_TOKEN_EXPIRED = 40101
ERR_BOT_NOT_CONNECTED = 50001


# ---------------------------------------------------------------------------
# Strip markdown helper (ShareCRM doesn't render markdown)
# ---------------------------------------------------------------------------

def _strip_markdown(text: str) -> str:
    """Convert basic markdown to plain text for ShareCRM."""
    # Bold: **text** or __text__
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    # Italic: *text* or _text_
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    # Inline code: `text`
    text = re.sub(r"`(.+?)`", r"\1", text)
    # Code blocks: ```...```
    text = re.sub(r"```\w*\n?", "", text)
    # Images: ![alt](url)
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    # Links: [text](url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    # Headers: ## text
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Horizontal rules
    text = re.sub(r"^[-*_]{3,}\s*$", "---", text, flags=re.MULTILINE)
    return text


# ---------------------------------------------------------------------------
# ShareCRM Adapter
# ---------------------------------------------------------------------------

class ShareCRMAdapter(BasePlatformAdapter):
    """ShareCRM IM Gateway adapter using SSE for inbound, HTTP for outbound.

    This class is instantiated by the adapter_factory passed to
    register_platform().
    """

    # ShareCRM does not support editing sent messages
    SUPPORTS_MESSAGE_EDITING = False

    def __init__(self, config, **kwargs):
        platform = Platform("sharecrm")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        self.app_id = os.getenv("SHARECRM_APP_ID") or extra.get("app_id", "")
        self.app_secret = os.getenv("SHARECRM_APP_SECRET") or extra.get("app_secret", "")
        self.base_url = (
            os.getenv("SHARECRM_BASE_URL")
            or extra.get("base_url", DEFAULT_BASE_URL)
        ).rstrip("/")

        # Auth
        self.allowed_users: list = extra.get("allowed_users", [])
        self._allowed_users_set: set = set(
            str(u) for u in self.allowed_users if u
        )

        max_msg = extra.get("max_message_length", 4096)
        self.max_message_length = int(max_msg)

        # Token management
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._bot_full_id: Optional[str] = None

        # SSE state
        self._sse_task: Optional[asyncio.Task] = None
        self._last_event_id: Optional[str] = None
        self._sse_session: Optional[aiohttp.ClientSession] = None
        self._client_session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()

    @property
    def name(self) -> str:
        return "ShareCRM"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        """Get token, then start SSE listener."""
        if not aiohttp:
            self._set_fatal_error(
                "missing_deps",
                "aiohttp is required for ShareCRM. Install: pip install aiohttp",
                retryable=False,
            )
            return False

        if not self.app_id or not self.app_secret:
            self._set_fatal_error(
                "config_missing",
                "SHARECRM_APP_ID and SHARECRM_APP_SECRET must be set",
                retryable=False,
            )
            return False

        # Create shared client session
        connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=60, connect=15)
        self._client_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout
        )

        # Get initial token
        if not await self._refresh_token():
            return False

        # Start SSE listener
        self._stop_event.clear()
        self._connected_event.clear()
        self._sse_task = asyncio.create_task(self._sse_loop())

        # Wait for the connected event
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("ShareCRM: SSE connection timed out")
            await self.disconnect()
            self._set_fatal_error(
                "sse_timeout",
                "SSE connection did not receive 'connected' event",
                retryable=True,
            )
            return False

        self._mark_connected()
        logger.info(
            "ShareCRM: connected as bot %s to %s",
            self._bot_full_id,
            self.base_url,
        )
        return True

    async def disconnect(self) -> None:
        """Cancel SSE listener and close client session."""
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

    # ── Token management ────────────────────────────────────────────────

    async def _refresh_token(self) -> bool:
        """Request a new access token from /im-gateway/auth/token."""
        if not self._client_session:
            return False

        url = f"{self.base_url}/im-gateway/auth/token"
        payload = {"appId": self.app_id, "appSecret": self.app_secret}

        try:
            async with self._client_session.post(url, json=payload) as resp:
                data = await resp.json()

            if data.get("code") != 0:
                logger.error(
                    "ShareCRM: auth failed — code=%s msg=%s",
                    data.get("code"),
                    data.get("msg"),
                )
                self._set_fatal_error(
                    f"auth_error_{data.get('code')}",
                    data.get("msg", "Authentication failed"),
                    retryable=False,
                )
                return False

            token_data = data.get("data", {})
            self._access_token = token_data.get("accessToken", "")
            expires_in = token_data.get("expiresIn", 7200)
            self._token_expires_at = time.time() + expires_in - TOKEN_BUFFER_SECONDS

            logger.debug(
                "ShareCRM: token refreshed, expires in %ss", expires_in
            )
            return True

        except aiohttp.ClientError as e:
            logger.error("ShareCRM: auth request failed — %s", e)
            self._set_fatal_error("auth_network_error", str(e), retryable=True)
            return False
        except Exception as e:
            logger.error("ShareCRM: auth unexpected error — %s", e)
            self._set_fatal_error("auth_unexpected", str(e), retryable=True)
            return False

    async def _ensure_token(self) -> bool:
        """Ensure we have a valid token, refreshing if needed."""
        if not self._access_token or time.time() >= self._token_expires_at:
            return await self._refresh_token()
        return True

    # ── SSE listener ──────────────────────────────────────────────────────

    async def _sse_loop(self) -> None:
        """Main SSE listener loop with automatic reconnection."""
        retry_delay = 1.0  # Start with 1s, will be updated by server

        while not self._stop_event.is_set():
            if not await self._ensure_token():
                # Token refresh failed — wait before retrying
                await asyncio.sleep(5)
                continue

            url = (
                f"{self.base_url}/im-gateway/bot/events"
                f"?token={self._access_token}&version={SSE_VERSION}"
            )
            headers = {"Accept": "text/event-stream"}
            if self._last_event_id:
                headers["Last-Event-ID"] = self._last_event_id

            try:
                async with self._client_session.get(
                    url, headers=headers
                ) as resp:
                    if resp.status == 401:
                        logger.warning(
                            "ShareCRM: SSE returned 401, refreshing token"
                        )
                        self._access_token = None
                        await asyncio.sleep(1)
                        continue

                    if resp.status != 200:
                        logger.error(
                            "ShareCRM: SSE returned %s, retrying in %ss",
                            resp.status,
                            retry_delay,
                        )
                        await self._wait_with_backoff(retry_delay)
                        retry_delay = min(retry_delay * 2, 60)
                        continue

                    # Reset retry delay on successful connection
                    retry_delay = 1.0

                    await self._read_sse_stream(resp)

            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as e:
                logger.error("ShareCRM: SSE connection error — %s", e)
                await self._wait_with_backoff(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            except Exception as e:
                logger.error("ShareCRM: SSE unexpected error — %s", e)
                await self._wait_with_backoff(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def _read_sse_stream(self, resp: aiohttp.ClientResponse) -> None:
        """Read SSE events from the response stream."""
        event_type = ""
        event_id = ""
        data_buffer = ""

        async for line_bytes in resp.content:
            if self._stop_event.is_set():
                break

            try:
                line = line_bytes.decode("utf-8").rstrip("\n").rstrip("\r")
            except UnicodeDecodeError:
                continue

            # SSE comment (heartbeat)
            if line.startswith(":"):
                continue

            # Empty line = dispatch event
            if line == "":
                if data_buffer:
                    await self._dispatch_sse_event(
                        event_type, event_id, data_buffer
                    )
                    # Update last event ID for reconnection
                    if event_id:
                        self._last_event_id = event_id
                event_type = ""
                event_id = ""
                data_buffer = ""
                continue

            if line.startswith("event:"):
                event_type = line[6:].strip()
            elif line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data_buffer = line[5:].strip()
            elif line.startswith("retry:"):
                # Server-suggested retry interval — we track but don't
                # override our own backoff here (used by browsers)
                pass

        # If the stream ends normally (max_lifetime), reconnect
        logger.debug("ShareCRM: SSE stream ended, will reconnect")

    async def _dispatch_sse_event(
        self, event_type: str, event_id: str, data: str
    ) -> None:
        """Parse and dispatch an SSE event to the message handler."""
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            logger.warning("ShareCRM: invalid JSON in SSE event: %s", data[:200])
            return

        event_type = payload.get("type", event_type)

        if event_type == "connected":
            await self._handle_connected(payload)
        elif event_type == "message":
            await self._handle_message_event(payload)
        elif event_type == "reset":
            await self._handle_reset(payload)
        else:
            logger.debug("ShareCRM: unknown SSE event type: %s", event_type)

    async def _handle_connected(self, payload: dict) -> None:
        """Handle the 'connected' SSE event."""
        conn_data = payload.get("data", {})
        self._bot_full_id = conn_data.get("bot_full_id", "")
        self._connected_event.set()
        logger.info(
            "ShareCRM: connected — bot=%s protocol=%s client=%s",
            self._bot_full_id,
            conn_data.get("protocol_version", ""),
            conn_data.get("client_version", ""),
        )

    async def _handle_reset(self, payload: dict) -> None:
        """Handle the 'reset' SSE event — cursor expired, clear and reconnect."""
        reason = payload.get("reason", "unknown")
        logger.warning("ShareCRM: SSE reset received — reason=%s", reason)
        self._last_event_id = None

    async def _handle_message_event(self, payload: dict) -> None:
        """Handle an incoming 'message' SSE event."""
        msg_data = payload.get("data", {})
        if not msg_data:
            return

        chat_id = msg_data.get("chat_id", "")
        chat_type = msg_data.get("chat_type", "direct")
        sender = msg_data.get("from", {})
        user_id = sender.get("id", "")
        user_name = sender.get("name", user_id)

        # Extract text content
        text = msg_data.get("message", {}).get("content", "")
        if not text:
            text = msg_data.get("text", "")

        message_id = msg_data.get("message_id", "")
        reply_message_id = msg_data.get("reply_message_id")

        # Build message text with history context for group chats
        history_messages = msg_data.get("history_messages", [])
        if history_messages:
            context_lines = ["[Recent chat context:]"]
            for hmsg in history_messages:
                sender_full = hmsg.get("full_sender_id", hmsg.get("sender_id", ""))
                content = hmsg.get("content", "")
                if content:
                    context_lines.append(f"{sender_full}: {content}")
            context_lines.append("---")
            text = "\n".join(context_lines) + "\n" + text

        # Resolve reply_to_text from history
        reply_to_text = None
        if reply_message_id and history_messages:
            for hmsg in history_messages:
                if str(hmsg.get("message_id", "")) == str(reply_message_id):
                    reply_to_text = hmsg.get("content", "")
                    break

        # Auth check
        if self._allowed_users_set and user_id not in self._allowed_users_set:
            logger.debug(
                "ShareCRM: ignoring message from unauthorized user %s", user_id
            )
            return

        # Build source
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
            reply_to_message_id=str(reply_message_id) if reply_message_id else None,
            reply_to_text=reply_to_text,
            timestamp=datetime.now(),
        )

        await self.handle_message(event)

    async def _wait_with_backoff(self, delay: float) -> None:
        """Wait with exponential backoff, checking stop_event."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # ── Sending messages ─────────────────────────────────────────────────

    def format_message(self, content: str) -> str:
        """Override: strip markdown since ShareCRM doesn't render it."""
        return _strip_markdown(content)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to ShareCRM via HTTP POST."""
        result = await self._send_to_sharecrm(
            chat_id=chat_id,
            text=content,
            reply_message_id=reply_to,
        )
        if result.get("success"):
            return SendResult(
                success=True, message_id=result.get("message_id", "")
            )
        err = result.get("error", "Unknown error")
        retryable = result.get("retryable", False)
        return SendResult(success=False, error=err, retryable=retryable)

    async def _send_to_sharecrm(
        self,
        chat_id: str,
        text: str,
        reply_message_id: Optional[str] = None,
    ) -> dict:
        """Internal: POST /im-gateway/qixin/message/send."""
        if not self._client_session:
            return {"success": False, "error": "Not connected"}

        if not await self._ensure_token():
            return {"success": False, "error": "Token refresh failed", "retryable": True}

        url = f"{self.base_url}/im-gateway/qixin/message/send"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }
        payload: dict = {"chat_id": chat_id, "text": text}
        if reply_message_id:
            try:
                payload["reply_message_id"] = int(reply_message_id)
            except (ValueError, TypeError):
                payload["reply_message_id"] = reply_message_id

        try:
            async with self._client_session.post(
                url, json=payload, headers=headers
            ) as resp:
                data = await resp.json()

            code = data.get("code", -1)
            if code == 0:
                return {
                    "success": True,
                    "message_id": data.get("data", {}).get("message_id", ""),
                }
            elif code == ERR_BOT_NOT_CONNECTED:
                logger.warning("ShareCRM: bot not connected (50001)")
                return {
                    "success": False,
                    "error": data.get("msg", "Bot not connected"),
                    "retryable": True,
                }
            elif code in (ERR_TOKEN_INVALID, ERR_TOKEN_EXPIRED):
                logger.warning("ShareCRM: token invalid/expired, refreshing")
                self._access_token = None
                # Retry once with fresh token
                if await self._refresh_token():
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    try:
                        async with self._client_session.post(
                            url, json=payload, headers=headers
                        ) as resp2:
                            data2 = await resp2.json()
                        if data2.get("code") == 0:
                            return {
                                "success": True,
                                "message_id": data2.get("data", {}).get(
                                    "message_id", ""
                                ),
                            }
                    except Exception:
                        pass
                return {
                    "success": False,
                    "error": data.get("msg", "Token error"),
                    "retryable": True,
                }
            else:
                logger.error(
                    "ShareCRM: send failed — code=%s msg=%s",
                    code,
                    data.get("msg"),
                )
                return {
                    "success": False,
                    "error": data.get("msg", f"Error {code}"),
                    "retryable": code >= 50000,
                }

        except aiohttp.ClientError as e:
            logger.error("ShareCRM: send request failed — %s", e)
            return {"success": False, "error": str(e), "retryable": True}
        except Exception as e:
            logger.error("ShareCRM: send unexpected error — %s", e)
            return {"success": False, "error": str(e), "retryable": False}

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """ShareCRM does not support typing indicators — no-op."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic chat info from chat_id format."""
        # chat_id format: {env}:{ea}:{sessionId}:{parentSessionId}
        is_direct = chat_id.count(":") >= 3 and chat_id.split(":")[3] == ""
        return {
            "name": chat_id,
            "type": "direct" if is_direct else "group",
            "chat_id": chat_id,
        }


# ---------------------------------------------------------------------------
# Plugin registration hooks
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    """Check if ShareCRM is minimally configured."""
    app_id = os.getenv("SHARECRM_APP_ID", "")
    app_secret = os.getenv("SHARECRM_APP_SECRET", "")
    return bool(app_id and app_secret)


def validate_config(config) -> bool:
    """Validate that the platform config has enough info to connect."""
    extra = getattr(config, "extra", {}) or {}
    app_id = os.getenv("SHARECRM_APP_ID") or extra.get("app_id", "")
    app_secret = os.getenv("SHARECRM_APP_SECRET") or extra.get("app_secret", "")
    return bool(app_id and app_secret)


def interactive_setup() -> None:
    """Interactive `hermes gateway setup` flow for ShareCRM."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("纷享销客 ShareCRM")

    existing_id = get_env_value("SHARECRM_APP_ID")
    if existing_id:
        print_info(f"ShareCRM: already configured (appId: {existing_id})")
        if not prompt_yes_no("重新配置 ShareCRM?", False):
            return

    print_info("连接 Hermes 到纷享销客企信 IM Gateway。")
    print_info("需要先在纷享销客开放平台注册应用获取 appId 和 appSecret。")
    print_info("文档: https://open.fxiaoke.com/im-gateway/docs/bot-api.md")
    print()

    app_id = prompt("App ID", default=existing_id or "")
    if not app_id:
        print_warning("App ID 是必需的 — 跳过 ShareCRM 配置")
        return
    save_env_value("SHARECRM_APP_ID", app_id.strip())

    app_secret = prompt("App Secret", password=True)
    if not app_secret:
        print_warning("App Secret 是必需的 — 跳过 ShareCRM 配置")
        return
    save_env_value("SHARECRM_APP_SECRET", app_secret.strip())

    base_url = prompt(
        "IM Gateway Base URL",
        default=get_env_value("SHARECRM_BASE_URL") or DEFAULT_BASE_URL,
    )
    if base_url:
        save_env_value("SHARECRM_BASE_URL", base_url.strip())

    print()
    print_info("🔒 访问控制：限制可与 Bot 交互的用户")
    allow_all = prompt_yes_no("允许所有用户与 Bot 交互?", False)
    if allow_all:
        save_env_value("SHARECRM_ALLOW_ALL_USERS", "true")
        save_env_value("SHARECRM_ALLOWED_USERS", "")
        print_warning("⚠️  开放访问 — 任何用户都可以与 Bot 交互。")
    else:
        save_env_value("SHARECRM_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "允许的用户 ID (逗号分隔，留空则拒绝所有人)",
            default=get_env_value("SHARECRM_ALLOWED_USERS") or "",
        )
        if allowed:
            save_env_value("SHARECRM_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("允许列表已配置")
        else:
            save_env_value("SHARECRM_ALLOWED_USERS", "")
            print_info("未配置允许用户 — Bot 将忽略所有消息，直到添加用户。")

    print()
    print_success("ShareCRM 配置已保存到 ~/.hermes/.env")
    print_info("重启 Gateway 使配置生效: hermes gateway restart")


def is_connected(config) -> bool:
    """Check whether ShareCRM is configured (env or config.yaml)."""
    extra = getattr(config, "extra", {}) or {}
    app_id = os.getenv("SHARECRM_APP_ID") or extra.get("app_id", "")
    app_secret = os.getenv("SHARECRM_APP_SECRET") or extra.get("app_secret", "")
    return bool(app_id and app_secret)


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env vars during gateway config load."""
    app_id = os.getenv("SHARECRM_APP_ID", "").strip()
    app_secret = os.getenv("SHARECRM_APP_SECRET", "").strip()
    if not (app_id and app_secret):
        return None

    seed: dict = {
        "app_id": app_id,
        "app_secret": app_secret,
    }

    base_url = os.getenv("SHARECRM_BASE_URL", "").strip()
    if base_url:
        seed["base_url"] = base_url

    # Home channel for cron delivery
    home = os.getenv("SHARECRM_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": home,
        }

    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
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
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via 纷享销客 ShareCRM 企信. "
            "ShareCRM does not support markdown formatting — use plain text only. "
            "chat_id uses the format '{env}:{ea}:{sessionId}:{parentSessionId}' — "
            "always use the chat_id from incoming messages for replies, never construct it yourself. "
            "Messages are limited to ~4096 characters. "
            "When replying, use the exact chat_id from the original message."
        ),
    )
