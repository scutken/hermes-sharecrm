"""ShareCRM 企信插件单元测试。

测试尽量不依赖网络：SSE 解析、事件构造、去重、配置读取都用假的 session/response。

运行（需要装有 hermes-agent 的 Python 环境）::

    python -m pytest tests/ -q
    # 或
    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

PLUGIN_DIR = Path(__file__).resolve().parents[1]

if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

# 没有 hermes-agent 运行时就从 HERMES_AGENT_HOME / 本机默认安装位置找（不伪造 gateway）。
# pytest 下 conftest.py 已经做过一次，这里兜底 unittest discover / 直接跑本文件。
try:  # 这些测试依赖 hermes-agent 运行时
    import gateway  # noqa: F401
except ImportError as exc:
    import os as _os

    for _raw in (
        _os.getenv("HERMES_AGENT_HOME") or "",
        str(Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent"),
        str(Path.home() / ".hermes" / "hermes-agent"),
    ):
        if not _raw or not (Path(_raw) / "gateway" / "__init__.py").is_file():
            continue
        if _raw not in sys.path:
            sys.path.insert(0, _raw)
        try:
            import gateway  # noqa: F401
        except ImportError:
            sys.path.remove(_raw)
            continue
        break
    else:  # pragma: no cover
        raise unittest.SkipTest("hermes-agent (gateway) is required to run these tests") from exc


def _load_plugin():
    """按 Hermes 的加载方式把插件目录当包导入（相对 import 才能生效）。"""
    name = "hermes_plugins.sharecrm_under_test"
    if name in sys.modules:
        return sys.modules[name], importlib.import_module(name + ".adapter")
    spec = importlib.util.spec_from_file_location(
        name, PLUGIN_DIR / "__init__.py", submodule_search_locations=[str(PLUGIN_DIR)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module, importlib.import_module(name + ".adapter")


PKG, adapter = _load_plugin()
compat = importlib.import_module(PKG.__name__ + "._compat")

from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.platform_registry import PlatformEntry, platform_registry  # noqa: E402
from gateway.platforms.base import MessageType  # noqa: E402


def _ensure_platform():
    """让 Platform("sharecrm") 这个动态枚举成员可用（等价于插件 register 后）。"""
    if not platform_registry.is_registered("sharecrm"):
        platform_registry.register(
            PlatformEntry(
                name="sharecrm",
                label="ShareCRM",
                adapter_factory=lambda cfg: None,
                check_fn=lambda: True,
                source="builtin",
            )
        )
    return Platform("sharecrm")


def _make_adapter(extra: dict | None = None) -> "adapter.ShareCRMAdapter":
    _ensure_platform()
    return adapter.ShareCRMAdapter(PlatformConfig(extra=dict(extra or {})))


class _FakePost:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """按调用顺序返回预置响应；用于 token / send 测试。"""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def post(self, url, **kwargs):
        self.calls += 1
        idx = min(self.calls - 1, len(self._responses) - 1)
        resp = self._responses[idx]
        if isinstance(resp, Exception):
            raise resp
        return _FakePost(resp)


class _FakeResp:
    def __init__(self, payload=None, status: int = 200):
        self._payload = payload
        self.status = status

    async def json(self, content_type=None):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeContent:
    def __init__(self, lines):
        self._lines = lines

    def __aiter__(self):
        async def _gen():
            for line in self._lines:
                await asyncio.sleep(0)
                yield line

        return _gen()


class _FakeStreamResp:
    status = 200

    def __init__(self, lines):
        self.content = _FakeContent(lines)

    # _sse_loop 用 ``async with session.get(...) as resp``，假响应也要支持
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in (
            "SHARECRM_APP_ID", "SHARECRM_APP_SECRET", "SHARECRM_BASE_URL",
            "SHARECRM_HOME_CHANNEL", "SHARECRM_ALLOWED_USERS",
        )}
        for k in self._saved:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_adapter_reads_extra_then_defaults(self):
        a = _make_adapter({"app_id": "aid", "app_secret": "sec", "base_url": "https://example.com/"})
        self.assertEqual(a.app_id, "aid")
        self.assertEqual(a.app_secret, "sec")
        self.assertEqual(a.base_url, "https://example.com")
        self.assertEqual(a.sse_version, adapter.DEFAULT_SSE_VERSION)
        self.assertEqual(a.max_message_length, 4096)
        self.assertEqual(a.MAX_MESSAGE_LENGTH, 4096)

    def test_env_wins_over_extra(self):
        os.environ["SHARECRM_BASE_URL"] = "https://env.example.com"
        a = _make_adapter({"base_url": "https://yaml.example.com"})
        self.assertEqual(a.base_url, "https://env.example.com")

    def test_format_message_keeps_markdown(self):
        a = _make_adapter()
        raw = "**bold** and `code` and [x](https://y)"
        self.assertEqual(a.format_message(raw), raw)

    def test_env_enablement_seeds_extra_and_home(self):
        os.environ.update({
            "SHARECRM_APP_ID": "aid",
            "SHARECRM_APP_SECRET": "sec",
            "SHARECRM_BASE_URL": "https://env.example.com",
            "SHARECRM_HOME_CHANNEL": "0:fs:sess:",
        })
        seed = adapter._env_enablement()
        self.assertIsInstance(seed, dict)
        self.assertEqual(seed["app_id"], "aid")
        self.assertEqual(seed["base_url"], "https://env.example.com")
        self.assertEqual(seed["home_channel"]["chat_id"], "0:fs:sess:")

    def test_env_enablement_requires_credentials(self):
        os.environ["SHARECRM_APP_ID"] = "aid"
        self.assertIsNone(adapter._env_enablement())

    def test_apply_yaml_config_bridges_env_and_extra(self):
        out = adapter._apply_yaml_config({}, {"base_url": "https://y", "allowed_users": ["a", "b"]})
        self.assertEqual(out["base_url"], "https://y")
        self.assertEqual(out["allowed_users"], ["a", "b"])
        self.assertEqual(os.environ.get("SHARECRM_BASE_URL"), "https://y")

    def test_validate_config_and_check_requirements(self):
        self.assertFalse(adapter.validate_config(PlatformConfig(extra={})))
        self.assertFalse(adapter.is_connected(PlatformConfig(extra={})))
        cfg = PlatformConfig(extra={"app_id": "a", "app_secret": "b"})
        self.assertTrue(adapter.validate_config(cfg))
        self.assertTrue(adapter.is_connected(cfg))
        with mock.patch.dict(os.environ, {"SHARECRM_APP_ID": "a", "SHARECRM_APP_SECRET": "b"}):
            self.assertTrue(adapter.check_requirements())


class RegisterTests(unittest.TestCase):
    def test_register_wires_required_hooks(self):
        captured = {}

        class Ctx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

        adapter.register(Ctx())
        self.assertEqual(captured["name"], "sharecrm")
        self.assertIs(captured["standalone_sender_fn"], adapter._standalone_send)
        self.assertIs(captured["apply_yaml_config_fn"], adapter._apply_yaml_config)
        self.assertIs(captured["env_enablement_fn"], adapter._env_enablement)
        self.assertEqual(captured["allowed_users_env"], "SHARECRM_ALLOWED_USERS")
        self.assertEqual(captured["allow_all_env"], "SHARECRM_ALLOW_ALL_USERS")
        self.assertEqual(captured["cron_deliver_env_var"], "SHARECRM_HOME_CHANNEL")


class ParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_stream_joins_multiline_data_and_parses_retry(self):
        a = _make_adapter()
        a._dispatch = mock.AsyncMock(return_value=None)
        lines = [
            b"retry: 2500\n",
            b"id: 42\n",
            b"event: message\n",
            b'data: {"type":\n',
            b'data: "message"}\n',
            b"\n",
        ]
        reason = await a._read_stream(_FakeStreamResp(lines))
        self.assertEqual(reason, "eof")
        self.assertEqual(a._server_retry_ms, 2500)
        self.assertEqual(a._last_event_id, "42")
        a._dispatch.assert_awaited_once()
        args = a._dispatch.await_args.args
        self.assertEqual(args[0], "message")
        self.assertEqual(args[1], "42")
        self.assertEqual(args[2], '{"type":\n"message"}')

    async def test_read_stream_returns_reset(self):
        a = _make_adapter()
        lines = [
            b"event: reset\n",
            b'data: {"type":"reset","reason":"cursor_expired"}\n',
            b"\n",
        ]
        self.assertEqual(await a._read_stream(_FakeStreamResp(lines)), "reset")

    async def test_connected_event_captures_retry_and_lifetime(self):
        a = _make_adapter()
        payload = json.dumps({
            "type": "connected",
            "data": {"bot_full_id": "B.fs.demo", "retry": 1200, "max_lifetime": 1800000},
        })
        await a._dispatch("connected", "1", payload)
        self.assertEqual(a._bot_full_id, "B.fs.demo")
        self.assertEqual(a._server_retry_ms, 1200)
        self.assertEqual(a._max_lifetime_ms, 1800000)
        self.assertTrue(a._sse_online.is_set())
        self.assertTrue(a._connected_event.is_set())
        self.assertAlmostEqual(a._server_retry_seconds(9.0), 1.2, places=3)

    async def test_server_retry_fallback_when_absent(self):
        a = _make_adapter()
        self.assertEqual(a._server_retry_seconds(1.0), 1.0)

    async def test_sse_stream_end_logs_duration_and_reason(self):
        """连接结束必须留一条带时长和原因的 INFO，62 秒断连才有据可查。"""
        a = _make_adapter()
        a._stop_event = asyncio.Event()

        class _Sess:
            def get(self, url, **kwargs):
                return _FakeStreamResp([b"event: connected\n", b'data: {"type":"connected"}\n', b"\n"])

        a._client_session = _Sess()
        a._ensure_token = mock.AsyncMock(return_value=True)

        async def _stop_after(_delay):
            a._stop_event.set()

        a._wait = _stop_after
        with self.assertLogs(adapter.logger, level="INFO") as logs:
            await a._sse_loop()
        joined = "\n".join(logs.output)
        self.assertIn("SSE stream ended after", joined)
        self.assertIn("reason=eof", joined)

    async def test_sse_connection_error_logs_duration_and_exception_type(self):
        """网络层断要区分于 eof：日志里带异常类型和持续时长。"""
        a = _make_adapter()
        a._stop_event = asyncio.Event()

        class _Sess:
            def get(self, url, **kwargs):
                raise asyncio.TimeoutError("sock read timeout")

        a._client_session = _Sess()
        a._ensure_token = mock.AsyncMock(return_value=True)

        async def _stop_after(_delay):
            a._stop_event.set()

        a._wait = _stop_after
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            await a._sse_loop()
        joined = "\n".join(logs.output)
        self.assertIn("SSE connection error after", joined)
        self.assertIn("TimeoutError", joined)

    async def test_dedup_drops_duplicate(self):
        a = _make_adapter()
        a.handle_message = mock.AsyncMock()
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {"message_id": "m1", "chat_id": "0:fs:s:", "chat_type": "direct",
                                               "from": {"id": "8017", "name": "n"}, "ea": "fs",
                                               "message": {"type": "text", "content": "hi"}}}
        await a._handle_message(payload)
        await a._handle_message(payload)
        self.assertEqual(a.handle_message.await_count, 1)

    async def test_group_history_uses_channel_context_with_header(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "m2", "chat_id": "0:fs:grp:", "chat_type": "group",
            "from": {"id": "8017", "name": "n"}, "ea": "fs",
            "message": {"type": "text", "content": "现在几点"},
            "reply_message_id": 111,
            "history_messages": [
                {"message_id": "111", "content": "你好", "full_sender_id": "E.fs.9001", "message_timestamp": 1000},
                {"message_id": "222", "content": "在吗", "sender_id": "E.fs.8017", "message_timestamp": 2000},
            ],
        }}
        await a._handle_message(payload)
        event = captured["event"]
        self.assertEqual(event.text, "现在几点")
        self.assertTrue(event.channel_context.startswith(adapter.HISTORY_HEADER))
        self.assertIn("[9001] 你好", event.channel_context)
        self.assertIn("[8017] 在吗", event.channel_context)
        self.assertNotIn("现在几点", event.channel_context or "")
        self.assertEqual(event.reply_to_text, "你好")

    async def test_dm_does_not_inject_history(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "dm1", "chat_id": "0:fs:dm:", "chat_type": "direct",
            "from": {"id": "8017", "name": "n"}, "ea": "fs",
            "message": {"type": "text", "content": "hi"},
            "history_messages": [
                {"message_id": "h1", "content": "旧消息", "sender_id": "E.fs.1", "message_timestamp": 1000},
            ],
        }}
        await a._handle_message(payload)
        self.assertIsNone(captured["event"].channel_context)

    async def test_history_delta_respects_watermark(self):
        a = _make_adapter()
        a._last_self_ts["0:fs:grp:"] = 2000
        history = [
            {"message_id": "old", "content": "旧的", "sender_id": "E.fs.1", "message_timestamp": 1000},
            {"message_id": "new", "content": "新的", "sender_id": "E.fs.2", "message_timestamp": 3000},
        ]
        text = a._format_history(history, since_ms=2000)
        self.assertIn("新的", text)
        self.assertNotIn("旧的", text)

    async def test_format_history_skips_entries_without_timestamp(self):
        a = _make_adapter()
        history = [
            {"message_id": "x", "content": "无时间戳", "sender_id": "E.fs.1"},
            {"message_id": "y", "content": "有时间戳", "sender_id": "E.fs.2", "message_timestamp": 1},
        ]
        text = a._format_history(history)
        self.assertIn("有时间戳", text)
        self.assertNotIn("无时间戳", text)

    async def test_format_history_excludes_bot_messages(self):
        a = _make_adapter()
        history = [
            {"message_id": "b", "content": "机器人回复", "full_sender_id": "BOT.2140.82846_abc", "message_timestamp": 1},
            {"message_id": "u", "content": "用户消息", "sender_id": "E.fs.1", "message_timestamp": 2},
        ]
        text = a._format_history(history)
        self.assertIn("用户消息", text)
        self.assertNotIn("机器人回复", text)

    def test_strip_mention_generic(self):
        a = _make_adapter()
        self.assertEqual(a._strip_mention("@二哈 /new"), "/new")
        self.assertEqual(a._strip_mention("/new @二哈"), "/new")
        self.assertEqual(a._strip_mention("@二哈 /new @二哈"), "/new")
        self.assertEqual(a._strip_mention("@二哈 你好 @二哈"), "你好")
        self.assertEqual(a._strip_mention("hello @二哈"), "hello")
        self.assertEqual(a._strip_mention("你好 @某人 吗"), "你好 @某人 吗")  # 未配置名字时中间不动
        self.assertEqual(a._strip_mention("@二哈"), "@二哈")
        self.assertEqual(a._strip_mention("@file:xxx"), "@file:xxx")
        self.assertEqual(a._strip_mention("普通消息"), "普通消息")
        self.assertEqual(a._strip_mention("＠二哈 /reset"), "/reset")

    def test_strip_mention_configured_names(self):
        a = _make_adapter({"mention_names": "二哈,小 助手"})
        self.assertEqual(a._strip_mention("@小 助手 /new"), "/new")
        self.assertEqual(a._strip_mention("@二哈 /new"), "/new")
        self.assertEqual(a._strip_mention("/new @二哈"), "/new")
        self.assertEqual(a._strip_mention("你好 @二哈 帮我看"), "你好 帮我看")
        self.assertEqual(a._strip_mention("@二哈 你好 @二哈"), "你好")
        self.assertEqual(a._strip_mention("@二哈"), "@二哈")
        self.assertEqual(a._strip_mention("＠二哈 /reset"), "/reset")

    async def test_group_mention_is_stripped_before_event(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "g9", "chat_id": "0:fs:grp:", "chat_type": "group",
            "from": {"id": "8017", "name": "张三"}, "ea": "fs",
            "message": {"type": "text", "content": "@二哈 /new"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].text, "/new")
        self.assertTrue(captured["event"].is_command())

    async def test_group_trailing_mention_stripped(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "g10", "chat_id": "0:fs:grp:", "chat_type": "group",
            "from": {"id": "8017", "name": "张三"}, "ea": "fs",
            "message": {"type": "text", "content": "/new @二哈"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].text, "/new")

    async def test_dm_mention_is_not_stripped(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "d9", "chat_id": "0:fs:dm:", "chat_type": "direct",
            "from": {"id": "8017", "name": "张三"}, "ea": "fs",
            "message": {"type": "text", "content": "@二哈 /new"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].text, "@二哈 /new")

    async def test_image_only_marks_photo(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[("/tmp/x.png", "x.png")])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "m3", "chat_id": "0:fs:s:", "chat_type": "direct",
            "from": {"id": "8017", "name": "n"}, "ea": "fs",
            "message": {"type": "image", "content": "", "images": [{"url": "https://x/y.png"}]},
        }}
        await a._handle_message(payload)
        event = captured["event"]
        self.assertEqual(event.message_type, MessageType.PHOTO)
        self.assertEqual(event.media_urls, ["/tmp/x.png"])

class QuotedMessageTests(unittest.IsolatedAsyncioTestCase):
    """被引用消息（quoted）：文本块、图片顺序与上限、不依赖 history 开关。"""

    async def _run(self, data: dict, *, extra: dict | None = None, stage=None):
        a = _make_adapter(extra)
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(
            side_effect=stage or (lambda images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None: [])
        )
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": data.pop("_message_id", "q1"),
            "chat_id": data.pop("_chat_id", "0:fs:grp:"),
            "chat_type": data.pop("_chat_type", "group"),
            "from": {"id": "8017", "name": "张三"}, "ea": "fs",
            **data,
        }}
        await a._handle_message(payload)
        return a, captured["event"]

    async def test_quoted_hit_renders_block_with_sender_and_time(self):
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 看下这条"},
            "reply_message_id": 111,
            "quoted": {
                "message_id": "111", "sender_id": "9001", "full_sender_id": "E.fs.9001",
                "timestamp_ms": 1762500000000, "message_type": "T", "text": "服务器又挂了",
                "images": [], "missing": False, "images_unavailable": False,
            },
            "history_messages": [
                {"message_id": "111", "content": "服务器又挂了", "full_sender_id": "E.fs.9001", "message_timestamp": 1000},
            ],
        })
        times = adapter.ShareCRMAdapter._quoted_time({"timestamp_ms": 1762500000000})
        self.assertRegex(times, r"^\d{2}:\d{2}$")
        self.assertIn("看下这条", event.text)
        self.assertIn(f"[Quoted message · sender:9001 · {times}]", event.text)
        self.assertIn("服务器又挂了", event.text)
        self.assertEqual(event.reply_to_message_id, "111")
        # 有 quoted 时不再重复塞 reply_to_text（同一段文本会看两遍）
        self.assertIsNone(event.reply_to_text)
        self.assertNotIn("/tmp", event.text)

    async def test_quoted_missing_does_not_invent_content(self):
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 回你"},
            "reply_message_id": 222,
            "history_messages": [
                {"message_id": "222", "content": "历史里的旧内容", "sender_id": "E.fs.1", "message_timestamp": 1000},
            ],
            "quoted": {"message_id": "222", "missing": True},
        })
        self.assertIn("[Quoted message · id=222 · 超出可见范围]", event.text)
        self.assertNotIn("历史里的旧内容", event.text)

    async def test_quoted_images_unavailable_is_explicit(self):
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 这张图"},
            "quoted": {
                "message_id": "333", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "I", "text": "", "images": [],
                "missing": False, "images_unavailable": True,
            },
        })
        self.assertIn("[Quoted message · sender:9001 ·", event.text)
        self.assertIn("[引用的图片取不到]", event.text)
        self.assertEqual(event.media_urls, [])
        self.assertNotIn("其中图片", event.text)

    async def test_no_quoted_keeps_old_shape(self):
        _, event = await self._run({
            "message": {"type": "text", "content": "普通消息"},
            "reply_message_id": 111,
            "history_messages": [
                {"message_id": "111", "content": "你好", "full_sender_id": "E.fs.9001", "message_timestamp": 1000},
            ],
        })
        self.assertEqual(event.text, "普通消息")
        self.assertEqual(event.reply_to_text, "你好")
        self.assertEqual(event.reply_to_message_id, "111")
        self.assertNotIn("Quoted message", event.text)

    async def test_quoted_images_go_after_current_and_capped_at_four(self):
        def stage(images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None):
            return [(f"/tmp/{limit}-{i}.png", f"{i}.png") for i in range(min(len(images), limit))]

        quoted_images = [{"url": f"https://x/q{i}.png", "filename": f"q{i}.png"} for i in range(6)]
        a, event = await self._run({
            "message": {"type": "text", "content": "@二哈 对比下", "images": [{"url": "https://x/cur.png", "filename": "cur.png"}]},
            "quoted": {
                "message_id": "444", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "MIX", "text": "六张图", "images": quoted_images,
                "missing": False, "images_unavailable": False,
            },
        }, stage=stage)
        calls = a._stage_images.await_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1].kwargs.get("limit"), adapter.MAX_QUOTED_IMAGES)
        self.assertEqual(calls[1].kwargs.get("timeout"), adapter.QUOTED_IMAGE_TIMEOUT)
        self.assertEqual(adapter.MAX_QUOTED_IMAGES, 4)
        self.assertEqual(adapter.QUOTED_IMAGE_TIMEOUT, 5.0)
        # 当前消息那条路不带 timeout，走默认 15s
        self.assertNotIn("timeout", calls[0].kwargs)
        self.assertEqual(event.media_urls, ["/tmp/8-0.png", "/tmp/4-0.png", "/tmp/4-1.png", "/tmp/4-2.png", "/tmp/4-3.png"])
        self.assertEqual(len(event.media_types), len(event.media_urls))
        self.assertIn("[其中图片 4 张]", event.text)
        self.assertIn("六张图", event.text)

    async def test_quoted_renders_even_when_history_disabled_and_in_dm(self):
        _, event = await self._run({
            "_chat_id": "0:fs:dm:", "_chat_type": "direct",
            "message": {"type": "text", "content": "私聊里引用"},
            "history_messages": [
                {"message_id": "555", "content": "不该出现", "sender_id": "E.fs.1", "message_timestamp": 1000},
            ],
            "quoted": {
                "message_id": "555", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "T", "text": "私聊被引用的正文", "images": [],
                "missing": False, "images_unavailable": False,
            },
        }, extra={"include_history": False})
        self.assertIsNone(event.channel_context)
        self.assertIn("私聊被引用的正文", event.text)
        self.assertNotIn("不该出现", event.text)

    async def test_quoted_timestamp_seconds_and_iso_and_bad(self):
        fmt = adapter.ShareCRMAdapter._quoted_time
        # 契约主字段 timestamp_ms（毫秒），旧名 timestamp 仍是兼容路径
        self.assertEqual(fmt({"timestamp_ms": 1762500000000}), fmt({"timestamp": 1762500000000}))
        self.assertEqual(fmt({"timestamp_ms": 1762500000000}), fmt({"timestamp": 1762500000}))
        self.assertEqual(fmt({"timestamp_ms": 1762500000000, "timestamp": 1}), fmt({"timestamp_ms": 1762500000000}))
        self.assertTrue(fmt({"timestamp": "2026-11-20T13:05:00"}))
        self.assertEqual(fmt({"timestamp": ""}), "")
        self.assertEqual(fmt({"timestamp": "不是时间"}), "")
        self.assertEqual(fmt({}), "")

    async def test_quoted_mix_text_only_is_not_reported_as_missing_images(self):
        # 网关侧（白泽拍板）MIX 只有文字时不给 images_unavailable；插件不能凭空说图取不到
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 这段文字你怎么看"},
            "quoted": {
                "message_id": "888", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "MIX", "text": "只有文字的图文消息", "images": [],
            },
        })
        self.assertIn("只有文字的图文消息", event.text)
        self.assertNotIn("取不到", event.text)
        self.assertNotIn("其中图片", event.text)
        self.assertEqual(event.media_urls, [])

    async def test_quoted_images_listed_but_none_downloaded_is_explicit(self):
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 看"},
            "quoted": {
                "message_id": "666", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "I", "text": "", "images": [{"url": "https://x/a.png"}],
                "missing": False, "images_unavailable": False,
            },
        })
        self.assertIn("[引用的图片本次没取到]", event.text)

    def test_format_quoted_plain_and_mix_and_doc(self):
        fmt = adapter.ShareCRMAdapter._format_quoted
        base = {"message_id": "1", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000}
        clock = adapter.ShareCRMAdapter._quoted_time(base)
        self.assertEqual(fmt({**base, "message_type": "T", "text": " 你好 "}), f"[Quoted message · sender:9001 · {clock}]\n你好")
        mix = fmt({**base, "message_type": "MIX", "text": "图+文", "images": [{"url": "https://x/1.png"}]})
        self.assertIn("图+文", mix)
        self.assertNotIn("其中图片", mix)  # 计数行要靠真实下载结果，不在这里编
        doc = fmt({**base, "message_type": "D", "text": ""})
        self.assertIn("文档", doc)
        doc_raw = fmt({**base, "message_type": "D", "text": "report.pdf"})
        self.assertIn("report.pdf", doc_raw)
        self.assertIn("原始内容", doc_raw)  # 别让模型把文档原始 content 当用户原话
        self.assertEqual(fmt({}), "")
        self.assertEqual(fmt(None), "")

    async def test_quoted_without_any_sender_does_not_crash(self):
        """full_sender_id 与 sender_id 都缺失时，表头不能崩、也不能编造发送人。

        断言只盯「不崩 + 表头还在正文前面」，发送人位置具体是什么字面值是展示细节，
        以后想从 ``?`` 改成「未知发送人」不应该让这条用例失败。
        """
        fmt = adapter.ShareCRMAdapter._format_quoted
        base = {"message_id": "1", "timestamp_ms": 1762500000000, "message_type": "T", "text": "没有发送人"}
        for quoted in (dict(base), {**base, "sender_id": "", "full_sender_id": ""},
                       {**base, "sender_id": None, "full_sender_id": None}):
            block = fmt(quoted)
            self.assertIn("没有发送人", block)
            first, _, rest = block.partition("\n")
            self.assertTrue(first.startswith("[Quoted message · "), first)
            self.assertTrue(first.endswith("]"), first)
            # 表头里必须有一个「发送人」占位段，不能是空段或直接省略
            middle = first[len("[Quoted message · "):-1]
            self.assertTrue(middle.split(" · ")[0].strip(), first)
            self.assertIn("没有发送人", rest)
        # 整条消息路径也不崩
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 看这条"},
            "quoted": dict(base),
        })
        self.assertIn("没有发送人", event.text)
        self.assertIn("[Quoted message · ", event.text)

    async def test_quoted_empty_text_key_is_not_rendered(self):
        """网关 NON_NULL 只挡 null，空串 "" 仍会下发；插件不能把它当正文渲染出一行空。

        空正文时表头下面补一行「没有可读内容」（刘法克 2026-11-20 定的口径）：
        引用这个事实要保留，但要说清没内容不是故障；不能出现空字符串行。
        """
        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 这条"},
            "quoted": {
                "message_id": "999", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "T", "text": "", "images": [],
            },
        })
        block = adapter.ShareCRMAdapter._format_quoted({
            "message_id": "999", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
            "message_type": "T", "text": "", "images": [],
        })
        lines = block.split("\n")
        self.assertTrue(lines[0].startswith("[Quoted message · sender:9001 ·"), block)
        # 没有空行、没有空正文行，只有一行说明
        self.assertEqual(lines, [lines[0], "[被引用的消息没有可读内容]"])
        self.assertNotIn("\n\n", event.text)

    def test_quoted_without_readable_content_says_so(self):
        """表头之后什么都没有时补说明；有图/D 文档时不能误加这句。"""
        fmt = adapter.ShareCRMAdapter._format_quoted
        base = {"message_id": "1", "full_sender_id": "E.fs.1230", "timestamp_ms": 1795164496000}
        empty = fmt({**base, "message_type": "T", "text": "", "images": []})
        self.assertIn("[被引用的消息没有可读内容]", empty)
        # 有正文：不加
        self.assertNotIn("[被引用的消息没有可读内容]", fmt({**base, "message_type": "T", "text": "111", "images": []}))
        # 有图：由 _quoted_image_note 负责，这里不能抢话
        self.assertNotIn("[被引用的消息没有可读内容]", fmt({**base, "message_type": "MIX", "text": "", "images": [{"url": "https://x/1.png"}]}))
        # 纯图 I：同上，图片说明归 note
        self.assertNotIn("[被引用的消息没有可读内容]", fmt({**base, "message_type": "I", "text": "", "images": []}))
        # D 文档：用专门的文案，不混用
        doc = fmt({**base, "message_type": "D", "text": "", "images": []})
        self.assertIn("文档/文件消息", doc)
        self.assertNotIn("[被引用的消息没有可读内容]", doc)

    async def test_quoted_images_total_beyond_staged_is_disclosed(self):
        def stage(images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None):
            return [(f"/tmp/q{i}.png", f"q{i}.png") for i in range(min(len(images), limit))]

        _, event = await self._run({
            "message": {"type": "text", "content": "@二哈 看引用"},
            "quoted": {
                "message_id": "777", "full_sender_id": "E.fs.9001", "timestamp_ms": 1762500000000,
                "message_type": "I", "text": "",
                "images": [{"url": f"https://x/q{i}.png"} for i in range(4)],
                "images_total": 6, "missing": False, "images_unavailable": False,
            },
        }, stage=stage)
        self.assertIn("[其中图片 4 张][共 6 张，仅取到 4 张]", event.text)
        self.assertEqual(len(event.media_urls), 4)

    async def test_quoted_image_batch_times_out_without_raising(self):
        """引用图整批卡住时：到总上限就放弃、返回空、不抛异常、日志可分辨。"""
        a = _make_adapter()
        started = asyncio.Event()
        released = []

        async def _hang(images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None):
            started.set()
            try:
                await asyncio.sleep(60)  # 模拟文件服务卡住不返回
            except asyncio.CancelledError:
                released.append(True)
                raise
            return [("/tmp/never.png", "never.png")]

        current = asyncio.ensure_future(a._stage_images([]))
        quoted = asyncio.ensure_future(_hang([{"url": "https://x/a.png"}]))
        with mock.patch.object(adapter, "QUOTED_IMAGES_TOTAL_TIMEOUT", 0.05), \
                self.assertLogs(adapter.logger, level="WARNING") as logs:
            staged, quoted_staged = await a._join_quoted_batch(current, quoted)
        self.assertEqual(quoted_staged, [])
        self.assertEqual(staged, [])
        self.assertTrue(started.is_set())
        self.assertEqual(released, [True])  # 被取消，不是留着的僵尸任务
        self.assertTrue(quoted.cancelled() or quoted.done())
        self.assertTrue(any("quoted image batch timeout" in line for line in logs.output))

    async def test_quoted_batch_timeout_does_not_cut_current_attachment(self):
        """总上限只作用于引用图；当前消息的图再慢也要等它下完，不能被一起取消。"""
        a = _make_adapter()
        slow_started = asyncio.Event()

        async def _slow_current(images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None):
            slow_started.set()
            await asyncio.sleep(0.15)  # 超过引用图总上限（0.05s），但在自己的 15s 之内
            return [("/tmp/cur.png", "cur.png")]

        async def _hang(images, limit=adapter.MAX_INBOUND_IMAGES, timeout=None):
            await asyncio.sleep(60)
            return []

        current = asyncio.ensure_future(_slow_current([]))
        quoted = asyncio.ensure_future(_hang([]))
        with mock.patch.object(adapter, "QUOTED_IMAGES_TOTAL_TIMEOUT", 0.05), \
                self.assertLogs(adapter.logger, level="WARNING"):
            staged, quoted_staged = await a._join_quoted_batch(current, quoted)
        self.assertTrue(slow_started.is_set())
        self.assertEqual(staged, [("/tmp/cur.png", "cur.png")])
        self.assertEqual(quoted_staged, [])

    async def test_get_chat_info_unseen_is_dm_fallback(self):
        a = _make_adapter()
        info = await a.get_chat_info("0:fs:abcdef1234567890:")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "abcdef12")
        self.assertEqual(info["chat_id"], "0:fs:abcdef1234567890:")

    async def test_dm_chat_name_uses_sender_name(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "d1", "chat_id": "0:fs:dm123:", "chat_type": "direct",
            "from": {"id": "8017", "name": "张三"}, "ea": "fs",
            "message": {"type": "text", "content": "hi"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].source.chat_name, "私聊 张三")
        info = await a.get_chat_info("0:fs:dm123:")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "私聊 张三")

    async def test_dm_chat_name_falls_back_to_user_id(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "d2", "chat_id": "0:fs:dm456:", "chat_type": "direct",
            "from": {"id": "8017", "name": ""}, "ea": "fs",
            "message": {"type": "text", "content": "hi"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].source.chat_name, "私聊 E.fs.8017")

    async def test_group_chat_name_is_readable(self):
        a = _make_adapter()
        captured = {}

        async def _capture(event):
            captured["event"] = event

        a.handle_message = _capture
        a._stage_images = mock.AsyncMock(return_value=[])
        a._do_send = mock.AsyncMock(return_value={"success": True})
        a._home_channel_set = lambda: True
        payload = {"type": "message", "data": {
            "message_id": "g1", "chat_id": "0:fs:d3058fc2e0cb4d389c91b9c33b09658f:", "chat_type": "group",
            "from": {"id": "8017", "name": "李四"}, "ea": "fs",
            "message": {"type": "text", "content": "hi"},
        }}
        await a._handle_message(payload)
        self.assertEqual(captured["event"].source.chat_name, "群聊 d3058fc2")
        self.assertEqual(captured["event"].source.chat_type, "group")
        info = await a.get_chat_info("0:fs:d3058fc2e0cb4d389c91b9c33b09658f:")
        self.assertEqual(info["type"], "group")
        self.assertEqual(info["name"], "群聊 d3058fc2")

    async def test_chat_name_cache_is_bounded(self):
        a = _make_adapter()
        for i in range(adapter.CHAT_NAME_CACHE_MAX + 20):
            a._remember_chat(f"0:fs:{i}:", f"name{i}", "dm")
        self.assertLessEqual(len(a._chat_meta), adapter.CHAT_NAME_CACHE_MAX)
        self.assertNotIn("0:fs:0:", a._chat_meta)

    async def test_format_history_limits(self):
        a = _make_adapter()
        history = [
            {"content": f"c{i}", "sender_id": "E.fs.1", "message_timestamp": i}
            for i in range(50)
        ]
        text = a._format_history(history)
        self.assertLessEqual(len(text), adapter.HISTORY_MAX_CHARS + len(adapter.HISTORY_HEADER) + 1)
        self.assertIn("c49", text)
        self.assertTrue(text.startswith(adapter.HISTORY_HEADER))

    async def test_send_updates_self_watermark(self):
        a = _make_adapter()
        a._client_session = object()
        a._ensure_token = mock.AsyncMock(return_value=True)
        with mock.patch.object(adapter, "_post_text", mock.AsyncMock(return_value=(True, "m1", 0, ""))):
            result = await a._do_send("0:fs:grp:", "hi")
        self.assertTrue(result["success"])
        self.assertGreater(a._last_self_ts.get("0:fs:grp:", 0), 0)


class _FakeImageResp:
    """单张图片下载的假响应：只实现 _stage_images 用到的 status / read。"""

    def __init__(self, status: int = 200, body: bytes = b"img"):
        self.status = status
        self._body = body

    async def read(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeImageSession:
    """按调用顺序返回预置响应，并记录每次 get 的 url 与 timeout。"""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        resp = self._responses[min(len(self.calls) - 1, len(self._responses) - 1)]
        if isinstance(resp, Exception):
            raise resp
        return resp


class ImageStagingLogTests(unittest.IsolatedAsyncioTestCase):
    """图片下载耗时日志：只在 debug 级，且不改动下载结果。"""

    async def test_single_and_batch_timing_logged_without_changing_result(self):
        a = _make_adapter()
        session = _FakeImageSession([_FakeImageResp(200), _FakeImageResp(403)])
        a._client_session = session
        a._cache_image = mock.AsyncMock(return_value="/tmp/ok.png")
        images = [
            {"url": "https://x/a.png", "filename": "a.png"},
            {"url": "https://x/b.png", "filename": "b.png"},
        ]
        with self.assertLogs(adapter.logger, level="DEBUG") as logs:
            staged = await a._stage_images(images)
        # 行为不变：成功的进 media，403 的跳过
        self.assertEqual(staged, [("/tmp/ok.png", "a.png")])
        self.assertEqual(len(session.calls), 2)
        joined = "\n".join(logs.output)
        self.assertIn("inbound image single took", joined)
        self.assertIn("inbound image batch took", joined)
        self.assertIn("outcome=downloaded", joined)
        self.assertIn("outcome=status403", joined)
        self.assertIn("tried=2", joined)
        self.assertIn("staged=1", joined)
        # 超时值带在日志里，真机能分清走的是 15s（当前消息）还是 5s（引用图）
        self.assertIn("timeout=15.0s", joined)

    async def test_quoted_path_logs_its_own_timeout(self):
        a = _make_adapter()
        a._client_session = _FakeImageSession([_FakeImageResp(200)])
        a._cache_image = mock.AsyncMock(return_value="/tmp/q.png")
        with self.assertLogs(adapter.logger, level="DEBUG") as logs:
            staged = await a._fetch_quoted_images([{"url": "https://x/q.png", "filename": "q.png"}])
        self.assertEqual(staged, [("/tmp/q.png", "q.png")])
        joined = "\n".join(logs.output)
        self.assertIn("timeout=5.0s", joined)
        self.assertIn("limit=4", joined)


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_post_text_success(self):
        session = _FakeSession([_FakeResp({"code": 0, "data": {"message_id": "9"}})])
        ok, mid, code, err = await adapter._post_text(session, "https://x", "t", "c", "hi")
        self.assertTrue(ok)
        self.assertEqual(mid, "9")
        self.assertEqual(code, 0)

    async def test_post_text_business_error(self):
        session = _FakeSession([_FakeResp({"code": 50001, "msg": "Bot not connected"})])
        ok, mid, code, err = await adapter._post_text(session, "https://x", "t", "c", "hi")
        self.assertFalse(ok)
        self.assertEqual(code, 50001)
        self.assertEqual(err, "Bot not connected")

    async def test_post_text_non_json_is_network_error(self):
        session = _FakeSession([_FakeResp(ValueError("not json"), status=502)])
        ok, mid, code, err = await adapter._post_text(session, "https://x", "t", "c", "hi")
        self.assertFalse(ok)
        self.assertEqual(code, -1)
        self.assertIn("502", err)

    async def test_request_token_retries_then_succeeds(self):
        session = _FakeSession([
            _FakeResp({"code": 50000, "msg": "boom"}),
            _FakeResp({"code": 0, "data": {"accessToken": "tok", "expiresIn": 7200}}),
        ])
        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
            token, expires, err, retryable = await adapter._request_token(session, "https://x", "a", "b")
        self.assertEqual(token, "tok")
        self.assertEqual(expires, 7200)
        self.assertEqual(session.calls, 2)

    async def test_request_token_non_retryable(self):
        session = _FakeSession([_FakeResp({"code": 40004, "msg": "Account disabled"})])
        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
            token, expires, err, retryable = await adapter._request_token(session, "https://x", "a", "b")
        self.assertIsNone(token)
        self.assertFalse(retryable)
        self.assertEqual(session.calls, 1)


class StandaloneTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._saved = {k: os.environ.pop(k, None) for k in ("SHARECRM_APP_ID", "SHARECRM_APP_SECRET")}

    def tearDown(self):
        for k, v in self._saved.items():
            if v is not None:
                os.environ[k] = v

    async def test_missing_chat_id(self):
        result = await adapter._standalone_send(PlatformConfig(extra={}), "  ", "hi")
        self.assertIn("error", result)

    async def test_missing_credentials(self):
        result = await adapter._standalone_send(PlatformConfig(extra={}), "c", "hi")
        self.assertIn("error", result)
        self.assertIn("SHARECRM_APP_ID", result["error"])

    async def test_control_characters_rejected(self):
        result = await adapter._standalone_send(PlatformConfig(extra={}), "a\nb", "hi")
        self.assertIn("error", result)


class CompatTests(unittest.TestCase):
    def test_extra_or_secret_precedence(self):
        with mock.patch.dict(os.environ, {"SHARECRM_X": "env"}, clear=False):
            self.assertEqual(compat.extra_or_secret({"x": "yaml"}, "x", "SHARECRM_X"), "env")
        os.environ.pop("SHARECRM_X", None)
        self.assertEqual(compat.extra_or_secret({"x": "yaml"}, "x", "SHARECRM_X"), "yaml")
        self.assertEqual(compat.extra_or_secret({}, "x", "SHARECRM_X", "dflt"), "dflt")

    def test_apply_yaml_bridge_fallback_shapes(self):
        for key in ("SHARECRM_BASE_URL", "SHARECRM_ALLOWED_USERS", "SHARECRM_ALLOW_ALL_USERS"):
            os.environ.pop(key, None)
        out = compat.apply_yaml_bridge(
            {"base_url": "https://y", "allowed_users": ["a", "b"], "allow_all_users": True},
            adapter._YAML_SPEC,
        )
        self.assertEqual(out["base_url"], "https://y")
        self.assertEqual(out["allowed_users"], ["a", "b"])
        self.assertEqual(out["allow_all_users"], True)
        if not compat.HAS_SHARED:
            self.assertEqual(os.environ["SHARECRM_BASE_URL"], "https://y")
            self.assertEqual(os.environ["SHARECRM_ALLOWED_USERS"], "a,b")
            self.assertEqual(os.environ["SHARECRM_ALLOW_ALL_USERS"], "true")

    def test_compat_delegates_to_shared_when_present(self):
        """_shared 可用时兼容层应直接复用官方实现（不关心真实运行时是否真有）。"""
        import types

        fake = types.ModuleType("fake_shared")
        fake.get_scoped_secret = lambda name, default=None, **kw: "scoped" if name == "SHARECRM_X" else default
        fake.extra_or_secret = lambda extra, key, env, default="": "shared-" + key
        fake.seed_extra_from_env = lambda spec, **kw: {"from": "shared"}
        fake.apply_yaml_bridge = lambda cfg, spec: {"from": "shared"}
        fake.env_is_connected = lambda *names: (lambda cfg: True)
        fake.send_error = lambda message: {"error": str(message), "redacted": True}
        with mock.patch.object(compat, "_shared", fake), mock.patch.object(compat, "HAS_SHARED", True):
            self.assertEqual(compat.get_secret("SHARECRM_X"), "scoped")
            self.assertEqual(compat.extra_or_secret({}, "k", "ENV"), "shared-k")
            self.assertEqual(compat.seed_extra_from_env([]), {"from": "shared"})
            self.assertEqual(compat.apply_yaml_bridge({}, []), {"from": "shared"})
            self.assertTrue(compat.send_error("x")["redacted"])

    def test_compat_falls_back_when_shared_absent(self):
        """_shared 不可用时（旧运行时）应走等价的 os.environ 兜底。"""
        with mock.patch.object(compat, "_shared", None), mock.patch.object(compat, "HAS_SHARED", False):
            with mock.patch.dict(os.environ, {"SHARECRM_X": "env"}, clear=False):
                self.assertEqual(compat.get_secret("SHARECRM_X"), "env")
                self.assertEqual(compat.extra_or_secret({"x": "yaml"}, "x", "SHARECRM_X"), "env")
            os.environ.pop("SHARECRM_X", None)
            self.assertEqual(compat.extra_or_secret({"x": "yaml"}, "x", "SHARECRM_X"), "yaml")
            self.assertEqual(compat.send_error("x"), {"error": "x"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
