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

try:  # 这些测试依赖 hermes-agent 运行时
    import gateway  # noqa: F401
except ImportError as exc:  # pragma: no cover
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
