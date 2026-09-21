"""Hermes 运行时兼容层。

`gateway.platforms._shared` 是 Hermes 在 2026-09-02（commit 661fc669a0）才引入的
跨适配器配置工具，提供 profile-scoped 的密钥读取（multiplex 安全）。本插件要同时
支持新旧运行时，所以在这里做一次能力探测：

- 新运行时：直接复用官方实现。官方在 multiplex 下会隔离到当前 profile 的 secret
  scope，绝不串读别的 profile。
- 旧运行时（如 0.21.0 / 2026-09-01）：退回等价的 os.environ 实现。旧运行时没有
  这套 scope 机制，行为与历史版本一致。

为什么把差异收敛在这一层：官方明确要求 multiplex 场景下不能裸读 os.environ，否则
会用错 profile 的凭证。adapter 只调这里，保证将来官方接口变动时只改一个文件。
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Iterable, Optional

try:  # Hermes >= 2026-09-02
    from gateway.platforms import _shared as _shared  # type: ignore
except Exception:  # pragma: no cover - 旧运行时没有该模块
    _shared = None

HAS_SHARED = _shared is not None


def get_secret(name: str, default: Any = "") -> Any:
    """读一个 profile-scoped 的配置/密钥；旧运行时退回 os.getenv。

    不走 `_shared.get_scoped_secret` 的 ``external_fallback``：本插件只在适配器/
    注册期读取，外部托管凭证（Bitwarden 等）由 core 的启动检查负责。
    """
    if _shared is not None:
        try:
            val = _shared.get_scoped_secret(name, None)
            return default if val is None else val
        except Exception:
            pass
    val = os.getenv(name)
    return default if val is None else val


def extra_or_secret(extra: Optional[dict], key: str, env: str, default: Any = "") -> Any:
    """env（profile-scoped）优先，其次 YAML/config ``extra[key]``，最后 ``default``。

    与官方 ``_shared.extra_or_secret`` 语义一致：空白 env 视为未设置；显式
    ``False``/``0`` 是真实值。
    """
    if _shared is not None:
        try:
            return _shared.extra_or_secret(extra, key, env, default)
        except Exception:
            pass
    env_value = get_secret(env, None)
    if env_value is not None and str(env_value).strip():
        return env_value
    value = (extra or {}).get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    return value


def seed_extra_from_env(
    spec: Iterable[tuple], *, home_env: Optional[str] = None, home_default: str = ""
) -> dict:
    """表驱动地从 env 播种 ``PlatformConfig.extra``（供 ``env_enablement_fn``）。

    ``spec`` 每项为 ``(ENV_VAR, extra_key, conv)``；``conv`` 抛 ``ValueError`` 时跳过。
    ``home_env`` 存在时额外播种 ``home_channel``（官方 core 会把它提升为 HomeChannel）。
    """
    if _shared is not None:
        try:
            return _shared.seed_extra_from_env(spec, home_env=home_env, home_default=home_default)
        except Exception:
            pass
    seed: dict = {}
    for env, key, conv in spec:
        raw = str(get_secret(env, "") or "").strip()
        if not raw:
            continue
        try:
            seed[key] = conv(raw) if conv else raw
        except (ValueError, TypeError):
            continue
    if home_env:
        home = str(get_secret(home_env, "") or "").strip() or home_default
        if home:
            seed["home_channel"] = {
                "chat_id": home,
                "name": str(get_secret(f"{home_env}_NAME", "Home") or "Home"),
            }
    return seed


def _yaml_applies(kind: str, cfg: dict, key: str) -> bool:
    if kind in ("lower", "json"):
        return key in cfg
    if kind == "str":
        return cfg.get(key) not in (None, "")
    if kind == "csv":
        return cfg.get(key) is not None
    return False


def _yaml_encode(kind: str, value: Any) -> Any:
    if kind == "lower":
        return str(value).lower()
    if kind == "json":
        return json.dumps(value)
    return value


def apply_yaml_bridge(cfg: dict, spec: Iterable[tuple]) -> Optional[dict]:
    """表驱动的 YAML→env/config 桥（供 ``apply_yaml_config_fn``）。

    ``spec`` 每项为 ``(yaml_key, ENV_VAR, kind)``，``kind`` ∈ {str, lower, csv, json}。
    新运行时复用官方实现（env 优先、secondary profile 下跳过 env 写入、播种 extra）。
    旧运行时兜底：仅在 env 未设置时写入（保持 env > YAML），并回填 extra。
    """
    if _shared is not None:
        try:
            return _shared.apply_yaml_bridge(cfg, spec)
        except Exception:
            pass
    seeded: dict = {}
    for key, env, kind in spec:
        if not _yaml_applies(kind, cfg, key):
            continue
        seeded[key] = cfg[key]
        if not os.getenv(env):
            encoded = _yaml_encode(kind, cfg[key])
            if isinstance(encoded, list):
                encoded = ",".join(str(v) for v in encoded)
            os.environ[env] = str(encoded)
    return seeded or None


def env_is_connected(*names: str) -> Callable[[Any], bool]:
    """返回一个 ``is_connected(config)`` 判定：给定 env 变量全部非空即为已连接。"""
    if _shared is not None:
        try:
            return _shared.env_is_connected(*names)
        except Exception:
            pass

    def _check(config: Any = None) -> bool:
        try:
            import hermes_cli.gateway as gateway_mod

            return all((gateway_mod.get_env_value(name) or "").strip() for name in names)
        except Exception:
            return all(str(os.getenv(name) or "").strip() for name in names)

    return _check


def send_error(message: Any) -> dict:
    """构造 standalone 发送失败信封；新运行时复用官方的脱敏实现。"""
    if _shared is not None:
        try:
            return _shared.send_error(message)
        except Exception:
            pass
    return {"error": str(message)}
