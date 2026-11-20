"""pytest 会话引导：让插件的包内相对导入在测试里可用。

背景：插件根目录本身就是 Hermes 插件包（``__init__.py`` 里是
``from .adapter import register``）。直接在仓库根跑 ``pytest tests/`` 有两处坑：

1. 本机 Python 里通常没有 ``hermes-agent``，用例顶部 ``import gateway`` 会 SkipTest。
   这里不伪造 gateway：若导入失败，把 Hermes 运行时仓库挂到 ``sys.path``
   （``HERMES_AGENT_HOME`` 优先，其次本机默认安装位置）。找不到就交给用例自己 Skip。
2. pytest 会把插件根目录收成 ``Package`` 节点，并在每个用例 setup 阶段执行
   ``import_module("__init__")``；插件 ``__init__.py`` 是相对导入，于是整轮用例全部
   ERROR（``attempted relative import with no known parent package``）。
   实测 pytest 9.1 读的是运行期 ``--import-mode`` option，改收集节点属性无效；改成
   ``raw`` 又会让测试文件按文件名导入、用例里的 ``import gateway`` 失效。
   所以这里用惰性代理预先占用 ``sys.modules["__init__"]``：模块本身没有相对导入，
   真要用 ``register`` 时才去导入 adapter，pytest 的 ``Package.setup()`` 因此能通过。
   unittest discover 不走本文件，同样的兜底在用例文件顶部（见
   ``test_sharecrm_adapter.py`` 的 import 段）。

完整跑法见 README「测试」一节。
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
PKG_NAME = "hermes_plugins.sharecrm_under_test"

_FALLBACK_RUNTIME_DIRS = (
    Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent",
    Path.home() / ".hermes" / "hermes-agent",
)


def _ensure_gateway() -> None:
    try:
        import gateway  # noqa: F401
    except ImportError:
        pass
    else:
        return
    candidates = [os.getenv("HERMES_AGENT_HOME") or ""] + [str(p) for p in _FALLBACK_RUNTIME_DIRS]
    for raw in candidates:
        if not raw:
            continue
        root = Path(raw).expanduser()
        if not (root / "gateway" / "__init__.py").is_file():
            continue
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        try:
            import gateway  # noqa: F401
        except ImportError:
            sys.path.remove(str(root))
            continue
        return


def _load_plugin_package() -> None:
    """按 Hermes 的加载方式把插件目录当包导入（相对 import 才能生效）。"""
    if PKG_NAME in sys.modules:
        return
    init_file = PLUGIN_DIR / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        PKG_NAME, init_file, submodule_search_locations=[str(PLUGIN_DIR)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[PKG_NAME] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        # 加载失败（例如确实没有 hermes 运行时）时交给用例自己 SkipTest，
        # 但要把半初始化的包摘掉，避免后续再误用。
        sys.modules.pop(PKG_NAME, None)
        raise


class _RootInitShim:
    """``sys.modules["__init__"]`` 占位：给 pytest 的 ``Package.setup()`` 兜底。

    真文件是 ``from .adapter import register``，脱离包上下文必挂；这里只转发
    ``register``（用时才加载插件包），其余属性按未定义处理。
    """

    __file__ = None
    __name__ = "__init__"

    def __getattr__(self, name):
        if name == "register":
            _load_plugin_package()
            return importlib.import_module(PKG_NAME + ".adapter").register
        raise AttributeError(name)


_ensure_gateway()
try:
    _load_plugin_package()
    importlib.import_module(PKG_NAME + ".adapter")
except Exception:  # pragma: no cover - 无运行时环境
    pass

if sys.modules.get("__init__") is None:
    sys.modules["__init__"] = _RootInitShim()  # type: ignore[assignment]
