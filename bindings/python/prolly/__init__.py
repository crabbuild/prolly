"""Public Python wrapper for the generated Prolly UniFFI bindings."""

from importlib import import_module

try:
    _generated = import_module(".uniffi.prolly", __name__)
except ModuleNotFoundError as exc:
    if exc.name not in {
        f"{__name__}.uniffi",
        f"{__name__}.uniffi.prolly",
    }:
        raise

    from src import *  # type: ignore  # noqa: F401,F403
    from src import __all__ as __all__  # type: ignore  # noqa: F401
except (ImportError, OSError):
    from src import *  # type: ignore  # noqa: F401,F403
    from src import __all__ as __all__  # type: ignore  # noqa: F401
else:
    from .uniffi import *  # type: ignore  # noqa: F401,F403,E402

    __all__ = _generated.__all__
