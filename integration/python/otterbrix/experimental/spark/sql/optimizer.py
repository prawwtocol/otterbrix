import importlib.util
import sys
from pathlib import Path

_path = Path(__file__).resolve().parent / ".archive" / "optimizer.py"
_spec = importlib.util.spec_from_file_location(__name__, _path)
_module = importlib.util.module_from_spec(_spec)
sys.modules[__name__] = _module
_spec.loader.exec_module(_module)
