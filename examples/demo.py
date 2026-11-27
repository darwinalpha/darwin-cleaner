"""Run every example script under examples/ in order.

Usage:
    python examples/demo.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

SCRIPTS = [
    "01_basic.py",
    "02_parquet.py",
    "03_forks.py",
    "04_middleware.py",
    "05_dedupe.py",
]


def run(name: str) -> None:
    script = HERE / name
    print(f"\n{'=' * 60}\n=== {name} ===\n{'=' * 60}")
    spec = importlib.util.spec_from_file_location(name[:-3], script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()


def main() -> None:
    for name in SCRIPTS:
        run(name)


if __name__ == "__main__":
    main()
