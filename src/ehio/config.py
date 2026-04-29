"""Config loading and editing for ehio."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

PACKAGE_DIR = Path(__file__).parent
CONFIG_PATH = PACKAGE_DIR / "data" / "config.yaml"


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def get(key: str, default: Any = None) -> Any:
    return load_config().get(key, default)


def require(key: str, label: str | None = None) -> str:
    value = get(key, "").strip()
    if not value:
        name = label or key
        print(
            f"Error: {name} is not configured. "
            f"Set {key} in config (ehio config --edit) or pass it as a CLI flag.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def view_config() -> int:
    if not CONFIG_PATH.exists():
        print(f"Error: config not found at {CONFIG_PATH}", file=sys.stderr)
        return 1
    print(CONFIG_PATH.resolve())
    print()
    sys.stdout.write(CONFIG_PATH.read_text(encoding="utf-8"))
    return 0


def edit_config() -> int:
    if not CONFIG_PATH.exists():
        print(f"Error: config not found at {CONFIG_PATH}", file=sys.stderr)
        return 1
    cmd: list[str] = []
    for env_var in ("VISUAL", "EDITOR"):
        val = os.environ.get(env_var, "").strip()
        if val:
            cmd = shlex.split(val)
            break
    if not cmd:
        for candidate in ("nano", "vim", "vi"):
            if shutil.which(candidate):
                cmd = [candidate]
                break
    if not cmd:
        print("Error: no terminal editor found. Set $VISUAL or $EDITOR.", file=sys.stderr)
        return 1
    try:
        subprocess.run([*cmd, str(CONFIG_PATH)], check=True)
    except subprocess.CalledProcessError as exc:
        return exc.returncode or 1
    return 0
