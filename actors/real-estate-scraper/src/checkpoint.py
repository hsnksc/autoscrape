"""
Checkpoint (ilerleme kaydı) yardımcıları.
JSON dosyasına yazarak kaldığı yerden devam etmeyi destekler.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_checkpoint(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
