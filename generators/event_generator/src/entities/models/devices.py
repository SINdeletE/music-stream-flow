"""Backward-compatible names backed by the shared smartphone catalog."""

import json
from pathlib import Path

_CATALOG = Path(__file__).resolve().parents[4] / "catalogs" / "devices.json"
devices = [entry["name"] for entry in json.loads(_CATALOG.read_text(encoding="utf-8"))]
