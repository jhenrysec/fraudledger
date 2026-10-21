from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from fraudledger.hashing import sha256_text


def _deep_merge(base: dict, overlay: dict) -> dict:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def package_config_text() -> str:
    path = Path(__file__).with_name("default_config.yaml")
    return path.read_text(encoding="utf-8")


def discover_config_path(explicit: str | None = None) -> Path | None:
    if explicit:
        return Path(explicit)
    env = os.environ.get("FRAUDLEDGER_CONFIG")
    if env:
        return Path(env)
    system = Path("/etc/fraudledger/config.yaml")
    if system.is_file():
        return system
    return None


@dataclass
class Settings:
    data_dir: Path
    bind_host: str
    bind_port: int
    min_peer_n: int
    min_ownership_pct: float
    louvain_resolution: float
    louvain_seed: int
    cluster_bonus: float
    multi_flag_bonus: float
    multi_flag_min: int
    tier_critical: float
    tier_high: float
    tier_elevated: float
    sources: dict[str, Any]
    rules: dict[str, Any]
    role_strength: dict[str, float]
    default_role_strength: float
    control_roles: list[str]
    config_path: Path | None
    config_text: str
    raw: dict[str, Any] = field(repr=False)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "warehouse" / "ledger.duckdb"

    @property
    def config_sha256(self) -> str:
        return sha256_text(self.config_text)

    def ensure_dirs(self) -> None:
        for relative in (
            "raw",
            "parquet/facts",
            "warehouse",
            "runs",
            "exports",
            "tmp",
            "logs",
        ):
            (self.data_dir / relative).mkdir(parents=True, exist_ok=True)


def load_settings(
    config: str | None = None,
    data_dir: str | None = None,
) -> Settings:
    default_text = package_config_text()
    default = yaml.safe_load(default_text) or {}
    path = discover_config_path(config)
    user_text = ""
    user: dict[str, Any] = {}
    if path is not None:
        if not path.is_file():
            raise SystemExit(f"Config file not found: {path}")
        user_text = path.read_text(encoding="utf-8")
        user = yaml.safe_load(user_text) or {}
    merged = _deep_merge(default, user)
    if data_dir:
        merged["data_dir"] = data_dir
    elif os.environ.get("FRAUDLEDGER_DATA_DIR"):
        merged["data_dir"] = os.environ["FRAUDLEDGER_DATA_DIR"]
    config_text = user_text if user_text else default_text
    strength = {str(k): float(v) for k, v in (merged.get("role_strength") or {}).items()}
    return Settings(
        data_dir=Path(merged["data_dir"]),
        bind_host=str(merged.get("bind_host", "127.0.0.1")),
        bind_port=int(merged.get("bind_port", 8088)),
        min_peer_n=int(merged.get("min_peer_n", 30)),
        min_ownership_pct=float(merged.get("min_ownership_pct", 5)),
        louvain_resolution=float(merged.get("louvain_resolution", 1.0)),
        louvain_seed=int(merged.get("louvain_seed", 42)),
        cluster_bonus=float(merged.get("cluster_bonus", 1.5)),
        multi_flag_bonus=float(merged.get("multi_flag_bonus", 2.0)),
        multi_flag_min=int(merged.get("multi_flag_min", 3)),
        tier_critical=float(merged.get("tier_critical", 8)),
        tier_high=float(merged.get("tier_high", 5)),
        tier_elevated=float(merged.get("tier_elevated", 3)),
        sources=dict(merged.get("sources") or {}),
        rules=dict(merged.get("rules") or {}),
        role_strength=strength,
        default_role_strength=float(merged.get("default_role_strength", 1.0)),
        control_roles=[str(x) for x in (merged.get("control_roles") or [])],
        config_path=path,
        config_text=config_text,
        raw=merged,
    )
