from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass
class Settings:
    lake_root: str = "./lake"
    landing_root: str = "./landing"
    ckan_api: str = "https://opendata.nhsbsa.net/api/3/action"
    dataset_id: str = "english-prescribing-data-epd"
    resource_prefix: str = "EPD_"
    timeout_seconds: int = 60
    ods_practices_path: str | None = None
    fail_on_error: bool = True
    max_reject_ratio: float = 0.01
    shuffle_partitions: int = 8
    extra: dict = field(default_factory=dict)

    # table locations -------------------------------------------------------
    def table(self, layer: str, name: str) -> str:
        return f"{self.lake_root.rstrip('/')}/{layer}/{name}"

    @property
    def bronze_epd(self) -> str:
        return self.table("bronze", "epd")

    @property
    def silver_epd(self) -> str:
        return self.table("silver", "epd")

    @property
    def silver_rejects(self) -> str:
        return self.table("silver", "epd_rejects")

    def gold(self, name: str) -> str:
        return self.table("gold", name)

    def landing_file(self, year_month: int) -> str:
        return f"{self.landing_root.rstrip('/')}/epd/{self.resource_prefix}{year_month}.csv"


def load_settings(path: str | os.PathLike | None = None) -> Settings:
    path = Path(path or os.getenv("RXLAKE_CONFIG", DEFAULT_CONFIG))
    raw = yaml.safe_load(path.read_text()) if path.exists() else {}

    source = raw.get("source", {})
    quality = raw.get("quality", {})
    s = Settings(
        lake_root=raw.get("lake_root", Settings.lake_root),
        landing_root=raw.get("landing_root", Settings.landing_root),
        ckan_api=source.get("ckan_api", Settings.ckan_api),
        dataset_id=source.get("dataset_id", Settings.dataset_id),
        resource_prefix=source.get("resource_prefix", Settings.resource_prefix),
        timeout_seconds=int(source.get("timeout_seconds", Settings.timeout_seconds)),
        ods_practices_path=raw.get("lookups", {}).get("ods_practices_path"),
        fail_on_error=bool(quality.get("fail_on_error", True)),
        max_reject_ratio=float(quality.get("max_reject_ratio", Settings.max_reject_ratio)),
        shuffle_partitions=int(raw.get("spark", {}).get("shuffle_partitions", 8)),
    )

    # env vars win over the yaml so the same file works locally and in the cloud
    for name in ("lake_root", "landing_root", "ckan_api", "ods_practices_path"):
        env = os.getenv(f"RXLAKE_{name.upper()}")
        if env:
            setattr(s, name, env)
    if os.getenv("RXLAKE_FAIL_ON_ERROR"):
        s.fail_on_error = os.environ["RXLAKE_FAIL_ON_ERROR"].lower() in ("1", "true", "yes")
    return s


_MONTH = re.compile(r"^(\d{4})-?(\d{2})$")


def parse_month(value: str | int) -> int:
    """Accept 202401, '202401' or '2024-01' and return 202401."""
    m = _MONTH.match(str(value).strip())
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError(f"not a valid month: {value!r} (expected YYYYMM or YYYY-MM)")
    return int(m.group(1) + m.group(2))
