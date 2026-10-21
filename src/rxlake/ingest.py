"""Download monthly EPD files from the NHSBSA open data portal (CKAN).

Each month is published as a resource called EPD_YYYYMM inside the
english-prescribing-data-epd dataset. A month is roughly 17m rows / 6GB of CSV,
so we stream it to disk rather than holding it in memory.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path

import requests

from rxlake.config import Settings

log = logging.getLogger(__name__)


class SourceNotFound(RuntimeError):
    pass


def list_resources(settings: Settings, session: requests.Session | None = None) -> list[dict]:
    session = session or requests.Session()
    url = f"{settings.ckan_api.rstrip('/')}/package_show"
    resp = session.get(url, params={"id": settings.dataset_id}, timeout=settings.timeout_seconds)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"CKAN returned success=false for {settings.dataset_id}")
    return body["result"]["resources"]


def find_month_resource(resources: list[dict], year_month: int, prefix: str = "EPD_") -> dict:
    wanted = f"{prefix}{year_month}"
    matches = [
        r for r in resources
        if (r.get("name") or "").upper() == wanted and (r.get("format") or "").upper() == "CSV"
    ]
    if not matches:
        raise SourceNotFound(f"{wanted} isn't published yet (or the name changed)")
    # Very occasionally a month gets republished - take the newest
    return max(matches, key=lambda r: r.get("last_modified") or r.get("created") or "")


def available_months(resources: list[dict], prefix: str = "EPD_") -> list[int]:
    months = set()
    for r in resources:
        name = (r.get("name") or "").upper()
        if name.startswith(prefix) and name[len(prefix):].isdigit():
            months.add(int(name[len(prefix):]))
    return sorted(months)


def download(url: str, dest: str | os.PathLike, timeout: int = 60,
             session: requests.Session | None = None) -> Path:
    """Stream url to dest. Writes to a temp file first so a dropped
    connection never leaves a half-written CSV that looks complete."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    session = session or requests.Session()

    with session.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        fd, tmp = tempfile.mkstemp(dir=dest.parent, suffix=".part")
        try:
            with os.fdopen(fd, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)
            shutil.move(tmp, dest)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
    return dest


def ingest_month(settings: Settings, year_month: int, force: bool = False,
                 session: requests.Session | None = None) -> Path:
    dest = Path(settings.landing_file(year_month))
    if dest.exists() and dest.stat().st_size > 0 and not force:
        log.info("%s already in landing, skipping download", dest.name)
        return dest

    resource = find_month_resource(
        list_resources(settings, session), year_month, settings.resource_prefix
    )
    log.info("downloading %s from %s", resource["name"], resource["url"])
    return download(resource["url"], dest, settings.timeout_seconds, session)
