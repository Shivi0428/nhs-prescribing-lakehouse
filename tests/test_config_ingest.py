import pytest

from rxlake import ingest
from rxlake.config import Settings, load_settings, parse_month


@pytest.mark.parametrize("value,expected", [("202401", 202401), ("2024-01", 202401), (202312, 202312)])
def test_parse_month_ok(value, expected):
    assert parse_month(value) == expected


@pytest.mark.parametrize("value", ["2024-13", "24-01", "abc", "202400"])
def test_parse_month_rejects_garbage(value):
    with pytest.raises(ValueError):
        parse_month(value)


def test_env_overrides_yaml(monkeypatch, tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("lake_root: ./from_yaml\n")
    monkeypatch.setenv("RXLAKE_LAKE_ROOT", "/data/lake")
    s = load_settings(cfg)
    assert s.lake_root == "/data/lake"
    assert s.silver_epd == "/data/lake/silver/epd"


def test_landing_file_name():
    assert Settings(landing_root="/l/").landing_file(202401) == "/l/epd/EPD_202401.csv"


RESOURCES = [
    {"name": "EPD_202401", "format": "CSV", "url": "http://x/1", "last_modified": "2024-03-01"},
    {"name": "EPD_202401", "format": "CSV", "url": "http://x/1b", "last_modified": "2024-04-01"},
    {"name": "EPD_202402", "format": "CSV", "url": "http://x/2"},
    {"name": "EPD_202402", "format": "JSON", "url": "http://x/2j"},
    {"name": "EPD_SNOMED_202402", "format": "CSV", "url": "http://x/s"},
]


def test_find_month_takes_newest_republish():
    assert ingest.find_month_resource(RESOURCES, 202401)["url"] == "http://x/1b"


def test_find_month_ignores_non_csv():
    assert ingest.find_month_resource(RESOURCES, 202402)["url"] == "http://x/2"


def test_find_month_missing():
    with pytest.raises(ingest.SourceNotFound):
        ingest.find_month_resource(RESOURCES, 209912)


def test_available_months_skips_other_series():
    assert ingest.available_months(RESOURCES) == [202401, 202402]


def test_download_is_atomic(tmp_path):
    class Boom:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield b"abc"
            raise ConnectionError("dropped")

    class Sess:
        def get(self, *a, **k):
            return Boom()

    dest = tmp_path / "out" / "f.csv"
    with pytest.raises(ConnectionError):
        ingest.download("http://x", dest, session=Sess())
    assert not dest.exists()
    assert list(dest.parent.glob("*.part")) == []
