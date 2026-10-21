"""End-to-end test on a tiny made-up file. Needs Java, so skipped locally
if it's missing and run in CI."""

import csv

import pytest

from rxlake import pipeline
from rxlake.config import Settings
from tests.conftest import SAMPLE_COLUMNS, sample_rows

pytestmark = pytest.mark.spark


@pytest.fixture()
def settings(tmp_path):
    s = Settings(lake_root=str(tmp_path / "lake"), landing_root=str(tmp_path / "landing"))
    path = tmp_path / "landing" / "epd"
    path.mkdir(parents=True)
    with open(path / "EPD_202401.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(SAMPLE_COLUMNS)
        w.writerows(sample_rows())
    s.max_reject_ratio = 0.5   # the sample is deliberately dirty
    return s


def test_bronze_keeps_every_row(spark, settings):
    assert pipeline.run_bronze(spark, settings, 202401) == 5


def test_silver_cleans_and_rejects(spark, settings):
    pipeline.run_bronze(spark, settings, 202401)
    out = pipeline.run_silver(spark, settings, 202401)
    # 5 in: 1 exact dup collapses -> 2 clean, 2 rejected
    assert out["clean"] == 2
    assert out["rejected"] == 2

    rows = spark.read.format("delta").load(settings.silver_epd).collect()
    assert {r.postcode for r in rows} == {"TS1 1AB"}
    assert {r.practice_code for r in rows} == {"A81001", "A81002"}

    reasons = {r.reject_reason for r in spark.read.format("delta").load(settings.silver_rejects).collect()}
    assert reasons == {"bad_practice_code", "bad_bnf_code"}


def test_silver_is_idempotent(spark, settings):
    pipeline.run_bronze(spark, settings, 202401)
    pipeline.run_silver(spark, settings, 202401)
    pipeline.run_silver(spark, settings, 202401)
    assert spark.read.format("delta").load(settings.silver_epd).count() == 2


def test_gold_star_schema(spark, settings):
    pipeline.run_bronze(spark, settings, 202401)
    pipeline.run_silver(spark, settings, 202401)
    counts = pipeline.run_gold(spark, settings)
    assert counts["dim_practice"] == 2
    assert counts["dim_drug"] == 2
    assert counts["dim_date"] == 1
    assert counts["fact_prescribing"] == 2

    dim_date = spark.read.format("delta").load(settings.gold("dim_date")).first()
    assert dim_date.financial_year == "2023/24"

    chapters = {
        r.bnf_chapter_name
        for r in spark.read.format("delta").load(settings.gold("dim_drug")).collect()
    }
    assert chapters == {"Infections", "Cardiovascular system"}
