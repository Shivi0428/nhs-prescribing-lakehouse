"""Wires bronze -> silver -> gold together. Each stage is callable on its own,
which is what the Airflow DAG does."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rxlake import bronze, gold, ingest, quality, silver
from rxlake.config import Settings
from rxlake.spark import overwrite_partitions, overwrite_table

log = logging.getLogger(__name__)


def run_ingest(settings: Settings, year_month: int, force: bool = False) -> Path:
    return ingest.ingest_month(settings, year_month, force=force)


def run_bronze(spark: SparkSession, settings: Settings, year_month: int) -> int:
    csv = settings.landing_file(year_month)
    if not Path(csv).exists():
        raise FileNotFoundError(f"{csv} not found - run the ingest step first")
    rows = bronze.load_month(spark, csv, year_month, settings.bronze_epd)
    log.info("bronze %s: %s rows", year_month, f"{rows:,}")
    return rows


def run_silver(spark: SparkSession, settings: Settings, year_month: int) -> dict:
    src = spark.read.format("delta").load(settings.bronze_epd).where(F.col("period") == year_month)
    result = silver.clean(src)

    # cache: the clean and reject frames both come from the same scan
    clean_df, rejects = result.clean.cache(), result.rejects.cache()
    n_clean, n_rej = clean_df.count(), rejects.count()
    total = n_clean + n_rej
    ratio = n_rej / total if total else 0.0
    log.info("silver %s: %s clean, %s rejected (%.3f%%)", year_month, n_clean, n_rej, ratio * 100)

    if total and ratio > settings.max_reject_ratio:
        raise quality.DataQualityError(
            f"{year_month}: {ratio:.2%} of rows rejected, limit is {settings.max_reject_ratio:.2%}"
        )

    report = quality.validate(clean_df, f"silver_{year_month}", quality.silver_expectations())
    if settings.fail_on_error:
        report.raise_if_failed()

    overwrite_partitions(clean_df, settings.silver_epd, "year_month", [year_month])
    overwrite_partitions(rejects, settings.silver_rejects, "year_month", [year_month])
    clean_df.unpersist()
    rejects.unpersist()
    return {"clean": n_clean, "rejected": n_rej, "quality_ok": report.success}


def run_gold(spark: SparkSession, settings: Settings) -> dict:
    """Gold is rebuilt from all of silver. Dimensions are small, and the fact
    aggregation is cheap next to reading the data, so a full rebuild keeps
    "latest practice details" etc. correct without clever merge logic."""
    s = spark.read.format("delta").load(settings.silver_epd)

    ods = None
    if settings.ods_practices_path and Path(settings.ods_practices_path).exists():
        ods = gold.read_ods_practices(spark, settings.ods_practices_path)

    dim_date = gold.make_dim_date(s)
    dim_practice = gold.make_dim_practice(s, ods)
    dim_drug = gold.make_dim_drug(s)
    dim_icb = gold.make_dim_icb(s)
    fact = gold.make_fact(s)

    tables = {
        "dim_date": dim_date,
        "dim_practice": dim_practice,
        "dim_drug": dim_drug,
        "dim_icb": dim_icb,
        "fact_prescribing": fact,
        "agg_icb_chapter_month": gold.make_agg_icb_chapter_month(fact, dim_drug, dim_icb),
        "agg_practice_month": gold.make_agg_practice_month(fact, dim_practice),
    }

    report = quality.validate(fact, "gold_fact", quality.gold_fact_expectations())
    orphans = {
        k: quality.check_referential_integrity(fact, d, k)
        for k, d in (("practice_key", dim_practice), ("drug_key", dim_drug))
    }
    log.info("gold orphans: %s", orphans)
    if settings.fail_on_error:
        report.raise_if_failed()
        if any(orphans.values()):
            raise quality.DataQualityError(f"fact rows with no dimension match: {orphans}")

    counts = {}
    for name, df in tables.items():
        overwrite_table(df, settings.gold(name))
        counts[name] = spark.read.format("delta").load(settings.gold(name)).count()
    return counts


def run_month(spark: SparkSession, settings: Settings, year_month: int, skip_download: bool = False):
    if not skip_download:
        run_ingest(settings, year_month)
    run_bronze(spark, settings, year_month)
    run_silver(spark, settings, year_month)
    return run_gold(spark, settings)
