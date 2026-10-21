"""Bronze: the raw CSV, untouched apart from a few audit columns.

Everything is read as a string. Typing happens in silver, so if NHSBSA ever
ships a bad value we still keep the row here and can see what came in.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rxlake.spark import overwrite_partitions


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(path)
    )


def to_bronze(raw: DataFrame, year_month: int) -> DataFrame:
    # Header case has drifted over the years, so normalise it to upper
    # input_file_name() has to sit in the first projection over the scan
    df = raw.select(
        *[F.col(f"`{c}`").alias(c.strip().upper()) for c in raw.columns],
        F.input_file_name().alias("_source_file"),
    )
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        # partition column. Can't call it year_month - the CSV already has
        # YEAR_MONTH and Spark column names are case-insensitive.
        .withColumn("period", F.lit(int(year_month)))
    )


def load_month(spark: SparkSession, csv_path: str, year_month: int, bronze_path: str) -> int:
    df = to_bronze(read_raw_csv(spark, csv_path), year_month)
    # older months have STP_* columns, newer ones ICB_* - mergeSchema keeps both
    overwrite_partitions(df, bronze_path, "period", [year_month], merge_schema=True)
    return spark.read.format("delta").load(bronze_path).where(F.col("period") == year_month).count()
