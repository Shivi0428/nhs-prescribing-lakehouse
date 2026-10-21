from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str = "rxlake", shuffle_partitions: int = 8) -> SparkSession:
    # On Databricks the session (and Delta) already exist, so don't fight it
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def overwrite_partitions(
    df: DataFrame,
    path: str,
    partition_col: str,
    values: list[int],
    merge_schema: bool = False,
) -> None:
    """Replace only the given partitions of a Delta table (or create it).

    This is what makes every stage safe to re-run for a month: running
    January twice leaves you with one copy of January.
    """
    spark = df.sparkSession
    writer = df.write.format("delta").partitionBy(partition_col)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if not table_exists(spark, path):
        writer.mode("overwrite").save(path)
        return

    in_list = ", ".join(str(int(v)) for v in values)
    writer.mode("overwrite").option("replaceWhere", f"{partition_col} IN ({in_list})").save(path)


def overwrite_table(df: DataFrame, path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
