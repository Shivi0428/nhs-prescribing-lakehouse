"""Gold: a star schema for BI, plus two pre-aggregated marts.

    dim_date      one row per month
    dim_practice  one row per practice (latest known details)
    dim_drug      one row per BNF presentation code
    dim_icb       one row per ICB
    fact_prescribing   practice x drug x month, measures: items, quantity, nic, actual_cost

    agg_icb_chapter_month   spend and items per ICB, BNF chapter and month
    agg_practice_month      spend and items per practice and month
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

BNF_CHAPTERS = {
    "01": "Gastro-intestinal system",
    "02": "Cardiovascular system",
    "03": "Respiratory system",
    "04": "Central nervous system",
    "05": "Infections",
    "06": "Endocrine system",
    "07": "Obstetrics, gynaecology and urinary-tract disorders",
    "08": "Malignant disease and immunosuppression",
    "09": "Nutrition and blood",
    "10": "Musculoskeletal and joint diseases",
    "11": "Eye",
    "12": "Ear, nose and oropharynx",
    "13": "Skin",
    "14": "Immunological products and vaccines",
    "15": "Anaesthesia",
    "18": "Preparations used in diagnosis",
    "19": "Other drugs and preparations",
    "20": "Dressings",
    "21": "Appliances",
    "22": "Incontinence appliances",
    "23": "Stoma appliances",
}


def make_dim_date(silver: DataFrame) -> DataFrame:
    return (
        silver.select("year_month", "month_start").distinct()
        .select(
            F.col("year_month").alias("date_key"),
            F.col("month_start"),
            F.year("month_start").alias("calendar_year"),
            F.month("month_start").alias("calendar_month"),
            F.date_format("month_start", "MMMM").alias("month_name"),
            F.quarter("month_start").alias("calendar_quarter"),
            # NHS financial year runs April to March
            F.when(F.month("month_start") >= 4, F.year("month_start"))
            .otherwise(F.year("month_start") - 1).alias("fy_start_year"),
        )
        .withColumn(
            "financial_year",
            F.concat(F.col("fy_start_year"), F.lit("/"),
                     F.substring((F.col("fy_start_year") + 1).cast("string"), 3, 2)),
        )
        .drop("fy_start_year")
    )


def make_dim_practice(silver: DataFrame, ods: DataFrame | None = None) -> DataFrame:
    """Latest name/address per practice from EPD, optionally enriched from ODS."""
    w = Window.partitionBy("practice_code").orderBy(F.col("year_month").desc())
    latest = (
        silver.select(
            "practice_code", "practice_name", "address_1", "address_2", "address_3",
            "address_4", "postcode", "icb_code", "sub_icb_code", "year_month",
        )
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .withColumnRenamed("year_month", "last_seen_year_month")
    )
    first_seen = silver.groupBy("practice_code").agg(F.min("year_month").alias("first_seen_year_month"))
    dim = latest.join(first_seen, "practice_code", "left")

    if ods is not None:
        dim = dim.join(
            ods.select("practice_code", "ods_status", "ods_setting", "open_date", "close_date"),
            "practice_code", "left",
        )
    return dim.withColumn("practice_key", F.xxhash64("practice_code"))


def make_dim_drug(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("bnf_code").orderBy(F.col("year_month").desc())
    return (
        silver.select(
            "bnf_code", "bnf_description", "chemical_substance_code",
            "chemical_substance_name", "year_month",
        )
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "year_month")
        .withColumn("bnf_chapter_code", F.substring("bnf_code", 1, 2))
        .withColumn("bnf_section_code", F.substring("bnf_code", 1, 4))
        .withColumn(
            "bnf_chapter_name",
            F.coalesce(
                F.create_map(*[x for kv in BNF_CHAPTERS.items() for x in map(F.lit, kv)])[
                    F.col("bnf_chapter_code")
                ],
                F.lit("Unknown"),
            ),
        )
        .withColumn("drug_key", F.xxhash64("bnf_code"))
    )


def make_dim_icb(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("icb_code").orderBy(F.col("year_month").desc())
    return (
        silver.where(F.col("icb_code").isNotNull())
        .select("icb_code", "icb_name", "region_code", "region_name", "year_month")
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "year_month")
        .withColumn("icb_key", F.xxhash64("icb_code"))
    )


def make_fact(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("year_month", "practice_code", "bnf_code")
        .agg(
            F.sum("items").alias("items"),
            F.sum("quantity").alias("quantity"),
            F.sum("total_quantity").alias("total_quantity"),
            F.sum("nic").alias("nic"),
            F.sum("actual_cost").alias("actual_cost"),
            F.max("icb_code").alias("icb_code"),
        )
        .select(
            F.col("year_month").alias("date_key"),
            F.xxhash64("practice_code").alias("practice_key"),
            F.xxhash64("bnf_code").alias("drug_key"),
            F.xxhash64("icb_code").alias("icb_key"),
            "items", "quantity", "total_quantity", "nic", "actual_cost",
        )
    )


def make_agg_icb_chapter_month(fact: DataFrame, dim_drug: DataFrame, dim_icb: DataFrame) -> DataFrame:
    return (
        fact.join(dim_drug.select("drug_key", "bnf_chapter_code", "bnf_chapter_name"), "drug_key")
        .join(dim_icb.select("icb_key", "icb_code", "icb_name"), "icb_key", "left")
        .groupBy("date_key", "icb_code", "icb_name", "bnf_chapter_code", "bnf_chapter_name")
        .agg(
            F.sum("items").alias("items"),
            F.sum("actual_cost").alias("actual_cost"),
            F.sum("nic").alias("nic"),
        )
    )


def make_agg_practice_month(fact: DataFrame, dim_practice: DataFrame) -> DataFrame:
    return (
        fact.groupBy("date_key", "practice_key")
        .agg(F.sum("items").alias("items"), F.sum("actual_cost").alias("actual_cost"))
        .join(
            dim_practice.select("practice_key", "practice_code", "practice_name", "postcode", "icb_code"),
            "practice_key", "left",
        )
    )


def read_ods_practices(spark: SparkSession, path: str) -> DataFrame:
    """epraccur: headerless CSV. Columns used: 0 code, 10 open, 11 close, 12 status, 25 setting."""
    raw = spark.read.option("header", False).csv(path)
    return raw.select(
        F.upper(F.trim(F.col("_c0"))).alias("practice_code"),
        F.col("_c12").alias("ods_status"),
        F.to_date(F.col("_c10"), "yyyyMMdd").alias("open_date"),
        F.to_date(F.col("_c11"), "yyyyMMdd").alias("close_date"),
        F.col("_c25").alias("ods_setting"),
    )
