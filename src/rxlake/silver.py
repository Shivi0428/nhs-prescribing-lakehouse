"""Silver: typed, cleaned, one consistent schema across every month.

Things this deals with:
  * STP_CODE/STP_NAME were renamed ICB_CODE/ICB_NAME when ICBs replaced STPs
    in July 2022. Silver always calls them icb_*.
  * PCO_* is what NHSBSA still call the sub-ICB location. Renamed sub_icb_*.
  * numbers arrive as text, sometimes with stray spaces.
  * the odd exact duplicate row.
Rows that fail basic checks go to a rejects table with a reason, instead of
being silently dropped.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PRACTICE_CODE_RE = r"^[A-Z0-9]{6}$"
BNF_CODE_RE = r"^[0-9A-Z]{15}$"

SILVER_COLUMNS = [
    "year_month", "month_start",
    "region_code", "region_name", "icb_code", "icb_name", "sub_icb_code", "sub_icb_name",
    "practice_code", "practice_name", "address_1", "address_2", "address_3", "address_4",
    "postcode",
    "chemical_substance_code", "chemical_substance_name",
    "bnf_code", "bnf_description", "bnf_chapter_plus_code",
    "quantity", "items", "total_quantity", "adq_usage", "nic", "actual_cost",
    "unidentified", "_source_file", "_ingested_at",
]

MONEY = "decimal(18,5)"


@dataclass
class SilverResult:
    clean: DataFrame
    rejects: DataFrame


def _first_present(df: DataFrame, *names: str) -> Column:
    """coalesce() over whichever of the candidate columns this month actually has."""
    cols = [F.col(n) for n in names if n in df.columns]
    if not cols:
        return F.lit(None).cast("string")
    return cols[0] if len(cols) == 1 else F.coalesce(*cols)


def _text(df: DataFrame, *names: str) -> Column:
    c = F.trim(_first_present(df, *names))
    return F.when(c == "", None).otherwise(c)


def _code(df: DataFrame, *names: str) -> Column:
    return F.upper(_text(df, *names))


def _num(df: DataFrame, name: str, dtype: str) -> Column:
    return F.regexp_replace(_text(df, name), ",", "").cast(dtype)


def standardise(bronze: DataFrame) -> DataFrame:
    df = bronze
    year_month = F.coalesce(_text(df, "YEAR_MONTH").cast("int"), F.col("period"))

    postcode = F.regexp_replace(_code(df, "POSTCODE"), r"\s+", " ")

    out = df.select(
        year_month.alias("year_month"),
        F.to_date(year_month.cast("string"), "yyyyMM").alias("month_start"),
        _code(df, "REGIONAL_OFFICE_CODE").alias("region_code"),
        _text(df, "REGIONAL_OFFICE_NAME").alias("region_name"),
        _code(df, "ICB_CODE", "STP_CODE").alias("icb_code"),
        _text(df, "ICB_NAME", "STP_NAME").alias("icb_name"),
        _code(df, "PCO_CODE").alias("sub_icb_code"),
        _text(df, "PCO_NAME").alias("sub_icb_name"),
        _code(df, "PRACTICE_CODE").alias("practice_code"),
        _text(df, "PRACTICE_NAME").alias("practice_name"),
        _text(df, "ADDRESS_1").alias("address_1"),
        _text(df, "ADDRESS_2").alias("address_2"),
        _text(df, "ADDRESS_3").alias("address_3"),
        _text(df, "ADDRESS_4").alias("address_4"),
        postcode.alias("postcode"),
        _code(df, "BNF_CHEMICAL_SUBSTANCE").alias("chemical_substance_code"),
        _text(df, "CHEMICAL_SUBSTANCE_BNF_DESCR").alias("chemical_substance_name"),
        _code(df, "BNF_CODE").alias("bnf_code"),
        _text(df, "BNF_DESCRIPTION").alias("bnf_description"),
        _text(df, "BNF_CHAPTER_PLUS_CODE").alias("bnf_chapter_plus_code"),
        _num(df, "QUANTITY", "double").alias("quantity"),
        _num(df, "ITEMS", "bigint").alias("items"),
        _num(df, "TOTAL_QUANTITY", "double").alias("total_quantity"),
        _num(df, "ADQUSAGE", "double").alias("adq_usage"),
        _num(df, "NIC", MONEY).alias("nic"),
        _num(df, "ACTUAL_COST", MONEY).alias("actual_cost"),
        F.upper(_text(df, "UNIDENTIFIED")).isin("Y", "YES", "TRUE", "1").alias("unidentified"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.col("period"),
    )
    return out


def _reject_reason(df: DataFrame) -> Column:
    checks = [
        (F.col("year_month").isNull(), "missing_year_month"),
        (F.col("year_month") != F.col("period"), "year_month_mismatch"),
        (F.col("practice_code").isNull(), "missing_practice_code"),
        (~F.col("practice_code").rlike(PRACTICE_CODE_RE), "bad_practice_code"),
        (F.col("bnf_code").isNull(), "missing_bnf_code"),
        (~F.col("bnf_code").rlike(BNF_CODE_RE), "bad_bnf_code"),
        (F.col("items").isNull() | (F.col("items") < 0), "bad_items"),
        (F.col("actual_cost").isNull(), "missing_actual_cost"),
    ]
    # concat_ws skips nulls, so a clean row ends up as ""
    return F.concat_ws(";", *[F.when(cond, F.lit(name)) for cond, name in checks])


def clean(bronze: DataFrame) -> SilverResult:
    std = standardise(bronze)
    std = std.withColumn("_reject_reason", _reject_reason(std))

    good = (
        std.where(F.col("_reject_reason") == "")
        # EPD isn't unique on practice + bnf_code (one row per pack quantity),
        # so only exact duplicates are removed here
        .dropDuplicates([c for c in SILVER_COLUMNS if not c.startswith("_")])
        .select(*SILVER_COLUMNS)
    )
    bad = std.where(F.col("_reject_reason") != "").select(
        *SILVER_COLUMNS, "_reject_reason"
    ).withColumnRenamed("_reject_reason", "reject_reason")
    return SilverResult(clean=good, rejects=bad)
