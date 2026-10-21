"""Data quality checks built on Great Expectations.

Spark DataFrames are validated directly with the GX 1.x Spark datasource, so
nothing is collected to the driver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import DataFrame

from rxlake.silver import BNF_CODE_RE, PRACTICE_CODE_RE

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class QualityReport:
    suite: str
    success: bool
    failures: list[str]

    def raise_if_failed(self) -> None:
        if not self.success:
            raise DataQualityError(f"{self.suite} failed: " + "; ".join(self.failures))


def silver_expectations() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="practice_code"),
        gxe.ExpectColumnValuesToNotBeNull(column="bnf_code"),
        gxe.ExpectColumnValuesToNotBeNull(column="year_month"),
        gxe.ExpectColumnValuesToMatchRegex(column="practice_code", regex=PRACTICE_CODE_RE),
        gxe.ExpectColumnValuesToMatchRegex(column="bnf_code", regex=BNF_CODE_RE),
        gxe.ExpectColumnValuesToBeBetween(column="items", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=0, mostly=0.999),
        gxe.ExpectColumnValuesToBeBetween(column="year_month", min_value=201001, max_value=209912),
    ]


def gold_fact_expectations() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="date_key"),
        gxe.ExpectColumnValuesToNotBeNull(column="practice_key"),
        gxe.ExpectColumnValuesToNotBeNull(column="drug_key"),
        gxe.ExpectColumnValuesToBeBetween(column="items", min_value=0),
        # a negative total cost is legitimate for a single credit-note row, but a
        # negative cost across a practice-drug-month group would be odd
        gxe.ExpectColumnValuesToBeBetween(column="actual_cost", min_value=-1_000, mostly=0.9999),
    ]


def validate(df: DataFrame, suite: str, expectations: list) -> QualityReport:
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name="spark")
    asset = source.add_dataframe_asset(name=suite)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{suite}_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    failures = []
    for exp in expectations:
        result = batch.validate(exp)
        if not result.success:
            cfg = result.expectation_config
            col = cfg.kwargs.get("column", "table")
            failures.append(f"{cfg.type}({col})")
    report = QualityReport(suite=suite, success=not failures, failures=failures)
    log.info("quality %s: %s", suite, "PASS" if report.success else f"FAIL {failures}")
    return report


def check_referential_integrity(fact: DataFrame, dim: DataFrame, key: str) -> int:
    """Fact rows whose key is missing from the dimension (should be 0)."""
    return fact.join(dim.select(key), key, "left_anti").count()
