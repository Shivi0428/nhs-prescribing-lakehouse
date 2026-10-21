import os
import shutil
import sys

import pytest


def _spark_usable() -> bool:
    if shutil.which("java") is None:
        return False
    # Spark on Windows needs Hadoop's winutils; CI and Docker are Linux
    return sys.platform != "win32" or bool(os.getenv("HADOOP_HOME"))


def pytest_collection_modifyitems(config, items):
    if _spark_usable():
        return
    skip = pytest.mark.skip(reason="Java/Hadoop not set up here - Spark tests run in CI / Docker")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    from rxlake.spark import get_spark

    s = get_spark("rxlake-tests", shuffle_partitions=2)
    yield s
    s.stop()


SAMPLE_COLUMNS = [
    "YEAR_MONTH", "REGIONAL_OFFICE_NAME", "REGIONAL_OFFICE_CODE", "ICB_NAME", "ICB_CODE",
    "PCO_NAME", "PCO_CODE", "PRACTICE_NAME", "PRACTICE_CODE", "ADDRESS_1", "ADDRESS_2",
    "ADDRESS_3", "ADDRESS_4", "POSTCODE", "BNF_CHEMICAL_SUBSTANCE", "CHEMICAL_SUBSTANCE_BNF_DESCR",
    "BNF_CODE", "BNF_DESCRIPTION", "BNF_CHAPTER_PLUS_CODE", "QUANTITY", "ITEMS",
    "TOTAL_QUANTITY", "ADQUSAGE", "NIC", "ACTUAL_COST", "UNIDENTIFIED",
]


def sample_rows():
    """Tiny made-up slice shaped like EPD. Includes one bad practice code,
    one bad BNF code, and an exact duplicate."""
    base = ["202401", "LONDON", "Y56", "NHS NORTH EAST LONDON ICB", "QMF", "NHS NEL SUB", "08W",
            "THE TEST SURGERY", "A81001", "1 HIGH ST", "", "", "", "ts1  1ab", "0501013B0",
            "Amoxicillin", "0501013B0AAAAAA", "Amoxicillin 500mg capsules", "05: Infections",
            "28", "2", "56", "0", "1.50", "1.20", "N"]
    rows = [
        base,
        base,  # exact duplicate
        [*base[:8], "A81002", *base[9:16], "0212000B0AAAAAA", "Atorvastatin 20mg tablets",
         "02: Cardiovascular system", "84", "3", "252", "0", "2.10", "1.80", "N"],
        [*base[:8], "BAD", *base[9:]],                       # bad practice code
        [*base[:16], "SHORT", *base[17:]],                   # bad bnf code
    ]
    return rows
