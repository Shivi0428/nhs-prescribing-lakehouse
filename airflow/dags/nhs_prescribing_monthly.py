"""Monthly NHS prescribing refresh.

NHSBSA publish EPD roughly two months in arrears, so each run processes the
month that is two months before the run's logical date. The tasks call the
same CLI you'd run by hand, which keeps the DAG thin.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# {{ data_interval_start }} is the 1st of the month being scheduled; step back 2 months
MONTH = "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=2)).strftime('%Y%m') }}"


@dag(
    dag_id="nhs_prescribing_monthly",
    schedule="0 6 15 * *",          # 15th of the month, after NHSBSA usually publish
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=30)},
    tags=["nhs", "lakehouse"],
)
def nhs_prescribing_monthly():
    ingest = BashOperator(task_id="ingest", bash_command=f"rxlake ingest {MONTH}")
    bronze = BashOperator(task_id="bronze", bash_command=f"rxlake bronze {MONTH}")
    silver = BashOperator(task_id="silver_with_quality_checks", bash_command=f"rxlake silver {MONTH}")
    gold = BashOperator(task_id="gold", bash_command="rxlake gold")

    ingest >> bronze >> silver >> gold


nhs_prescribing_monthly()
