"""Morning Executive Briefing — workshop demo DAG.

Pattern: fan-out -> join -> load.

    fetch_sales     ─┐
    fetch_marketing ─┼─► aggregate_data ─► generate_and_send_report
    fetch_support   ─┘

Three extract tasks run in parallel (each a separate KubernetesExecutor pod),
`aggregate_data` only starts once all three succeed, then the report step runs.

Demo simplifications (mention these when presenting):
  1. Data is GENERATED ON THE FLY inside the fetch_* tasks — not read from S3.
  2. This file is delivered with `kubectl cp` into the shared dags volume — not git-synced.
  3. The final step LOGS the report — it does not send a real email.

`aggregate_data` is a KubernetesPodOperator: it runs in its own plain `python`
container to make the point that *any* container image can be an Airflow task.
"""

from __future__ import annotations

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# --- Extract tasks (fan-out) ---------------------------------------------
# Each generates fake business data deterministically from the reference date so
# every run produces stable, explainable numbers. Returned dicts land in XCom.

def _ref_date(context) -> datetime.datetime:
    """A stable reference datetime.

    Manual runs in Airflow 3 have ``logical_date=None``, so fall back to the
    run's data interval / run_after, and finally to "now".
    """
    dt = context.get("logical_date") or context.get("data_interval_start")
    if dt is None:
        dag_run = context.get("dag_run")
        dt = getattr(dag_run, "run_after", None) or getattr(dag_run, "queued_at", None)
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    return dt


def fetch_sales(**context) -> dict:
    day = _ref_date(context).day
    revenue = 50_000 + day * 1_250
    orders = 800 + day * 17
    print(f"[sales] revenue={revenue} orders={orders}")
    return {"revenue": revenue, "orders": orders}


def fetch_marketing(**context) -> dict:
    day = _ref_date(context).day
    leads = 1_000 + day * 42
    spend = 8_000 + day * 90
    print(f"[marketing] leads={leads} spend={spend}")
    return {"leads": leads, "spend": spend}


def fetch_support(**context) -> dict:
    day = _ref_date(context).day
    tickets = 120 + (day * 7) % 60
    csat = round(4.2 + (day % 7) * 0.1, 2)
    print(f"[support] tickets={tickets} csat={csat}")
    return {"tickets": tickets, "csat": csat}


# --- Load task (report) ---------------------------------------------------

def generate_and_send_report(**context) -> str:
    ti = context["ti"]
    sales = ti.xcom_pull(task_ids="fetch_sales")
    marketing = ti.xcom_pull(task_ids="fetch_marketing")
    support = ti.xcom_pull(task_ids="fetch_support")
    health = ti.xcom_pull(task_ids="aggregate_data")

    report = f"""
================ MORNING EXECUTIVE BRIEFING ================
Date: {_ref_date(context).date()}

  SALES      revenue=${sales['revenue']:,}   orders={sales['orders']:,}
  MARKETING  leads={marketing['leads']:,}   spend=${marketing['spend']:,}
  SUPPORT    tickets={support['tickets']}   CSAT={support['csat']}/5

  >> BUSINESS HEALTH SCORE: {health['health_score']} / 100
     {health['headline']}
===========================================================
"""
    print(report)
    # Simplification #3: we LOG the report instead of emailing it.
    return report


# The aggregation runs in a separate plain `python` container. It reads the
# three upstream results from env vars (templated XCom pulls) and pushes a
# result back via /airflow/xcom/return.json (do_xcom_push=True).
AGGREGATE_SCRIPT = """
import json, os

revenue = float(os.environ["REVENUE"])
leads = float(os.environ["LEADS"])
tickets = float(os.environ["TICKETS"])

# Toy "health score": revenue and leads help, open tickets hurt.
score = revenue / 1000 + leads / 50 - tickets / 4
health_score = max(0, min(100, round(score / 12)))

if health_score >= 75:
    headline = "Strong morning. Keep the momentum."
elif health_score >= 50:
    headline = "Solid, but watch the support backlog."
else:
    headline = "Attention needed: support load is dragging us down."

result = {"health_score": health_score, "headline": headline}
print("aggregate ->", result)

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.json", "w") as f:
    json.dump(result, f)
"""


with DAG(
    dag_id="morning_executive_briefing",
    description="Fan-out -> join -> load demo (KubernetesExecutor + KubernetesPodOperator)",
    schedule="0 7 * * *",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "workshop", "ist"],
) as dag:

    fetch_sales_task = PythonOperator(
        task_id="fetch_sales",
        python_callable=fetch_sales,
    )
    fetch_marketing_task = PythonOperator(
        task_id="fetch_marketing",
        python_callable=fetch_marketing,
    )
    fetch_support_task = PythonOperator(
        task_id="fetch_support",
        python_callable=fetch_support,
    )

    aggregate_data = KubernetesPodOperator(
        task_id="aggregate_data",
        name="aggregate-data",
        namespace="airflow",
        image="python:3.12-slim",
        cmds=["python", "-c"],
        arguments=[AGGREGATE_SCRIPT],
        env_vars={
            "REVENUE": "{{ ti.xcom_pull(task_ids='fetch_sales')['revenue'] }}",
            "LEADS": "{{ ti.xcom_pull(task_ids='fetch_marketing')['leads'] }}",
            "TICKETS": "{{ ti.xcom_pull(task_ids='fetch_support')['tickets'] }}",
        },
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="delete_pod",
    )

    report_task = PythonOperator(
        task_id="generate_and_send_report",
        python_callable=generate_and_send_report,
    )

    [fetch_sales_task, fetch_marketing_task, fetch_support_task] >> aggregate_data >> report_task
