import os
import time

import pendulum
from airflow.operators.bash import BashOperator
from airflow.sdk import dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",
    catchup=False,
    start_date=pendulum.datetime(year=2026, month=7, day=11, tz="America/Bogota"),
)
def orchestrate():
    @task
    def ingest_cdc():
        ws = WorkspaceClient()

        job_trigger = ws.jobs.run_now(
            job_id=int(os.environ["DATABRICKS_INGEST_JOB_ID"])
        )

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            print(
                f"Job run status: {job_run.state.life_cycle_state}, "
                f"result state: {job_run.state.result_state}"
            )

            if job_run.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break

                print("State message:", job_run.state.state_message)
                print("Databricks run URL:", job_run.run_page_url)

                raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)

        return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t",
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_t",
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_b",
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_b",
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold.ephemeral",
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_project",
        bash_command=(
            "dbt snapshot --select "
            "dim_orders dim_customers dim_products dim_stores dim_employees"
        ),
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold.fact",
    )

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical
        >> silver_technical_tests
        >> silver_business
        >> silver_business_tests
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_facts
    )


orchestrate_dag = orchestrate()
