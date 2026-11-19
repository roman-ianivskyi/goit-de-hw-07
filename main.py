from datetime import datetime
import random
import time

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (PythonOperator, BranchPythonOperator)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "goit_mysql_db"
TARGET_TABLE = "olympic_dataset.medal_counts_ri"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
DELAY_SECONDS = 35
# DELAY_SECONDS = 20


def choose_random_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


def branch_by_medal(ti):
    selected_medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{selected_medal}"


def delay_execution():
    time.sleep(DELAY_SECONDS)


with DAG(
    dag_id="medal_counting_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olympic", "medals", "airflow3"],
) as dag:

    # create target table if it does not exist
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(16),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=choose_random_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=branch_by_medal,
    )

    calc_Bronze = SQLExecuteQueryOperator(
        task_id="calc_Bronze",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(1), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = SQLExecuteQueryOperator(
        task_id="calc_Silver",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Silver', COUNT(1), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Silver';
        """,
    )

    calc_Gold = SQLExecuteQueryOperator(
        task_id="calc_Gold",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Gold', COUNT(1), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Gold';
        """,
    )

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS, # one succeeds
    )

    # Verify that the newest record in the table is no older than 30 seconds
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            FROM {TARGET_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=6,
    )

    # task dependencies
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness