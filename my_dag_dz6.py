from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import pandas as pd
import os

DATA_DIR = "/opt/airflow/data_files"
OUTPUT_DIR = "/opt/airflow/output"

#Топ 3 (мин/макс)
def top_3_min_max_customers():
    customers = pd.read_csv(os.path.join(DATA_DIR, "customer.csv"))
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))

    revenue = (
        orders
        .groupby("customer_id", as_index=False)["total_amount"]
        .sum()
        .rename(columns={"total_amount": "total_revenue"})
    )

    df = (
        customers
        .merge(revenue, on="customer_id", how="left")
        .fillna({"total_revenue": 0})
    )

    top_min = df.sort_values("total_revenue").head(3)
    top_max = df.sort_values("total_revenue", ascending=False).head(3)

    result = pd.concat([top_min, top_max])[
        ["first_name", "last_name", "total_revenue"]
    ]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_csv(
        os.path.join(OUTPUT_DIR, "top_3_min_max_customers.csv"),
        index=False
    )


def top_5_customers_by_wealth_segment():
    customers = pd.read_csv(os.path.join(DATA_DIR, "customer.csv"))
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))

    revenue = (
        orders
        .groupby("customer_id", as_index=False)["total_amount"]
        .sum()
        .rename(columns={"total_amount": "total_revenue"})
    )

    df = (
        customers
        .merge(revenue, on="customer_id", how="left")
        .fillna({"total_revenue": 0})
    )

    result = (
        df
        .sort_values(
            ["wealth_segment", "total_revenue"],
            ascending=[True, False]
        )
        .groupby("wealth_segment")
        .head(5)
    )[
        ["first_name", "last_name", "wealth_segment", "total_revenue"]
    ]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_csv(
        os.path.join(
            OUTPUT_DIR,
            "top_5_customers_by_wealth_segment.csv"
        ),
        index=False
    )


#Проверки (Без email)
def check_file_not_empty(file_name: str):
    path = os.path.join(OUTPUT_DIR, file_name)

    if not os.path.exists(path):
        raise ValueError(f"Файл не найден: {file_name}")

    if pd.read_csv(path).empty:
        raise ValueError(f"Файл пуст: {file_name}")

#Финальные сообщения
def print_success():
    print("DAG выполнен успешно")


def print_error():
    print("Ошибка")


with DAG(
    dag_id="my_dag_dz6",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["python"],
) as dag:

    calc_top_min_max = PythonOperator(
        task_id="top_3_min_max_customers",
        python_callable=top_3_min_max_customers,
    )

    calc_top_by_segment = PythonOperator(
        task_id="top_5_customers_by_wealth_segment",
        python_callable=top_5_customers_by_wealth_segment,
    )

    check_min_max = PythonOperator(
        task_id="check_top_3_min_max_not_empty",
        python_callable=check_file_not_empty,
        op_kwargs={"file_name": "top_3_min_max_customers.csv"},
    )

    check_by_segment = PythonOperator(
        task_id="check_top_5_by_segment_not_empty",
        python_callable=check_file_not_empty,
        op_kwargs={"file_name": "top_5_customers_by_wealth_segment.csv"},
    )

    email_on_failure = EmailOperator(
        task_id="email_on_failure",
        to="data-team@example.com",
        subject="Airflow alert: аналитические запросы вернули 0 строк",
        html_content="""
        <p>Один или несколько аналитических запросов
        вернули пустой результат.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    success_task = PythonOperator(
        task_id="dag_success",
        python_callable=dag_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_task = PythonOperator(
        task_id="dag_failure",
        python_callable=dag_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    #Зависимости:
    #Сначала выполняются расчеты
    #Затем проверки файлов

    # Зависимости для аналитических задач
    calc_top_min_max >> check_min_max
    calc_top_by_segment >> check_by_segment

    # Зависимости для проверок
    check_min_max >> success_task
    check_by_segment >> success_task

    check_min_max >> error_task
    check_by_segment >> error_task