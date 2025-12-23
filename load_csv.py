from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

DATA_DIR = "/opt/airflow/data_files"

FILES = {
    "customer": "customer.csv",
    "product": "product.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
}

def load_csv_to_postgres(table_name: str, file_name: str):
    file_path = os.path.join(DATA_DIR, file_name)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(file_path, "r") as f:
        cursor.copy_expert(
            sql=f"""
                COPY {table_name}
                FROM STDIN
                WITH CSV HEADER
            """,
            file=f
        )

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="load_local_files_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "csv", "postgres"],
) as dag:

    load_customer = PythonOperator(
        task_id="load_customer",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "table_name": "customer",
            "file_name": FILES["customer"],
        },
    )

    load_product = PythonOperator(
        task_id="load_product",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "table_name": "product",
            "file_name": FILES["product"],
        },
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "table_name": "orders",
            "file_name": FILES["orders"],
        },
    )

    load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "table_name": "order_items",
            "file_name": FILES["order_items"],
        },
    )

    # Все задачи выполняются параллельно
    [
        load_customer,
        load_product,
        load_orders,
        load_order_items,
    ]