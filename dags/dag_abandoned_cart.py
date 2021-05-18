from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jozimar',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def abandonet_carts():
    df = pd.read_json('/opt/airflow/dags/input/page-views.json', orient="records", lines=True)

    abandoned_carts = df.groupby(["customer"]).filter(lambda x: (x.page != "checkout").all())
    abandoned_carts.loc[abandoned_carts.groupby("customer")['timestamp'].idxmax()].to_json("/opt/airflow/dags/output/abandoned-carts.json",orient="records")


with DAG(
    'dag_abandoned_cart_v1',start_date=datetime(2021,5,16), default_args=default_args, schedule_interval=None
) as dag:
    PythonOperator(
        task_id='abandoned_carts',
        python_callable=abandonet_carts
    )

if __name__ == "__main__":
    abandonet_carts()