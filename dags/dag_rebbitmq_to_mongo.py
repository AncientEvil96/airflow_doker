from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from rebbitmq_to_mongo_project.rebbitmq_to_mongo_test import callback_rebbit


@dag(
    default_args={
        'owner': 'airflow',
    },
    dag_id='rebbitmq_to_mongo',
    tags=['rebbitmq', 'mongo'],
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False
)
def customer_etl():
    @task
    def extract():
        callback_rebbit()

    @task
    def transform(order_data_dict: dict) -> dict:
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task
    def load(total_order_value: float):
        print("Total order value is: %.2f" % total_order_value)

    extract()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])


tutorial_etl_dag = tutorial_taskflow_api_etl()
