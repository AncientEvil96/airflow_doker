from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from rebbitmq_to_mongo_project.rebbitmq_to_mongo_test import callback_rebbit, load_mongo


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
def customer_to_mongo_etl():
    @task
    def extract():
        return callback_rebbit()

    @task
    def transform(set_message: set) -> list:
        return list(set_message)

    @task
    def load(list_message: list):
        load_mongo(list_message)

    set_message = extract()
    list_message = transform(set_message)
    load(list_message)


tutorial_etl_dag = customer_to_mongo_etl()
