from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.mongo.hooks.mongo import MongoHook
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
        server_mongo = '84.38.187.211'
        port = 27017
        schema = 'info_checks'
        database = 'checks'
        login = 'transfer'
        password = 'QXm6ditoC06BaoA6iZbS'

        mongo = MongoHook(
            'mongo',
            connection={
                'port': port,
                'host': server_mongo,
                'login': login,
                'password': password,
                'database': database
            },
            extras={
                'srv': schema
            }
        )
        mongo.insert_many(list_message)

    set_message = extract()
    list_message = transform(set_message)
    load(list_message)


tutorial_etl_dag = customer_to_mongo_etl()
