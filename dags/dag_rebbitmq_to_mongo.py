from airflow.decorators import dag, task
# from airflow.models.baseoperator import chain
# from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from rebbitmq_to_mongo_project.rebbitmq_to_mongo_test import callback_rebbit, load_pymongo
from datetime import datetime, timedelta

mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
mongo_pass = Variable.get("secret_mongo_pass")
rebbit_srv = Variable.get("rebbit_srv", deserialize_json=True)
rebbit_login = Variable.get("rebbit_login")
rebbit_pass = Variable.get("secret_rebbit_pass")


@dag(
    default_args={
        'owner': 'airflow',
        'email': ['bersek123@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,

    },
    dag_id='rebbitmq_to_mongo',
    tags=['rebbitmq', 'mongo'],
    # schedule_interval='*/30 * * * * *',
    schedule_interval=timedelta(seconds=30),
    start_date=days_ago(2),
    catchup=False
)
def customer_to_mongo_etl():
    @task
    def extract(srv, rebbit_login, rebbit_pass):
        return callback_rebbit(srv, rebbit_login, rebbit_pass)

    # @task
    # def transform(set_message: set) -> list:
    #     pass

    @task
    def load(list_message: list):
        load_pymongo(
            list_message,
            host=mongo_connect['host'],
            port=mongo_connect['port'],
            schema=mongo_connect['schema'],
            database=mongo_connect['database'],
            login=mongo_connect['login'],
            password=mongo_pass
        )
        # server_mongo = '84.38.187.211'
        # port = 27017
        # schema = 'info_checks'
        # database = 'checks'
        # login = 'transfer'
        # password = 'QXm6ditoC06BaoA6iZbS'
        #
        # url = f'mongodb://{login}:{password}@{server_mongo}:{port}/{schema}'
        #
        # mongo = MongoHook(
        #     'mongo',
        #     connection={
        #         'port': port,
        #         'host': server_mongo,
        #         'login': login,
        #         'password': password,
        #         'database': database
        #     },
        #     extras={
        #         'srv': schema
        #     }
        # )
        # mongo.insert_many(list_message)

    for srv in rebbit_srv:
        list_message = extract(srv, rebbit_login, rebbit_pass)
        # list_message = extract()
        # list_message = transform(set_message)
        load(list_message)


tutorial_etl_dag = customer_to_mongo_etl()
