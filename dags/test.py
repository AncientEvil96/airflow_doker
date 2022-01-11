from airflow.decorators import dag, task
# from airflow.models.baseoperator import chain
# from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from rebbitmq_to_mongo_project.rebbitmq_to_mongo_test import RebbitMongoETL

# from datetime import datetime, timedelta

mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
mongo_pass = Variable.get("secret_mongo_pass")
rebbit_srv = Variable.get("rebbit_srv", deserialize_json=True)
rebbit_login = Variable.get("rebbit_login")
rebbit_pass = Variable.get("secret_rebbit_pass")
mongo_bases = ''


# host = connection.pop('host')  # '84.38.187.211'
# port = connection.pop('port')  # 27017
# schema = connection.pop('schema')  # 'info_checks'
# database = connection.pop('database')  # 'customers'
# login = connection.pop('login')  # 'transfer'
# password = connection.pop('password')  # 'QXm6ditoC06BaoA6iZbS'


# list_queue = Variable.get("rebbit_queue", deserialize_json=True)

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
    schedule_interval='*/1 * * * *',
    # schedule_interval=timedelta(seconds=15),
    start_date=days_ago(2),
    catchup=False
)
def customer_to_mongo_etl():
    @task
    def extract(etl):
        return etl.callback_rebbit()

    @task
    def load(etl, list_message: list):
        etl.load_pymongo(list_message)

        # load_pymongo(
        #     list_message,
        #     host=mongo_connect['host'],
        #     port=mongo_connect['port'],
        #     schema=mongo_connect['schema'],
        #     database=mongo_connect['database'],
        #     login=mongo_connect['login'],
        #     password=mongo_pass
        # )
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

    for rebbitmq in rebbit_srv:
        rebbitmq['password'] = rebbit_pass
        for mongodb in mongo_bases:
            rebbitmq['queue'] = ''
            mongodb['database'] = ''
            etl = RebbitMongoETL(rebbitmq=rebbitmq, mongodb=mongodb)
            list_message = extract(etl)
            load(etl, list_message)


tutorial_etl_dag = customer_to_mongo_etl()
