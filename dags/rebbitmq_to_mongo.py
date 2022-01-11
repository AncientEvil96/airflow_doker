from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from rebbitmq_to_mongo_project.rebbitmq_to_mongo_test import RebbitMongoETL

mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
mongo_pass = Variable.get("secret_mongo_pass")
mongo_login = Variable.get("mongo_login")
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
    schedule_interval='*/1 * * * *',
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

    for rebbitmq in rebbit_srv:
        rebbitmq['password'] = rebbit_pass
        rebbitmq['login'] = rebbit_login
        for mongodb in mongo_connect:
            rebbitmq['queue'] = mongodb['queue']
            mongodb['login'] = mongo_login
            mongodb['password'] = mongo_pass
            etl = RebbitMongoETL(rebbitmq=rebbitmq, mongodb=mongodb)
            list_message = extract(etl)
            load(etl, list_message)


tutorial_etl_dag = customer_to_mongo_etl()
