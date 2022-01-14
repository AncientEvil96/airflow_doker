from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from bases.mongo import Mongo
from bases.rebbit import Rebbit
from copy import deepcopy

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
    tags=['rebbitmq', 'mongo', 'customer', 'checks'],
    schedule_interval='*/1 * * * *',
    start_date=days_ago(1),
    catchup=False
)
def rebbit_to_mongo_etl():
    for rebbitmq in rebbit_srv:
        rebbitmq['password'] = rebbit_pass
        rebbitmq['login'] = rebbit_login
        for mongodb in mongo_connect:
            rebbitmq['queue'] = mongodb['queue']
            mongodb['login'] = mongo_login
            mongodb['password'] = mongo_pass
            sourse = Rebbit(params=deepcopy(rebbitmq))
            target = Mongo(params=deepcopy(mongodb))

            @task(task_id=f"extract_{rebbitmq['host']}_{rebbitmq['queue']}")
            def extract(sourse):
                return sourse.callback_rebbit()

            @task(task_id=f"uploading_{mongodb['host']}_{mongodb['database']}")
            def load(target, list_message: list):
                target.update_mongo(list_message)

            list_message = extract(sourse)
            load(target, list_message)


tutorial_etl_dag = rebbit_to_mongo_etl()
