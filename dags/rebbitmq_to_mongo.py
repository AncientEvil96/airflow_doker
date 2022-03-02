from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable
import json

mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
mongo_pass = Variable.get("secret_mongo_pass")
mongo_login = Variable.get("mongo_login")
rebbit_srv = Variable.get("rebbit_srv", deserialize_json=True)
rebbit_login = Variable.get("rebbit_login")
rebbit_pass = Variable.get("secret_rebbit_pass")
main_folder = Variable.get('main_folder')

project_name = 'rebbitmq_to_mongo'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'

mount_dir = [
    # Mount(
    #     source=folder,
    #     target=working_dir,
    #     type='bind'
    # ),
    Mount(
        source=f'{main_folder}/project/{project_name}',
        target=f'{working_dir}/project',
        type='bind'
    ),
    Mount(
        source=f'{main_folder}/base',
        target=f'{working_dir}/project/base',
        type='bind'
    )
]


@dag(
    default_args={
        'owner': 'Efimov Ilya',
        'email': ['bersek123@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    dag_id=project_name,
    tags=['rebbitmq', 'mongo', 'customer', 'checks'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 1),
    catchup=False,
    max_active_runs=1
)
def rebbit_to_mongo_etl():
    for rebbitmq in rebbit_srv:
        rebbitmq['password'] = rebbit_pass
        rebbitmq['login'] = rebbit_login
        for mongodb in mongo_connect:
            rebbitmq['queue'] = mongodb['queue']
            mongodb['login'] = mongo_login
            mongodb['password'] = mongo_pass

            rebbitmq_s = str(list(rebbitmq.items())).replace(', ', ',')
            mongodb_s = str(list(mongodb.items())).replace(', ', ',')

            DockerOperator(
                task_id=f"RM_{rebbitmq['host']}_{rebbitmq['queue']}_M_{mongodb['host']}_{mongodb['database']}".replace(
                    '.tkvprok.ru', '').replace('MDB_WhoIs_queue_', ''),
                image=image,
                container_name='rebbit_to_mongo_etl_{{ task_instance.job_id }}',
                api_version='1.41',
                auto_remove=True,
                mounts=mount_dir,
                working_dir=working_dir,
                command=f'python project/etl_rebbit.py {rebbitmq_s} {mongodb_s}',
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge"
            )


tutorial_etl_dag = rebbit_to_mongo_etl()