from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount
from airflow.models import Variable

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
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2022, 3, 1),
    catchup=False,
    max_active_runs=1
)
def rebbit_to_mongo_etl():

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'mkdir -p -m 777 {airflow_work_dir}'
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'rm -r {airflow_work_dir}',
        trigger_rule='none_skipped'
    )

    for rebbitmq in rebbit_srv:
        rebbitmq['password'] = rebbit_pass
        rebbitmq['login'] = rebbit_login
        for mongodb in mongo_connect:
            rebbitmq['queue'] = mongodb['queue']
            mongodb['login'] = mongo_login
            mongodb['password'] = mongo_pass

            rebbitmq_s = str(list(rebbitmq.items())).replace(', ', ',')
            mongodb_s = str(list(mongodb.items())).replace(', ', ',')

            rb_mdb = DockerOperator(
                task_id=f"RM_{rebbitmq['host']}_{rebbitmq['queue']}_M_{mongodb['host']}_{mongodb['database']}".replace(
                    '.tkvprok.ru', '').replace('MDB_WhoIs_queue_', ''),
                image=image,
                container_name='rebbit_to_mongo_etl_{{ task_instance.job_id }}',
                api_version='1.41',
                auto_remove=True,
                environment={
                    'REBBIT': rebbitmq_s,
                    'MONGO': mongodb_s
                },
                mounts=mount_dir,
                working_dir=working_dir,
                command=f'bash -c "python project/etl_rebbit.py $REBBIT $MONGO"',
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge"
            )

            create_folder >> rb_mdb >> delete_folder


tutorial_etl_dag = rebbit_to_mongo_etl()
