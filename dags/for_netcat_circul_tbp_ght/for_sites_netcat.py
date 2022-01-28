from airflow.decorators import dag, task
from copy import deepcopy
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from docker.types import Mount
from airflow.models import Variable

mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
mongo_pass = Variable.get("secret_mongo_pass")
mongo_login = Variable.get("mongo_login")
rebbit_srv = Variable.get("rebbit_srv", deserialize_json=True)
rebbit_login = Variable.get("rebbit_login")
rebbit_pass = Variable.get("secret_rebbit_pass")


today = datetime.today().strftime("%Y_%m_%d")
folder = f'/home/deus/PycharmProjects/airflow_doker/tmp/{today}'


@dag(
    default_args={
        'owner': 'Efimov Ilya',
        'email': ['bersek123@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    dag_id='for_sites_netcat',
    tags=['netcat', 'vprok', 'compass', 'ght'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 28),
    catchup=False
)
def for_sites_netcat():

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'mkdir -m 777 /opt/airflow/tmp/{today}'
    )

    extract_tbp = DockerOperator(
        task_id='extract_tbp',
        image='airflow_task_python_3.9',
        container_name='extract_tbp_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        working_dir='/tmp/tmp',
        command='touch extract_tbp_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    transform_vprok = DockerOperator(
        task_id='transform_vprok',
        image='airflow_task_python_3.9',
        container_name='transform_vprok_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch transform_vprok_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    transform_compass = DockerOperator(
        task_id='transform_compass',
        image='airflow_task_python_3.9',
        container_name='transform_compass_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch transform_compass_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    transform_ght = DockerOperator(
        task_id='transform_ght',
        image='airflow_task_python_3.9',
        container_name='transform_ght_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch transform_ght_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    load_subdivision = DockerOperator(
        task_id='load_subdivision',
        image='airflow_task_python_3.9',
        container_name='load_subdivision_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch load_subdivision_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    load_message_176 = DockerOperator(
        task_id='load_message_176',
        image='airflow_task_python_3.9',
        container_name='load_message_176_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch load_message_176_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    load_message_173 = DockerOperator(
        task_id='load_message_173',
        image='airflow_task_python_3.9',
        container_name='load_message_173_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch load_message_173_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    load_message_347 = DockerOperator(
        task_id='load_message_347',
        image='airflow_task_python_3.9',
        container_name='load_message_347_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': "{{ ds }}"
        },
        mounts=[
            Mount(
                source=folder,
                target='/tmp/tmp',
                type='bind'
            )
        ],
        command='touch load_message_347_{{ ds }}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'rm -r /opt/airflow/tmp/{today}',
        trigger_rule='none_skipped'
    )

    create_folder >> extract_tbp
    extract_tbp >> [transform_vprok, transform_compass, transform_ght] >> load_subdivision
    load_subdivision >> [load_message_176, load_message_173, load_message_347] >> delete_folder


tutorial_etl_dag = for_sites_netcat()
