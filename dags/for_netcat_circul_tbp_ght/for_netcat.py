from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from airflow.providers.docker.operators.docker import DockerOperator, DockerHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator


# from airflow.models import Variable
#
# mongo_connect = Variable.get("mongo_connect", deserialize_json=True)
# mongo_pass = Variable.get("secret_mongo_pass")
# mongo_login = Variable.get("mongo_login")
# rebbit_srv = Variable.get("rebbit_srv", deserialize_json=True)
# rebbit_login = Variable.get("rebbit_login")
# rebbit_pass = Variable.get("secret_rebbit_pass")


@dag(
    default_args={
        'owner': 'Efimov Ilya',
        'email': ['bersek123@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    dag_id='a_for_netcat',
    tags=['netcat', 'ght', 'tbp', 'compass'],
    # schedule_interval='* * * * *',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 25),
    catchup=False,
    max_active_runs=4
)
def a_for_netcat():
    # t1 = BashOperator(
    #     task_id='print_current_date',
    #     bash_command='date'
    # )
    # t2 = DockerOperator(
    #     task_id='docker_command',
    #     image='ubuntu:latest',
    #     api_version='auto',
    #     auto_remove=True,
    #     environment={
    #         'AF_EXECUTION_DATE': "{{ ds }}",
    #         'AF_OWNER': "{{ task.owner }}"
    #     },
    #     command='echo "TASK ID (from macros): {{ task.task_id }} - EXECUTION DATE (from env vars): $AF_EXECUTION_DATE',
    #     container_name='a_for_netcat',
    #     cpus=1,
    #     mem_limit='1g',
    #     network_mode='bridge',
    #     tmp_dir='./tmp',
    #     user='test'
    # )
    # t3 = BashOperator(
    #     task_id='print_hello',
    #     bash_command='echo "hello world"'
    #
    # )
    # t1 >> t2 >> t3

    start_dag = DummyOperator(
        task_id='start_dag'
    )

    end_dag = DummyOperator(
        task_id='end_dag'
    )

    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )

    t2 = DockerOperator(
        task_id='docker_command_sleep',
        image='docker_image_task',
        container_name='task___command_sleep',
        api_version='auto',
        auto_remove=True,
        command="echo hello",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t3 = DockerOperator(
        task_id='docker_command_hello',
        image='docker_image_task',
        container_name='task___command_hello',
        api_version='auto',
        auto_remove=True,
        command="/bin/sleep 40",
        # docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t4 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello world"'
    )

    start_dag >> t1

    t1 >> t2 >> t4
    t1 >> t3 >> t4

    t4 >> end_dag


tutorial_etl_dag = a_for_netcat()
