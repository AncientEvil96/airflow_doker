from airflow.models import Connection
from airflow.decorators import dag
from airflow.utils.dates import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import macros
from docker.types import Mount

ms_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
mongo_pass = Variable.get('secret_mongo_pass')
mongo_login = Variable.get('mongo_login')

user_folder = 'deus'

main_folder = f'/home/{user_folder}/PycharmProjects/airflow_doker'
folder = f'{main_folder}/tmp/mssql_checks_in_mongodb_{today}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/mssql_checks_in_mongodb_{today}'
image = 'airflow_task_python_3.8'

mount_dir = [
    Mount(
        source=folder,
        target=working_dir,
        type='bind'
    ),
    Mount(
        source=f'{main_folder}/project/mssql_checks_in_mongodb',
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
        'retries': 0
    },
    dag_id='checks_ms_in_mongo',
    tags=['ms', 'mongo', 'checks'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def checks_ms_in_mongo():
    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'mkdir -m 777 {{ ts_nodash }}'
    )

    day_ago = -1
    mongodb = {}
    e_headers = DockerOperator(
        task_id='e_headers',
        image=image,
        container_name='e_headers_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_headers.py {ms_connect}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_payments = DockerOperator(
        task_id='e_payments',
        image=image,
        container_name='e_payments_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_payments.py {ms_connect}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_products = DockerOperator(
        task_id='e_products',
        image=image,
        container_name='e_products_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_products.py {ms_connect}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_lottery_tickets = DockerOperator(
        task_id='e_lottery_tickets',
        image=image,
        container_name='e_lottery_tickets_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_lottery_tickets.py {ms_connect}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t_data = DockerOperator(
        task_id='t_data',
        image=image,
        container_name='t_data_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/t_data.py"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    l_insert_many = DockerOperator(
        task_id='l_insert_many',
        image=image,
        container_name='l_insert_many_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_insert_many.py {mongodb}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    l_update = DockerOperator(
        task_id='l_update',
        image=image,
        container_name='l_update_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': "{{ ds }}",
            # 'B_EXECUTION_DATE': "{{ ts_nodash }}",
            # 'E_EXECUTION_DATE': "{{ macros.ds_add(ts_nodash, %s) }}" % day_ago,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_update.py {mongodb}"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        trigger_rule='one_failed'
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'rm -r {airflow_work_dir}',
        trigger_rule='one_success'
    )

    [e_headers, e_products, e_payments, e_lottery_tickets] >> t_data >> l_insert_many
    l_insert_many >> [l_update, delete_folder]
    l_update >> delete_folder


tutorial_etl_dag = checks_ms_in_mongo()
