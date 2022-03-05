from airflow.models import Connection
from airflow.decorators import dag
from airflow.utils.dates import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from docker.types import Mount

ms_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
mongo_pass = Variable.get('secret_mongo_pass')
mongo_login = Variable.get('mongo_login')
main_folder = Variable.get('main_folder')
project_name = 'checks_ms_in_mongo'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'

# The start of the data interval as YYYYMMDD
mount_dir_server = f'{folder}'

mount_dir = [
    Mount(
        source=mount_dir_server,
        target=working_dir,
        type='bind'
    ),
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
        'retries': 0
    },
    dag_id=project_name,
    tags=['ms', 'mongo', 'checks'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2020, 1, 1),
    catchup=True
)
def checks_ms_in_mongo():
    b_date = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
    e_date = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'mkdir -p -m 777 {airflow_work_dir}/{b_date}'
    )

    mongodb = mongo_connect[0]
    mongodb['login'] = mongo_login
    mongodb['password'] = mongo_pass

    ms_c = {
        'host': ms_connect.host,
        'password': ms_connect.password,
        'login': ms_connect.login,
        'database': ms_connect.schema
    }

    mongodb_s = str(list(mongodb.items())).replace(', ', ',')
    ms_s = str(list(ms_c.items())).replace(', ', ',')

    e_headers = DockerOperator(
        task_id='e_headers',
        image=image,
        container_name='e_headers_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
            'E_EXECUTION_DATE': e_date,
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_headers.py $B_EXECUTION_DATE $E_EXECUTION_DATE $MS"',
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
            'B_EXECUTION_DATE': b_date,
            'E_EXECUTION_DATE': e_date,
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_payments.py $B_EXECUTION_DATE $E_EXECUTION_DATE $MS"',
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
            'B_EXECUTION_DATE': b_date,
            'E_EXECUTION_DATE': e_date,
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_products.py $B_EXECUTION_DATE $E_EXECUTION_DATE $MS"',
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
            'B_EXECUTION_DATE': b_date,
            'E_EXECUTION_DATE': e_date,
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_lottery_tickets.py $B_EXECUTION_DATE $E_EXECUTION_DATE $MS"',
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
            'B_EXECUTION_DATE': b_date,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/t_data.py $B_EXECUTION_DATE"',
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
            'B_EXECUTION_DATE': b_date,
            'MONGO': mongodb_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_insert_many.py $B_EXECUTION_DATE $MONGO"',
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
            'B_EXECUTION_DATE': b_date,
            'MONGO': mongodb_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_update.py $B_EXECUTION_DATE $MONGO"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        trigger_rule='one_failed'
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'rm -r {airflow_work_dir}/{b_date}',
        trigger_rule='none_skipped'
    )

    create_folder >> [e_headers, e_products, e_payments, e_lottery_tickets] >> t_data >> l_insert_many
    l_insert_many >> [l_update, delete_folder]
    l_update >> delete_folder


tutorial_etl_dag = checks_ms_in_mongo()
