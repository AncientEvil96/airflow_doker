from airflow.models import Connection
from airflow.decorators import dag
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from docker.types import Mount

ms_tbp_connect = Connection.get_connection_from_secrets(conn_id='MS_TBP_WORK')
ms_ch_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
mongo_s_connect = Connection.get_connection_from_secrets(conn_id='Mongo_Directory')
mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
mongo_pass = Variable.get('secret_mongo_pass')
mongo_login = Variable.get('mongo_login')
main_folder = Variable.get('main_folder')

project_name = 'mongo_stocks'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'
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
    tags=['ms', 'mongo', 'stocks', 'directory'],
    # schedule_interval=timedelta(days=1),
    schedule_interval='10 3 * * *',
    start_date=datetime(2022, 3, 30),
    catchup=False,
    max_active_runs=1,

)
def checks_ms_in_mongo():
    b_date = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
    e_date = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'bash -c "mkdir -p -m 777 {airflow_work_dir}/{b_date}"'
    )

    mongodb_s_ = {
        'host': mongo_s_connect.host,
        'port': mongo_s_connect.port,
        'password': mongo_s_connect.password,
        'login': mongo_s_connect.login,
        'database': 'stocks',
        'schema': mongo_s_connect.schema
    }

    ms_ch_ = {
        'host': ms_ch_connect.host,
        'password': ms_ch_connect.password,
        'login': ms_ch_connect.login,
        'database': ms_ch_connect.schema
    }

    ms_tbp_ = {
        'host': ms_tbp_connect.host,
        'password': ms_tbp_connect.password,
        'login': ms_tbp_connect.login,
        'database': ms_tbp_connect.schema
    }

    mongodb_s = str(list(mongodb_s_.items())).replace(', ', ',')
    ms_ch = str(list(ms_ch_.items())).replace(', ', ',')
    ms_tbp = str(list(ms_tbp_.items())).replace(', ', ',')

    e_mongo_stock = DockerOperator(
        task_id='e_mongo_stock',
        image=image,
        container_name='e_mongo_stock{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
            'MONGO': mongodb_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_mongo_stock.py $B_EXECUTION_DATE $MONGO"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_shops_from_checks = DockerOperator(
        task_id='e_shops_from_checks',
        image=image,
        container_name='e_shops_from_checks_{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
            'E_EXECUTION_DATE': e_date,
            'MS': ms_ch
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_shops_from_checks.py $B_EXECUTION_DATE $E_EXECUTION_DATE $MS"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_shops_from_tbp = DockerOperator(
        task_id='e_shops_from_tbp',
        image=image,
        container_name='e_shops_from_tbp_{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
            'MS': ms_tbp
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_shops_from_tbp.py $B_EXECUTION_DATE $MS"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t_merge_data = DockerOperator(
        task_id='t_merge_data',
        image=image,
        container_name='t_merge_data_{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/t_merge_data.py $B_EXECUTION_DATE"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    l_mongo_stock = DockerOperator(
        task_id='l_mongo_stock',
        image=image,
        container_name='l_mongo_stock_{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date,
            'MONGO': mongodb_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_mongo_stock.py $B_EXECUTION_DATE $MONGO"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'bash -c "rm -r {airflow_work_dir}/{b_date}"'
    )

    create_folder >> [e_mongo_stock, e_shops_from_tbp, e_shops_from_checks] >> t_merge_data
    t_merge_data >> l_mongo_stock >> delete_folder


tutorial_etl_dag = checks_ms_in_mongo()
