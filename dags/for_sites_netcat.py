from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Connection

ms_connect = Connection.get_connection_from_secrets(conn_id='MS_TBP_WORK')
my_connect = Connection.get_connection_from_secrets(conn_id='MariaDB_VPROK')

user_folder = 'deus'
project_name = 'for_sites_netcat'
main_folder = f'/home/{user_folder}/PycharmProjects/airflow_doker'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'

mount_dir = [
    Mount(
        source=folder,
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
        'retries': 0,
    },
    dag_id='for_sites_netcat',
    tags=['netcat', 'vprok', 'compass'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 28),
    catchup=False,
    max_active_runs=1
)
def for_sites_netcat():
    ms_c = {
        'host': ms_connect.host,
        'password': ms_connect.password,
        'login': ms_connect.login,
        'database': ms_connect.schema
    }

    my_c = {
        'host': my_connect.host,
        'port': my_connect.port,
        'password': my_connect.password,
        'login': my_connect.login,
        'database': my_connect.schema
    }

    ms_s = str(list(ms_c.items())).replace(', ', ',')
    my_s = str(list(my_c.items())).replace(', ', ',')

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'mkdir -m 777 {airflow_work_dir}'
    )

    e_subdivision_tbp = DockerOperator(
        task_id='e_subdivision_tbp',
        image=image,
        container_name='e_subdivision_tbp_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_subdivision_tbp.py $MS"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    tl_subdivision_vprok = DockerOperator(
        task_id='tl_subdivision_vprok',
        image=image,
        container_name='tl_subdivision_vprok_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MY': my_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/tl_subdivision_vprok.py $MY"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    etl_sub_class_vprok = DockerOperator(
        task_id='etl_sub_class_vprok',
        image=image,
        container_name='etl_sub_class_vprok_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MY': my_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/etl_sub_class_vprok.py $MY"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    e_product_tbp = DockerOperator(
        task_id='e_product_tbp',
        image=image,
        container_name='e_product_tbp_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_product_tbp.py $MS"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    tl_product_vprok_173 = DockerOperator(
        task_id='tl_product_vprok_173',
        image=image,
        container_name='tl_product_vprok_173_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MY': my_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/tl_product_vprok_173.py $MY"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    tl_product_vprok_176 = DockerOperator(
        task_id='tl_product_vprok_176',
        image=image,
        container_name='tl_product_vprok_176_{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MY': my_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/tl_product_vprok_176.py $MY"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'rm -r {airflow_work_dir}',
        trigger_rule='none_skipped'
    )

    create_folder >> e_subdivision_tbp >> tl_subdivision_vprok >> etl_sub_class_vprok
    etl_sub_class_vprok >> e_product_tbp >> [tl_product_vprok_173, tl_product_vprok_176] >> delete_folder


tutorial_etl_dag = for_sites_netcat()
