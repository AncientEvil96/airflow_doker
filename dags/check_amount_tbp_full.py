from docker.types import Mount

from airflow.models import Connection
from airflow.decorators import dag
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

ms_connect = Connection.get_connection_from_secrets(conn_id='MS_CHECK_CHANGE')
maria_connect = Connection.get_connection_from_secrets(conn_id='Maria_TBP_transfer')
main_folder = Variable.get('main_folder')

project_name = 'check_amount_tbp_full'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'
mount_dir_server = f'{folder}'
ssh_key = f'/opt/airflow/.ssh/sshkey_file'
sshHook = SSHHook(ssh_conn_id="ssh_222_180", key_file=ssh_key)

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
    tags=['ms', 'amount', 'change', 'full'],
    schedule_interval='0 10 * * *',
    start_date=datetime(2022, 5, 17),
    catchup=False,
    max_active_runs=1,
)
def update_amount_tbp():
    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'bash -c "mkdir -p -m 777 {airflow_work_dir}"'
    )

    delete_folder_success = BashOperator(
        task_id='delete_folder_success',
        bash_command=f'bash -c "rm -r {airflow_work_dir}"',
        trigger_rule='one_success'
    )

    delete_folder_failed = BashOperator(
        task_id='delete_folder_failed',
        bash_command=f'bash -c "rm -r {airflow_work_dir}"',
        trigger_rule='one_failed'
    )

    ms_s_ = {
        'host': ms_connect.host,
        'password': ms_connect.password,
        'login': ms_connect.login,
        'database': ms_connect.schema
    }

    maria_s_ = {
        'host': maria_connect.host,
        'password': maria_connect.password,
        'login': maria_connect.login,
        'database': maria_connect.schema,
        'port': maria_connect.port
    }

    ms_s = str(list(ms_s_.items())).replace(', ', ',')
    maria_s = str(list(maria_s_.items())).replace(', ', ',')

    e_amount_ms = DockerOperator(
        task_id='e_amount_ms',
        image=image,
        container_name='e_amount_ms{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'MS': ms_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_amount_ms.py $MS"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        retries=0
    )

    e_amount_maria = DockerOperator(
        task_id='e_amount_maria',
        image=image,
        container_name='e_amount_maria{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'MARIA': maria_s
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_amount_maria.py $MARIA"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        retries=0
    )

    t_amount = DockerOperator(
        task_id='t_amount',
        image=image,
        container_name='t_amount{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/t_amount.py"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        retries=0,
        trigger_rule='all_success'
    )

    tar_file = 'amount_new.tar.gz'

    tar_data = BashOperator(
        task_id='tar_data',
        bash_command=f'bash -c "cd {airflow_work_dir}/ && tar -czvf {tar_file} amount_new.csv"',
        trigger_rule='all_success'
    )

    ssh_mkdir_folder = SSHOperator(
        task_id="ssh_mkdir_folder",
        command=f'bash -c "mkdir -p -m 777 /tmp/load/{project_name}"',
        ssh_hook=sshHook,
        trigger_rule='all_success'
    )

    scp_data = BashOperator(
        task_id='scp_data',
        bash_command=f'scp -P 54322 -i {ssh_key} {airflow_work_dir}/{tar_file} root@178.57.222.180:/tmp/load/{project_name}/',
        trigger_rule='all_success'
    )

    ssh_untar_data = SSHOperator(
        task_id="ssh_untar_data",
        timeout=10,
        command=f"tar -xzvf /tmp/load/{project_name}/{tar_file} -C /tmp/load/{project_name}/",
        ssh_hook=sshHook,
        trigger_rule='all_success'
    )

    l_amount = DockerOperator(
        task_id='l_amount',
        image=image,
        container_name='l_amount{{ task_instance.job_id }}',
        api_version='auto',
        auto_remove=True,
        environment={
            'MARIA': maria_s,
            'MAIN_D': project_name
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/l_amount.py $MARIA $MAIN_D"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        trigger_rule='all_success'
    )

    ssh_mr_file = SSHOperator(
        task_id="ssh_mr_file",
        timeout=10,
        command=f"rm -r /tmp/load/{project_name}",
        trigger_rule='one_success',
        ssh_hook=sshHook
    )

    create_folder >> [e_amount_ms, e_amount_maria] >> t_amount
    t_amount >> tar_data
    tar_data >> ssh_mkdir_folder
    ssh_mkdir_folder >> scp_data
    scp_data >> ssh_untar_data
    ssh_untar_data >> l_amount
    l_amount >> [ssh_mr_file, delete_folder_success]
    t_amount >> delete_folder_failed


tutorial_etl_dag = update_amount_tbp()
