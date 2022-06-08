from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from docker.types import Mount
from airflow.models import Connection
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

maria_connect = Connection.get_connection_from_secrets(conn_id='MariaDB_tbp_211')
clickhouse_connect = Connection.get_connection_from_secrets(conn_id='ClickHouse_srv3')
main_folder = Variable.get('main_folder')

project_name = 'tbp_consultant_to_ch_full'
folder = f'{main_folder}/tmp/{project_name}'
working_dir = '/tmp/tmp'
airflow_work_dir = f'/opt/airflow/tmp/{project_name}'
image = 'airflow_task_python_3.8'
ssh_key = f'/opt/airflow/.ssh/sshkey_file'
sshHook = SSHHook(ssh_conn_id="ssh_srv3", key_file=ssh_key)

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
    dag_id=project_name,
    tags=['clickhouse', 'consultant', 'tbp'],
    schedule_interval=timedelta(days=1),
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=4
)
def for_sites_netcat():
    b_date = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'

    maria_ = {
        'host': maria_connect.host,
        'password': maria_connect.password,
        'login': maria_connect.login,
        'port': maria_connect.port,
        'database': maria_connect.schema
    }

    maria_s = str(list(maria_.items())).replace(', ', ',')

    create_folder = BashOperator(
        task_id='create_folder',
        bash_command=f'bash -c "mkdir -p -m 777 {airflow_work_dir}/{b_date}"'
    )

    delete_folder = BashOperator(
        task_id='delete_folder',
        bash_command=f'bash -c "rm -r {airflow_work_dir}/{b_date}"',
        trigger_rule='none_skipped'
    )

    e_consultant = DockerOperator(
        task_id='e_consultant.py',
        image=image,
        container_name='e_consultant.py{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'MARIA': maria_s,
            'B_EXECUTION_DATE': b_date
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/e_consultant.py $B_EXECUTION_DATE $MARIA"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        retries=0,
    )

    t_consultant = DockerOperator(
        task_id='t_consultant',
        image=image,
        container_name='t_consultant{{ task_instance.job_id }}',
        api_version='1.41',
        auto_remove=True,
        environment={
            'B_EXECUTION_DATE': b_date
        },
        mounts=mount_dir,
        working_dir=working_dir,
        command=f'bash -c "python project/t_consultant.py $B_EXECUTION_DATE"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        retries=0
    )

    tar_data = BashOperator(
        task_id='tar_data',
        bash_command=f'bash -c "cd {airflow_work_dir}/{b_date}/ && tar -czvf consultant{b_date}.tar.gz consultant.parquet.gzip"'
    )

    scp_data = BashOperator(
        task_id='scp_data',
        bash_command=f'scp -P 54322 -i {ssh_key} {airflow_work_dir}/{b_date}/consultant{b_date}.tar.gz root@prod-srv3.tkvprok.ru:~/'
    )

    docker_create_folder = SSHOperator(
        task_id="docker_create_folder",
        command=f"docker exec clickhouse_server_1 mkdir -p -m 777 /tmp/{b_date}/",
        timeout=5,
        ssh_hook=sshHook)

    docker_cp_data = SSHOperator(
        task_id="docker_cp_data",
        command=f"docker cp /root/consultant{b_date}.tar.gz clickhouse_server_1:/tmp/{b_date}/",
        ssh_hook=sshHook)

    docker_untar_data = SSHOperator(
        task_id="docker_untar_data",
        timeout=10,
        command=f"docker exec clickhouse_server_1 tar -xzvf /tmp/{b_date}/consultant{b_date}.tar.gz -C /tmp/{b_date}/",
        ssh_hook=sshHook)

    load_data_to_base = SSHOperator(
        task_id="load_data_to_base",
        timeout=20,
        command=f'docker exec clickhouse_server_1 bash -c "cat /tmp/{b_date}/consultant.parquet.gzip | clickhouse-client -u {clickhouse_connect.login} --password {clickhouse_connect.password} --query \'INSERT INTO consultant.consultant_v2 FORMAT Parquet\'"',
        ssh_hook=sshHook)

    query = f"""
        ALTER TABLE consultant.consultant_v2 DELETE WHERE toYYYYMMDD(toDate(created_at)) = {b_date};
        ALTER TABLE consultant.consultant_view DELETE WHERE toYYYYMMDD(toDate(created_at)) = {b_date};
    """

    i = 0
    for table in query.strip().split('\n'):
        i += 1
        del_old_data = SimpleHttpOperator(
            task_id=f'del_old_data_{i}',
            http_conn_id='ClickHouse_srv3',
            method='POST',
            data=table.strip(),
        )
        docker_untar_data >> del_old_data >> load_data_to_base

    docker_mr_folder = SSHOperator(
        task_id="docker_mr_folder",
        timeout=10,
        command=f"docker exec clickhouse_server_1 rm -r /tmp/{b_date}/",
        trigger_rule='none_skipped',
        ssh_hook=sshHook)

    mr_file_to_srv = SSHOperator(
        task_id="mr_folder_to_srv",
        timeout=10,
        command=f"rm -r /root/consultant{b_date}.tar.gz",
        trigger_rule='none_skipped',
        ssh_hook=sshHook)

    create_folder >> e_consultant
    e_consultant >> t_consultant >> tar_data
    tar_data >> scp_data >> docker_create_folder >> docker_cp_data >> docker_untar_data
    load_data_to_base >> [docker_mr_folder, mr_file_to_srv, delete_folder]


tutorial_etl_dag = for_sites_netcat()
