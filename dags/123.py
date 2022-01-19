from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _err_loading(**kwargs):
    accuracy = kwargs['ti'].xcom_pull(key='return_value', task_ids=['load_insert_many'])
    print(accuracy)
    return accuracy


@dag(
    default_args={
        'owner': 'Efimov Ilya',
        'retries': 0
    },
    dag_id='branching',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def checks_ms_in_mongo():
    @task(task_id='extract')
    def extract(yesterday):
        return [11, 1, 1, 1]

    @task(task_id='load_insert_many')
    def load_insert_many(load_list):
        try:
            err = 1/0
            return 'exit_dag'
        except Exception as err:
            print(err)
            return 'load_update'

    @task(task_id='load_update')
    def load_update(**kwargs):
        print(1)

    err_loading = BranchPythonOperator(
        task_id='err_loading',
        python_callable=_err_loading,
    )

    exit_dag = DummyOperator(task_id='exit_dag')

    data_list = extract(yesterday='{{ ds }}')
    load_insert_many(data_list) >> err_loading >> [load_update(), exit_dag]


tutorial_etl_dag = checks_ms_in_mongo()
