from airflow.models import Connection
from airflow.decorators import dag, task
from airflow.utils.dates import datetime, timedelta
from airflow.models import Variable
from bases.ms import MsSQL
from bases.mongo import Mongo
from copy import deepcopy

ms_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
mongo_pass = Variable.get('secret_mongo_pass')
mongo_login = Variable.get('mongo_login')


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
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def checks_ms_in_mongo():
    @task(task_id='extract')
    def extract(yesterday):
        executor_date = datetime.strptime(yesterday, '%Y-%m-%d')
        sourse = MsSQL(
            params={
                'host': ms_connect.host,
                'password': ms_connect.password,
                'login': ms_connect.login,
                'database': ms_connect.schema,
            }
        )

        t_begin = datetime(executor_date.year + 2000, executor_date.month, executor_date.day)
        t_end = t_begin + timedelta(days=1)

        query = f"""
            SELECT top 100 substring(sys.fn_sqlvarbasetostr([_Document16].[_IDRRef]),3,32) as uuid_db
                  ,CONVERT(int, [_Document16].[_Marked]) as del_mark
                  ,DATEADD(year, -2000 ,[_Date_Time]) as created_at
                  ,DATEADD(year, -2000 ,[_NumberPrefix]) as prefix
                  ,CONVERT(bigint, [_Number]) as number
                  ,CONVERT(int, [_Posted]) as posted
                  ,[_Fld564] as uuid
                  ,[_Fld90] as ip_cashbox
                  ,CONVERT(int, [_Fld17]) as cancellation
                  ,CONVERT(int, [_Fld21]) as shop_id
                  ,[_Reference37].[_Description] as shop_name
                  ,CONVERT(int, [_Fld18]) as refund
                  ,DATEADD(year, -2000 ,[_Fld208]) as check_open
                  ,CONVERT(int, [_Fld119]) as im_mark
                  ,[_Fld23] as cashier_name
                  ,CONVERT(int, [_Fld345]) as count_chips
                  ,[_Fld242] as description
                  ,CONVERT(bigint, [_Fld20]) as number_cashbox
                  ,CASE WHEN [_Reference73]._Description = '999-999-99-99' THEN NUll ELSE CONVERT(bigint, REPLACE([_Reference73]._Description,'-','')) END as phone
                  ,CONVERT(int, [_Fld22]) as number_check
                  ,CONVERT(int, [_Fld272]) as promo_code
                  ,CONVERT(int, [_Fld164]) as r_number_check
                  ,CONVERT(int, [_Fld273]) as gold_585
                  ,CASE WHEN [_Reference73]._Description = '999-999-99-99' THEN NUll ELSE CONVERT(int, [_Fld118]) END as employee
                  ,CONVERT(decimal(10,2), [_Fld140]) as sum_sale
                  ,CONVERT(decimal(10,2), [_Fld139]) as sum_check
                  ,[_Fld141] as employee_id
                  ,[_Fld585] as manager_id
                  ,[_Fld586] as manager_name
                  ,[_Fld142] as type_pay
                  ,[_Fld445] as discount_compass
            FROM [ChekKKM].[dbo].[_Document16]
                LEFT JOIN [ChekKKM].[dbo].[_Reference37] AS [_Reference37]
                ON [_Document16].[_Fld19RRef] = [_Reference37].[_IDRRef]
                INNER JOIN [ChekKKM].[dbo].[_Reference73] AS [_Reference73]
                ON [_Document16].[_Fld117RRef] = [_Reference73].[_IDRRef]
            WHERE [_Posted] = 1
            and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """
        file = sourse.select_db_df(query, f'/opt/airflow/tmp/checks_{executor_date.strftime("%Y_%m_%d")}')
        return file

    @task(task_id='load')
    def load(file_path):
        print(file_path)
        # mongodb = ''
        # target = Mongo(params=deepcopy(mongodb))
        # return target.callback_rebbit()

    file_path = extract(yesterday='{{ ds }}')
    # print(file_path)
    # file_path = transform(file_path)
    load(file_path)


tutorial_etl_dag = checks_ms_in_mongo()
