from airflow.models.connection import Connection
from airflow.decorators import dag, task
from datetime import date
from ..bases.ms import MsSQL
from ..bases.mongo import Mongo
from airflow.models import Variable
from copy import deepcopy

ms_connect = Connection(conn_id='MS_ChekKKM')
mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
mongo_pass = Variable.get('secret_mongo_pass')
mongo_login = Variable.get('mongo_login')


@dag(
    default_args={
        'owner': 'airflow',
        'email': ['bersek123@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    dag_id='checks_ms_in_mongo',
    tags=['ms', 'mongo', 'checks'],
    schedule_interval='0 0 * * *',
    start_date=date(2020, 1, 1),
    catchup=False
)
def checks_ms_in_mongo():
    t_begin = '{{ ds }}'
    t_end = '{{ ds }}'

    query = f"""
    SELECT top 100 substring(sys.fn_sqlvarbasetostr([_Document16].[_IDRRef]),3,32) as uuid
          ,CONVERT(int, [_Document16].[_Marked]) as del_mark
          ,DATEADD(year, -2000 ,[_Date_Time]) as created_at
          ,[_NumberPrefix] as prefix
          ,[_Number] as number
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
          ,CASE WHEN [_Reference73]._Description = '999-999-99-99' THEN NUll ELSE REPLACE([_Reference73]._Description,'-','') END as phone
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
        LEFT JOIN [dbo].[_Reference37] AS [_Reference37]
        ON [_Document16].[_Fld19RRef] = [_Reference37].[_IDRRef]
        INNER JOIN [dbo].[_Reference73] AS [_Reference73]
        ON [_Document16].[_Fld117RRef] = [_Reference73].[_IDRRef]
    WHERE [_Posted] = 1
    and [_Document16].[_Date_Time] between '{t_begin}' and '{t_end}'
    """

    @task(task_id='extract')
    def extract(query):
        sourse = MsSQL(params=ms_connect)
        return sourse.select_db_df(query)

    @task(task_id='transform')
    def transform(file):

        return file_path

    @task(task_id='load')
    def load(file_path):
        mongodb = ''
        target = Mongo(params=deepcopy(mongodb))

        return target.callback_rebbit()


    file_path = extract(query)
    file_path = transform(file_path)
    load(file_path)

tutorial_etl_dag = checks_ms_in_mongo()
