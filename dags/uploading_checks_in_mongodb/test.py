import pandas as pd
from dags.bases.operations_to_files import SaveFile
from dags.bases.ms import MsSQL
from datetime import datetime
from dags.bases.mongo import Mongo
from json import loads


def transform(file):
    df = pd.read_parquet(file)

    for i, row in df.iterrows():
        print(list(row))

    sf = SaveFile()
    return sf.create_file_parquet(df=df, file_name=file.replace('.parquet.gzip', ''))


def load(file):
    df = pd.read_parquet(file)
    load_list = df.to_json(orient='records')
    mongo = Mongo(
        params={
            "host": "84.38.187.211",
            "port": "27017",
            "schema": "info_checks",
            "database": "checks",
            "login": "transfer",
            "password": "QXm6ditoC06BaoA6iZbS"
        }
    )
    mongo.update_mongo(loads(load_list))


if __name__ == '__main__':
    dd = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    t_begin = str(datetime(dd.year + 2000, dd.month, dd.day, dd.hour, dd.minute, dd.second))
    t_end = str(datetime(dd.year + 2000, dd.month, dd.day, 23, 59, 59))

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
        and [_Document16].[_Date_Time] between '{t_begin}' and '{t_end}'
        """

    ms_connect = {
        'host': '192.168.0.175',
        'database': 'ChekKKM',
        'login': 'sa',
        'password': 'Delay159'
    }

    sourse = MsSQL(params=ms_connect)
    file = sourse.select_db_df(query, f'ch_{(dd.year - 2000):03}_{dd.month:03}_{dd.day:03}')
    # file = transform(file)
    load(file)
