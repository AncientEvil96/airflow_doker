import pandas as pd
from dags.bases.save_to_file import SaveFile
from dags.bases.ms import MsSQL
from datetime import datetime

def transform(file):
    df = pd.read_parquet(file)
    print(df)
    sf = SaveFile()
    return sf.create_file_parquet(df=df, file_name=file)


def load(file):
    df = pd.read_parquet(file)
    full_list = []
    for i, row in df.iterrows():
        line_d = {
            'uuid': row['uuid'],
            'phone': row['phone'],
            'shop_name': row['shop_name'],
            'product_name': row['product_name'],
            'created_at': str(row['created_at'].date()),
            'refund': row['refund'],
            'cancellation': row['cancellation'],
            'shop_id': row['shop_id'],
            'barcode': row['barcode'],
            'product_id': row['product_id'],
            'amount': row['amount'],
            'price': row['price']
        }
        full_list.append(line_d)


if __name__ == '__main__':
    dd = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    t_begin = str(datetime(dd.year + 2000, dd.month, dd.day, dd.hour, dd.minute, dd.second))
    t_end = str(datetime(dd.year + 2000, dd.month, dd.day, 23, 59, 59))

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

    ms_connect = {
        'host': '192.168.0.175',
        'database': 'ChekKKM',
        'login': 'sa',
        'password': 'Delay159'
    }

    sourse = MsSQL(params=ms_connect)
    file = sourse.select_db_df(query, t_begin)
    # file = transform(file)
    # load(file)
