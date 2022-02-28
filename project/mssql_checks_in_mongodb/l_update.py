from base.ms import MsSQL
from base.mongo import Mongo
from base.operations_to_files import File
import pandas as pd

def _delete_file(ti):
    load_files = ti.xcom_pull(key='return_value', task_ids=['extract_headers', 'extract_products', 'extract_payments',
                                                            'extract_lottery_tickets', 'transform'])

    file = File('')
    for line in load_files:
        if not line:
            continue
        file.file_name = line
        file.delete_file()


def _load_insert_many(**kwargs):
    load_list = kwargs['ti'].xcom_pull(key='return_value', task_ids=['transform'])[0]

    df = pd.read_parquet(load_list)
    df[['products', 'payments', 'lottery_tickets']] = df[['products', 'payments', 'lottery_tickets']].apply(
        lambda x: [list(i) for i in x])

    load_list = df.to_dict('records')

    mongodb = {}

    for line in mongo_connect:
        if line['database'] == 'checks' and line['schema'] == 'info_checks':
            mongodb = line
            break

    mongodb['login'] = mongo_login
    mongodb['password'] = mongo_pass

    target = Mongo(params=deepcopy(mongodb))
    target.insert_mongo(load_list)


def _load_update(**kwargs):
    load_list = kwargs['ti'].xcom_pull(key='return_value', task_ids=['transform'])[0]

    df = pd.read_parquet(load_list)
    df[['products', 'payments', 'lottery_tickets']] = df[['products', 'payments', 'lottery_tickets']].apply(
        lambda x: [list(i) for i in x])

    load_list = df.to_dict('records')

    mongodb = {}
    for line in mongo_connect:
        if line['database'] == 'checks' and line['schema'] == 'info_checks':
            mongodb = line
            break

    mongodb['login'] = mongo_login
    mongodb['password'] = mongo_pass

    target = Mongo(params=deepcopy(mongodb))
    target.update_mongo(load_list)


def _extract_headers(yesterday, sourse):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    t_begin = datetime(executor_date.year + 2000, executor_date.month, executor_date.day)
    t_end = t_begin + timedelta(days=1)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16].[_IDRRef]),3,32) as uuid_db
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
                  ,CASE WHEN [_Reference73]._Description = '999-999-99-99' THEN 0 ELSE CONVERT(bigint, REPLACE([_Reference73]._Description,'-','')) END as phone
                  ,CONVERT(int, [_Fld22]) as number_check
                  ,CONVERT(int, [_Fld272]) as promo_code
                  ,CONVERT(int, [_Fld164]) as r_number_check
                  ,CONVERT(int, [_Fld273]) as gold_585
                  ,CASE WHEN [_Reference73]._Description = '999-999-99-99' THEN 0 ELSE CONVERT(int, [_Fld118]) END as employee
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

    return sourse.select_to_file(
        query,
        f'tmp/checks_headers_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}'
    )


def _extract_products(yesterday, sourse):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    t_begin = datetime(executor_date.year + 2000, executor_date.month, executor_date.day)
    t_end = t_begin + timedelta(days=1)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid_db
                ,[_LineNo25] as line
                ,CONVERT(bigint, [_Fld97]) as barcode
                ,[_Fld99] as id_product
                ,[_Fld100] as name
                ,[_Fld101] as amount
                ,CONVERT(decimal(10,2), [_Fld102]) as price
                ,CONVERT(decimal(10,2), [_Fld103]) as summ
                ,[_Fld104] as gift_sertificate
                ,CONVERT(int, [_Fld287]) as promo_virt_bangle
                ,CONVERT(int, [_Fld308]) as promo_code
                ,CONVERT(decimal(10,2), [_Fld247]) as yield
                ,CONVERT(decimal(10,2), [_Fld248]) as purchase_price
                ,CONVERT(decimal(10,2), [_Fld376]) as cost_price
                ,[_Fld644] as code_markings
            FROM [ChekKKM].[dbo].[_Document16_VT24] as [_Document16_VT24]
                INNER JOIN [ChekKKM].[dbo].[_Document16] as [_Document16]
                ON [_Document16].[_IDRRef] = [_Document16_VT24].[_Document16_IDRRef]
            WHERE [_Posted] = 1 and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """

    return sourse.select_to_file(
        query,
        f'tmp/checks_products_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}'
    )


def _extract_payments(yesterday, sourse):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    t_begin = datetime(executor_date.year + 2000, executor_date.month, executor_date.day)
    t_end = t_begin + timedelta(days=1)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid_db
                ,[_LineNo89] as line
                ,[_Fld92] as [type]
                ,CONVERT(decimal(10,2), [_Fld93]) as summ
                ,CONVERT(decimal(10,2), [_Fld94]) as discount
                ,[_Fld95] as gift_sertificate
                ,[_Fld96] as bankcard
            FROM [ChekKKM].[dbo].[_Document16_VT88] as [_Document16_VT88]
                INNER JOIN [ChekKKM].[dbo].[_Document16] as [_Document16]
                ON [_Document16].[_IDRRef] = [_Document16_VT88].[_Document16_IDRRef]
            WHERE [_Posted] = 1 and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """

    return sourse.select_to_file(
        query,
        f'tmp/checks_payments_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}'
    )


def _extract_lottery_tickets(yesterday, sourse):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    t_begin = datetime(executor_date.year + 2000, executor_date.month, executor_date.day)
    t_end = t_begin + timedelta(days=1)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid_db
                  ,[_LineNo110] as line
                  ,CONVERT(int, [_Fld111]) as number
                  ,CONVERT(int, [_Fld116]) as employee_ticket_number
            FROM [ChekKKM].[dbo].[_Document16_VT109] as [_Document16_VT109]
                INNER JOIN [ChekKKM].[dbo].[_Document16] as [_Document16]
                ON [_Document16].[_IDRRef] = [_Document16_VT109].[_Document16_IDRRef]
            WHERE [_Posted] = 1 and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """

    return sourse.select_to_file(
        query,
        f'tmp/checks_lottery_tickets_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}'
    )


def _transform(yesterday, ti):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    headers = ti.xcom_pull(key='return_value', task_ids=['extract_headers'])[0]
    products = ti.xcom_pull(key='return_value', task_ids=['extract_products'])[0]
    payments = ti.xcom_pull(key='return_value', task_ids=['extract_payments'])[0]
    lottery_tickets = ti.xcom_pull(key='return_value', task_ids=['extract_lottery_tickets'])[0]

    if headers:
        headers = pd.read_parquet(headers)
    else:
        print('not headers.')
        exit(1)

    if products:
        df = pd.read_parquet(products)
        df = df.groupby(['uuid_db']).agg(list)
        columns_name = df.columns.tolist()
        df['products'] = df[columns_name].apply(
            lambda x: x.apply(pd.Series).T.to_dict('records'),
            axis=1)

        headers = headers.merge(df['products'], left_on='uuid_db', right_on='uuid_db')
    else:
        headers['products'] = headers['uuid_db'].apply(lambda x: [])

    if payments:
        df = pd.read_parquet(payments)
        df = df.groupby(['uuid_db']).agg(list)

        columns_name = df.columns.tolist()
        df['payments'] = df[columns_name].apply(
            lambda x: x.apply(pd.Series).T.to_dict('records'),
            axis=1)

        headers = headers.merge(df['payments'], left_on='uuid_db', right_on='uuid_db')
    else:
        headers['payments'] = headers['uuid_db'].apply(lambda x: [])

    if lottery_tickets:
        df = pd.read_parquet(lottery_tickets)
        df = df.groupby(['uuid_db']).agg(list)

        columns_name = df.columns.tolist()
        df['lottery_tickets'] = df[columns_name].apply(
            lambda x: x.apply(pd.Series).T.to_dict('records'),
            axis=1)

        headers = headers.merge(df['lottery_tickets'], left_on='uuid_db', right_on='uuid_db')
    else:
        headers['lottery_tickets'] = headers['uuid_db'].apply(lambda x: [])

    file = File(f'tmp/checks_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}')
    return file.create_file_parquet(headers)

if __name__=='__main__':
