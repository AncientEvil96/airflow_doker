from datetime import datetime
from dags.bases.operations_to_files import File
import pandas as pd
import numpy as np


# ms_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
# mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
# mongo_pass = Variable.get('secret_mongo_pass')
# mongo_login = Variable.get('mongo_login')

def _load_insert_many():
    load_list = 'tmp/checks_022_001_024.parquet.gzip'

    df = pd.read_parquet(load_list)
    # df['products'] = list(df['products'])
    # df['payments'] = list(df['payments'])
    # df['lottery_tickets'] = list(df['lottery_tickets'])
    load_list = df.to_dict('records')
    print(load_list)

    # for i in load_list:
    #     print(i)

    # print(load_list)


# для тестов
# полная агрегация
# db.checks.aggregate([{$group:{_id: {},totalAmount: { $sum: "$sum_check" },count: { $sum: 1 }}}])

# на день
# db.checks.aggregate(
#    [
#      {
#        $group:
#          {
#            _id: {$dateTrunc: {date: "$created_at", unit: "day"}},
#            totalAmount: { $sum: "$sum_check" },
#            count: { $sum: 1 }
#          }
#      }
#    ]
# )

def _transform(yesterday):
    executor_date = datetime.strptime(yesterday, '%Y-%m-%d')

    headers = 'tmp/checks_headers_022_001_024.parquet.gzip'
    products = 'tmp/checks_products_022_001_024.parquet.gzip'
    payments = 'tmp/checks_payments_022_001_024.parquet.gzip'
    lottery_tickets = None

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

    # df = pd.read_parquet(headers)
    # load_list = loads(df.to_json('records'))

    # print(headers.dtypes)
    # print(load_list)

    # print(headers[['products', 'payments', 'lottery_tickets']])
    # print(headers[['products', 'payments', 'lottery_tickets']])

    # headers[['products', 'payments', 'lottery_tickets']] = headers[['products', 'payments', 'lottery_tickets']].where(
    #     (pd.notnull(headers[['products', 'payments', 'lottery_tickets']])), [])

    # print(headers.columns.tolist())

    file = File(f'tmp/checks_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}')

    return file.create_file_parquet(headers)


if __name__ == '__main__':
    # _transform('2022-01-24')
    _load_insert_many()
