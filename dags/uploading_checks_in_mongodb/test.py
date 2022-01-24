# from airflow.models import Connection
# from airflow.decorators import dag
# from airflow.utils.dates import datetime, timedelta
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from dags.bases.ms import MsSQL
from dags.bases.mongo import Mongo
from datetime import datetime
from dags.bases.operations_to_files import File
from copy import deepcopy
import pandas as pd


# ms_connect = Connection.get_connection_from_secrets(conn_id='MS_ChekKKM')
# mongo_connect = Variable.get('mongo_connect', deserialize_json=True)
# mongo_pass = Variable.get('secret_mongo_pass')
# mongo_login = Variable.get('mongo_login')


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

def _convert_colum(x):
    # df2 = pd.DataFrame(x)
    # print(df2.apply(pd.Series).stack())
    print()
    answer = []
    a = {}
    for i in x:
        if len(i) > 1:
            exit(0)
        a.update({'aasd': i[0]})
        answer.append(a)
    return answer


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
        # df = df.groupby(['uuid_db']).agg(lambda x: [(x.name, i) for i in list(x)])
        df = df.groupby(['uuid_db']).agg(list)
        columns_name = df.columns.tolist()
        df['products'] = df[columns_name].apply(
            lambda x: x.apply(pd.Series).T.to_dict('records'),
            axis=1)


        # for i in df[['products']].to_dict('records'):
        #     print(i)

        # print(df)

        # df['products'] = df[columns_name].apply(
        #     lambda x: [{x: y for x, y in i} for i in list(
        #         zip(x['line'],
        #             x['barcode'],
        #             x['id_product'],
        #             x['name'],
        #             x['amount'],
        #             x['price'],
        #             x['summ'],
        #             x['gift_sertificate'],
        #             x['promo_virt_bangle'],
        #             x['promo_code'],
        #             x['yield'],
        #             x['purchase_price'],
        #             x['cost_price'],
        #             x['code_markings']
        #             ))],
        #     axis=1
        # )
        headers = headers.merge(df['products'], left_on='uuid_db', right_on='uuid_db')
    if payments:
        df = pd.read_parquet(payments)
        df = df.groupby(['uuid_db']).agg(lambda x: [(x.name, i) for i in list(x)])

        columns_name = df.columns.tolist()
        df['products'] = df[columns_name].apply(
            lambda x: [{x: y for x, y in i} for i in _convert_colum(x[columns_name])],
            axis=1)

        # payments['payments'] = payments[payments.columns.drop('uuid_db').tolist()].apply(
        #     lambda x: [{x: y for x, y in i} for i in list(
        #         zip(x['line'],
        #             x['type'],
        #             x['summ'],
        #             x['discount'],
        #             x['gift_sertificate'],
        #             x['bankcard']
        #             ))],
        #     axis=1
        # )
        headers = headers.merge(df, left_on='uuid_db', right_on='uuid_db')

    if lottery_tickets:
        df = pd.read_parquet(lottery_tickets)
        df = df.groupby(['uuid_db']).agg(lambda x: [(x.name, i) for i in list(x)])

        columns_name = df.columns.tolist()
        df['products'] = df[columns_name].apply(
            lambda x: [{x: y for x, y in i} for i in _convert_colum(x[columns_name])],
            axis=1)

        # lottery_tickets['payments'] = lottery_tickets[lottery_tickets.columns.drop('uuid_db').tolist()].apply(
        #     lambda x: [{x: y for x, y in i} for i in list(
        #         zip(x['line'],
        #             x['number'],
        #             x['employee_ticket_number']
        #             ))],
        #     axis=1
        # )
        headers = headers.merge(df, left_on='uuid_db', right_on='uuid_db')

    file = File(f'tmp/checks_{(executor_date.year - 2000):03}_{executor_date.month:03}_{executor_date.day:03}')
    return file.create_file_parquet(headers)


if __name__ == '__main__':
    _transform('2022-01-21')
