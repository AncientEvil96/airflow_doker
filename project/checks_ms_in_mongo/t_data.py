from base.operations_to_files import File
import pandas as pd
from sys import argv
from datetime import datetime
from pathlib import Path
import re

begin_dt = argv[1]
local_dir = f'/tmp/tmp/{begin_dt}/'


def get_date(date_):
    executor_date = datetime.strptime(str(date_), '%Y%m%d')
    return datetime(
        executor_date.year + 2000,
        executor_date.month,
        executor_date.day,
        executor_date.hour,
        executor_date.minute,
        executor_date.second
    )


def transform():
    t_begin = get_date(begin_dt)

    files = sorted(
        list(map(str, list(Path(local_dir).rglob(f'*.parquet.gzip')))))

    headers = [x for x in files if re.match('.*checks_headers*', x)][0] if any(
        "checks_headers" in word for word in files) else ''
    lottery_tickets = [x for x in files if re.match('.*checks_lottery_tickets*', x)][0] if any(
        "checks_lottery_tickets" in word for word in files) else ''
    payments = [x for x in files if re.match('.*checks_payments*', x)][0] if any(
        "checks_payments" in word for word in files) else ''
    products = [x for x in files if re.match('.*checks_products*', x)][0] if any(
        "checks_products" in word for word in files) else ''

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

    file = File(f'{local_dir}checks')
    return file.create_file_parquet(headers)


if __name__ == '__main__':
    transform()
