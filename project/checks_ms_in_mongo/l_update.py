from sys import argv
import pandas as pd
from datetime import datetime
from pathlib import Path
from base.mongo import Mongo

begin_dt, mongodb_s = argv[1:]
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


if __name__ == '__main__':

    s = str(mongodb_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    mongodb = dict(zip(s[::2], s[1::2]))

    t_begin = get_date(begin_dt)

    load_list = ''
    try:
        load_list = Path(f'{local_dir}').rglob(f'checks.parquet.gzip')[0]
    except FileNotFoundError:
        print(f'{local_dir}checks.parquet.gzip not file checks.parquet.gzip')
        exit(1)

    df = pd.read_parquet(load_list)
    df[['products', 'payments', 'lottery_tickets']] = df[['products', 'payments', 'lottery_tickets']].apply(
        lambda x: [list(i) for i in x])

    load_list = df.to_dict('records')
    target = Mongo(params=mongodb)
    target.update_mongo(load_list)