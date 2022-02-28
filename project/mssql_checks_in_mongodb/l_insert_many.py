from base.ms import MsSQL
from sys import argv
import pandas as pd
from datetime import datetime
from pathlib import Path

begin_dt, sours_params = argv[1:]
local_dir = '/tmp/tmp/'


def get_date(date_):
    executor_date = datetime.strptime(str(date_), '%Y-%m-%d %H:%M:%S')
    return datetime(
        executor_date.year + 2000,
        executor_date.month,
        executor_date.day,
        executor_date.hour,
        executor_date.minute,
        executor_date.second
    )


def load_insert_many():
    t_begin = get_date(begin_dt)

    load_list = ''
    try:
        load_list = Path(f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}').rglob(f'checks.parquet.gzip')[0]
    except FileNotFoundError:
        print(f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks.parquet.gzip not file checks.parquet.gzip')
        exit(1)

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


if __name__ == '__main__':
    # sourse = MsSQL(
    #     params={
    #         'host': ms_connect.host,
    #         'password': ms_connect.password,
    #         'login': ms_connect.login,
    #         'database': ms_connect.schema,
    #     }
    # )
    sourse = MsSQL(
        params=sours_params
    )
    load_insert_many(sourse)

if __name__ == '__main__':
