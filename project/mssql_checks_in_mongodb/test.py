from datetime import datetime
import re
from pathlib import Path

begin_dt = '2022-03-01 20:00:00'
end_dt = '2022-03-01 20:00:10'
executor_date = datetime.strptime(str(begin_dt), '%Y-%m-%d %H:%M:%S')
t_begin = datetime(
    executor_date.year + 2000,
    executor_date.month,
    executor_date.day,
    executor_date.hour,
    executor_date.minute,
    executor_date.second
)
executor_date = datetime.strptime(str(end_dt), '%Y-%m-%d %H:%M:%S')
t_end = datetime(
    executor_date.year + 2000,
    executor_date.month,
    executor_date.day,
    executor_date.hour,
    executor_date.minute,
    executor_date.second
)
# print(t_begin, t_end)
# print(f'checks_headers_{t_begin.strftime("%Y%m%d%H%M%S")}')

local_dir = '/home/deus/PycharmProjects/airflow_doker/project/mssql_checks_in_mongodb'

# customers = [f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks_headers.parquet.gzip',
#              f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks_lottery_tickets.parquet.gzip',
#              f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks_payments.parquet.gzip',
#              f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks_products.parquet.gzip']
#
# any("checks_lottery_tickets" in word for word in customers)
#
#
# print(checks_headers, checks_products, checks_payments, checks_lottery_tickets)


files = list(Path(f'{local_dir}').rglob(f'test.py'))[0]

print(files)
