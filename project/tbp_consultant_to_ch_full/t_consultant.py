from base.bases_metods import get_datetime
import pandas as pd
from pathlib import Path
from sys import argv

begin_dt = argv[1]
local_dir = f'/tmp/tmp/{begin_dt}/'


def transform():
    files = sorted(list(map(str, list(Path(local_dir).rglob("*.parquet.gzip")))))
    for file_name in files:
        print(file_name)
        df = pd.read_parquet(file_name)
        df[['created_at', 'updated_at', 'birth_date']] = df[['created_at', 'updated_at', 'birth_date']].astype(str)
        try:
            if len(df) == 0:
                return None
            file_name_new = f'{file_name}'
            df.to_parquet(file_name_new, compression='gzip')
            print(f'created {file_name_new}')
        except Exception as err:
            print(err, ':', file_name_new)


if __name__ == '__main__':
    transform()
