from base.ms import MsSQL
from sys import argv
from base.bases_metods import convert_dict_args
from pathlib import Path

sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/'


def _wait_file_sensor():
    supermarket_path = Path(local_dir)
    data_files = supermarket_path.glob("amount.csv")
    if len(list(data_files)) > 0:
        exit(0)
    else:
        print('no file')
        exit(1)


def extract_change_amount(sourse):
    query = f"""
            SET NOCOUNT ON
            EXECUTE dbo.get_change_with_version 'dbo.amount';
            """

    sourse.select_to_csv(
        query,
        f'{local_dir}amount'
    )


if __name__ == '__main__':
    sours_params = convert_dict_args(sours_params_s)

    sourse = MsSQL(
        params=sours_params
    )
    extract_change_amount(sourse)
    _wait_file_sensor()
