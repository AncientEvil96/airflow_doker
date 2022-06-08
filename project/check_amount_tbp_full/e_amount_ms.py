from base.ms import MsSQL
from sys import argv
from base.bases_metods import convert_dict_args
from pathlib import Path

sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/'


def file_sensor():
    supermarket_path = Path(local_dir)
    data_files = supermarket_path.glob("amount_ms.parquet.gzip")
    if len(list(data_files)) > 0:
        exit(0)
    else:
        print('no file')
        exit(1)


def extract_change_amount(sourse):
    query = f"""
            SELECT [stock_name]                                                        as stock_name
                 , CONVERT(integer, SUBSTRING([product_code], 4, LEN([product_code]))) as product_id
                 , CONVERT(integer, product_amount)                                    as product_amount
            FROM [CHECK_CHANGE].[dbo].[amount];
            """

    sourse.select_to_parquet(
        query,
        # f'{local_dir}amount_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        f'{local_dir}amount_ms'
    )


if __name__ == '__main__':
    sours_params = convert_dict_args(sours_params_s)

    sourse = MsSQL(
        params=sours_params
    )
    extract_change_amount(sourse)
    file_sensor()
