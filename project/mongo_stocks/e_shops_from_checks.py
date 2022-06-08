from base.ms import MsSQL
from sys import argv
from datetime import timedelta
from base.bases_metods import convert_dict_args, get_date

begin_dt, end_dt, sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/{begin_dt}/'


def extract_shops(sourse):
    query = f"""
            SELECT CONVERT(int, [_Fld21]) as id
            FROM [ChekKKM].[dbo].[_Document16]
            WHERE [_Posted] = 1
              and [_Document16].[_Date_Time]
                between DATEADD(year, 2000, DATEADD(day, -30, CURRENT_TIMESTAMP))
                and DATEADD(year, 2000, CURRENT_TIMESTAMP)
            GROUP BY CONVERT(int, [_Fld21])
            """

    sourse.select_to_parquet(query, f'{local_dir}df_shops_from_checks')


if __name__ == '__main__':
    t_begin = get_date(begin_dt, for_ms=True)
    t_end = get_date(end_dt, for_ms=True)
    sours_params = convert_dict_args(sours_params_s)

    sourse = MsSQL(
        params=sours_params
    )
    extract_shops(sourse)
