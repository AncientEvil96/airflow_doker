from base.ms import MsSQL
from sys import argv
from datetime import datetime

begin_dt, end_dt, sours_params_s = argv[1:]
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


def extract_payments(sourse):
    t_begin = get_date(begin_dt)
    t_end = get_date(end_dt)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid_db
                ,[_LineNo89] as line
                ,[_Fld92] as [type]
                ,CONVERT(decimal(10,2), [_Fld93]) as summ
                ,CONVERT(decimal(10,2), [_Fld94]) as discount
                ,[_Fld95] as gift_sertificate
                ,[_Fld96] as bankcard
            FROM [_Document16_VT88] as [_Document16_VT88]
                INNER JOIN [_Document16] as [_Document16]
                ON [_Document16].[_IDRRef] = [_Document16_VT88].[_Document16_IDRRef]
            WHERE [_Posted] = 1 and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """

    sourse.select_to_parquet(
        query,
        f'{local_dir}checks_payments'
    )


if __name__ == '__main__':
    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    sourse = MsSQL(
        params=sours_params
    )
    extract_payments(sourse)
