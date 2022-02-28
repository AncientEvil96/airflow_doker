from base.ms import MsSQL
from sys import argv
from datetime import datetime

begin_dt, end_dt, sours_params = argv[1:]
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


def extract_lottery_tickets(sourse):
    t_begin = get_date(begin_dt)
    t_end = get_date(end_dt)

    query = f"""
            SELECT substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid_db
                  ,[_LineNo110] as line
                  ,CONVERT(int, [_Fld111]) as number
                  ,CONVERT(int, [_Fld116]) as employee_ticket_number
            FROM [ChekKKM].[dbo].[_Document16_VT109] as [_Document16_VT109]
                INNER JOIN [ChekKKM].[dbo].[_Document16] as [_Document16]
                ON [_Document16].[_IDRRef] = [_Document16_VT109].[_Document16_IDRRef]
            WHERE [_Posted] = 1 and [_Document16].[_Date_Time] between '{str(t_begin)}' and '{str(t_end)}'
            """

    sourse.select_to_parquet(
        query,
        f'{local_dir}{t_begin.strftime("%Y%m%d%H%M%S")}/checks_lottery_tickets'
    )


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
    extract_lottery_tickets(sourse)
