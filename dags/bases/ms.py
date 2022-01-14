import pyodbc
import pandas as pd
import datetime
from save_to_file import SaveFile


class MsSQL:
    def __init__(self, **kwargs):
        """

        :param kwargs:

        host:
        password:
        login:
        database:

        """
        conn = kwargs.pop('params')
        self.host = conn.pop('host')
        self.__password = conn.pop('password')
        self.__login = conn.pop('login')
        self.database = conn.pop('database')

        self.conn_ms = "DRIVER={ODBC Driver 17 for SQL Server};" \
                       f"SERVER={self.host}" \
                       f";DATABASE={self.database}" \
                       f";UID={self.__login}" \
                       f";PWD={self.__password}"

    def select_db_df(self, query) -> str:
        """
        получение данных в формате DataFrame
        :return: file path
        """

        with pyodbc.connect(self.conn_ms) as cnxn:
            sf = SaveFile()
            return sf.create_file_parquet(
                df=pd.read_sql(query, cnxn),
                file_name=f'{datetime.date.today().strftime("%Y_%m_%d_%H_%M_%S")}'
            )

# async def bar(begin, end, file_name):
#
#     query = f"""
#         SELECT -- substring(sys.fn_sqlvarbasetostr([_Document16_IDRRef]),3,32) as uuid
#               [_Document16].[_Fld564] as uuid
#               ,DATEADD(year, -2000 ,[_Document16].[_Date_Time]) as created_at
#               ,CONVERT(int, [_Document16].[_Fld18]) as refund
#               ,CONVERT(int, [_Document16].[_Fld17]) as cancellation
#               ,CONVERT(int, [_Document16].[_Fld21]) as shop_id
#               ,[_Reference37].[_Description] as shop_name
#               ,[_Reference73]._Description as phone
#               ,[_Document16_VT24].[_Fld97] as barcode
#               ,CONVERT(int, [_Document16_VT24].[_Fld99]) as product_id
#               ,[_Reference77]._Description as product_name
#     --		  ,[_Reference77_l1].[_Description] as product_l1
#     --		  ,[_Reference77_l2].[_Description] as product_l2
#     --		  ,[_Reference77_l3].[_Description] as product_l3
#               ,CONVERT(int, [_Document16_VT24].[_Fld101]) as amount
#               ,CONVERT(varchar, [_Document16_VT24].[_Fld102]) as price
#             FROM [dbo].[_Document16] AS [_Document16]
#                 INNER JOIN [dbo].[_Document16_VT24] AS [_Document16_VT24]
#                     ON [_Document16]._IDRRef = [_Document16_VT24].[_Document16_IDRRef]
#                     AND [_Document16].[_Date_Time] between '{begin}' and '{end}'
#                 INNER JOIN [dbo].[_Reference37] AS [_Reference37]
#                     ON [_Document16].[_Fld19RRef] = [_Reference37].[_IDRRef]
#                 INNER JOIN [dbo].[_Reference73] AS [_Reference73]
#                     ON [_Document16].[_Fld117RRef] = [_Reference73].[_IDRRef]
#                 INNER JOIN [dbo].[_Reference77] AS [_Reference77]
#                     ON [_Document16_VT24].[_Fld99] = [_Reference77].[_Code]
#     --			INNER JOIN [dbo].[_Reference77] AS [_Reference77_l3]
#     --				ON [_Reference77_l3].[_IDRRef] = [_Reference77]._ParentIDRRef
#     --			INNER JOIN [dbo].[_Reference77] AS [_Reference77_l2]
#     --				ON [_Reference77_l2].[_IDRRef] = [_Reference77_l3]._ParentIDRRef
#     --			INNER JOIN [dbo].[_Reference77] AS [_Reference77_l1]
#     --				ON [_Reference77_l1].[_IDRRef] = [_Reference77_l2]._ParentIDRRef
#             ORDER BY [_Date_Time];
#     """
#     # print(begin, end, file_name, query)
#     create_file(query, file_name)
#
#
# if __name__ == '__main__':
#     query = f'''
#                 SELECT MIN([_Date_Time]) as min_date, MAX([_Date_Time]) as max_date
#                 FROM [dbo].[_Document16] AS [_Document16]
#                 ;
#              '''
#     ms_df.kwargs['query'] = query
#     df = ms_df.pd_df()
#     min_date = df.iloc[0]['min_date']
#     # min_date = datetime(4020, 2, 12)
#     max_date = df.iloc[0]['max_date'] + timedelta(days=30)
#     flow = 1
#
#     ioloop = asyncio.get_event_loop()
#     while min_date < max_date:
#         tasks = []
#         for i in range(0, flow):
#             year = min_date.year
#             month = min_date.month
#             day = min_date.day
#             # days = calendar.monthrange(year, month)[1]
#             # next_month_date = datetime(year, month, 1) + timedelta(days=days)
#             next_month_date = datetime(year, month, day) + timedelta(days=1)
#             # print(min_date, next_month_date)
#             try:
#                 os.mkdir(f'data_ch_{year - 2000}_{month:03}')
#             except Exception as err:
#                 pass
#             tasks.append(
#                 ioloop.create_task(
#                     bar(str(min_date), str(next_month_date),
#                         f'data_ch_{year - 2000}_{month:03}/ch_{year - 2000}_{month:03}_{day:03}')))
#             min_date = next_month_date
#         wait_tasks = asyncio.wait(tasks)
#         ioloop.run_until_complete(wait_tasks)
#         # exit(0)
#
#     ioloop.close()
