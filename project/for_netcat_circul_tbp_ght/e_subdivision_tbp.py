from base.ms import MsSQL
from sys import argv

host, password, login, database = argv[1:]

if __name__ == '__main__':
    source = MsSQL(
        params={
            'host': host,
            'password': password,
            'login': login,
            'database': database,
        }
    )

    query = """
    select CONVERT(bigint, [_Fld395])               AS TBP_ID
         , [_Reference505]._Description             AS Subdivision_Name
         , CONVERT(bigint, [_Fld396])               AS parent_id
         , IIF(PVC._Description = N'ЦИРКУЛЬ', 2, 1) AS Catalogue_ID
         , [_InfoRg393].[_Fld720]                   AS menu_pic
    from [TBP_WORK].[dbo]._Reference1145
             INNER JOIN (select _IDRRef, _Description
                         from [TBP_WORK].[dbo]._Reference1145
                         WHERE _Description in (N'ЦИРКУЛЬ', N'ВПРОК')) as PVC
                        on _Reference1145._ParentIDRRef = PVC._IDRRef
             INNER JOIN [TBP_WORK].[dbo]._InfoRg1149
                        on _Fld1162 = _Fld1152
             INNER JOIN [TBP_WORK].[dbo]._Reference11
                        ON _Reference11._Code = _InfoRg1149._Fld1150
                            AND _Reference11._Marked = 0
                            AND _Reference11.[_Fld1165] IN (1, 2, 3, 4)
             INNER JOIN [TBP_WORK].[dbo]._InfoRg406
                        ON _Fld407RRef = _Reference11._IDRRef
             INNER JOIN [TBP_WORK].[dbo]._Reference505 as _Reference505
                        ON _Fld731RRef = _Reference505._IDRRef
             INNER JOIN [TBP_WORK].[dbo].[_InfoRg393] ON _Fld394RRef = _Reference505._IDRRef
    WHERE _Reference1145._Marked = 0
    GROUP BY
           CONVERT(bigint, [_Fld395])
         , [_Reference505]._Description
         , CONVERT(bigint, [_Fld396])
         , IIF(PVC._Description = N'ЦИРКУЛЬ', 2, 1)
         , [_InfoRg393].[_Fld720]
    ;
    """

    source.select_to_parquet(query, '/tmp/tmp/subdivision')
