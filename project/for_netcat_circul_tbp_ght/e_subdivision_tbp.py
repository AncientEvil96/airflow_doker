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
    SELECT CONVERT(bigint, [_Fld395])                                     AS TBP_ID
         , [_Reference505]._Description                                    AS Subdivision_Name
         , CONVERT(bigint, [_Fld396])                                      AS parent_id
         , 1                                                               AS Catalogue_ID
    --      , 'category-' + CONVERT(nvarchar, [_Fld395])                      AS EnglishName
    --      , 0                                                               AS Checked
    --      , CONVERT(bigint, [_Fld395])                                      AS Priority
         , -1                                                              AS DisallowIndexing
         , -1                                                              AS IncludeInSitemap
    --      , 1                                                               AS UseMultiSubClass
         , [_InfoRg393].[_Fld720]                                          AS menu_pic
    --      , '' + [_Reference505]._Description + N'| сеть магазинов "ВПРОК"' AS Title
         , 2                                                               AS Template_ID
    --      , CONVERT(tinyint, [_Fld1003])                                    AS hide_tbp
    FROM [TBP_WORK].[dbo].[_InfoRg393]
             INNER JOIN [TBP_WORK].[dbo]._Reference505 as _Reference505
                        ON _Fld394RRef = _Reference505._IDRRef
    WHERE [_Fld508] = 1 AND [_Fld1003] = 0;
    """

    source.select_to_parquet(query, '/tmp/tmp/subdivision')
