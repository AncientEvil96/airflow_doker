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

    query_compass = """
        WITH RecursiveQuery (_IDRRef, _ParentIDRRef, TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic)
                 AS
                 (
                     select _Reference505._IDRRef        AS _IDRRef
                          , _Reference505._ParentIDRRef  AS _ParentIDRRef
                          , CONVERT(bigint, [_Fld395])   AS TBP_ID
                          , [_Reference505]._Description AS Subdivision_Name
                          , CONVERT(bigint, [_Fld396])   AS parent_id
                          , 2                            AS Catalogue_ID
                          , [_InfoRg393].[_Fld720]       AS menu_pic
                     from [TBP_WORK].[dbo]._Reference1145
                              INNER JOIN (select _IDRRef, _Description
                                          from [TBP_WORK].[dbo]._Reference1145
                                          WHERE _Description in (N'ЦИРКУЛЬ')) as PVC
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
                     GROUP BY _Reference505._IDRRef
                            , _Reference505._ParentIDRRef
                            , CONVERT(bigint, [_Fld395])
                            , [_Reference505]._Description
                            , CONVERT(bigint, [_Fld396])
                            , [_InfoRg393].[_Fld720]
                     UNION ALL
                     select _Reference505._IDRRef        AS _IDRRef
                          , _Reference505._ParentIDRRef  AS _ParentIDRRef
                          , CONVERT(bigint, [_Fld395])   AS TBP_ID
                          , [_Reference505]._Description AS Subdivision_Name
                          , CONVERT(bigint, [_Fld396])   AS parent_id
                          , 2                            AS Catalogue_ID
                          , [_InfoRg393].[_Fld720]       AS menu_pic
                     from [TBP_WORK].[dbo]._Reference505 as _Reference505
                              INNER JOIN [TBP_WORK].[dbo].[_InfoRg393] ON _Fld394RRef = _Reference505._IDRRef
                              INNER JOIN RecursiveQuery rec ON _Reference505._IDRRef = rec._ParentIDRRef
                 )
        SELECT TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic
        FROM RecursiveQuery
        group by TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic;
        """

    query_vprok = """
        WITH RecursiveQuery (_IDRRef, _ParentIDRRef, TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic)
                 AS
                 (
                     select _Reference505._IDRRef        AS _IDRRef
                          , _Reference505._ParentIDRRef  AS _ParentIDRRef
                          , CONVERT(bigint, [_Fld395])   AS TBP_ID
                          , [_Reference505]._Description AS Subdivision_Name
                          , CONVERT(bigint, [_Fld396])   AS parent_id
                          , 1                            AS Catalogue_ID
                          , [_InfoRg393].[_Fld720]       AS menu_pic
                     from [TBP_WORK].[dbo]._Reference1145
                              INNER JOIN (select _IDRRef, _Description
                                          from [TBP_WORK].[dbo]._Reference1145
                                          WHERE _Description in (N'ВПРОК')) as PVC
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
                     GROUP BY _Reference505._IDRRef
                            , _Reference505._ParentIDRRef
                            , CONVERT(bigint, [_Fld395])
                            , [_Reference505]._Description
                            , CONVERT(bigint, [_Fld396])
                            , [_InfoRg393].[_Fld720]
                     UNION ALL
                     select _Reference505._IDRRef        AS _IDRRef
                          , _Reference505._ParentIDRRef  AS _ParentIDRRef
                          , CONVERT(bigint, [_Fld395])   AS TBP_ID
                          , [_Reference505]._Description AS Subdivision_Name
                          , CONVERT(bigint, [_Fld396])   AS parent_id
                          , 1                            AS Catalogue_ID
                          , [_InfoRg393].[_Fld720]       AS menu_pic
                     from [TBP_WORK].[dbo]._Reference505 as _Reference505
                              INNER JOIN [TBP_WORK].[dbo].[_InfoRg393] ON _Fld394RRef = _Reference505._IDRRef
                              INNER JOIN RecursiveQuery rec ON _Reference505._IDRRef = rec._ParentIDRRef
                 )
        SELECT TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic
        FROM RecursiveQuery
        group by TBP_ID, Subdivision_Name, parent_id, Catalogue_ID, menu_pic;
        """

    local_dir = '/tmp/tmp/'
    # local_dir = ''

    source.select_to_parquet(query_compass, f'{local_dir}subdivision_compass')
    source.select_to_parquet(query_vprok, f'{local_dir}subdivision_vprok')