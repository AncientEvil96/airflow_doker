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
        SELECT CONVERT(bigint, Parent._Code)                                                        as parent_id
             , CONVERT(bigint, _Reference11.[_Code])                                                AS Article
             --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])   as VariantName
             , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])   as Name
             , _Reference11.[_Fld375]                                                               as Description
             , Vendor._Description                                                                  as VendorTbp
             , Breand._Description                                                                  as Vendor
             --, 0                                                                                    as User_ID
             --, 0                                                                                    as LastUser_ID
             --, 1                                                                                    as Checked
             , MAX(_Reference336._Fld340)                                                           as Weight
             , MAX(_Reference336._Fld341)                                                           as PackageSize2
             , MAX(_Reference336._Fld342)                                                           as PackageSize3
             , MAX(_Reference336._Fld343)                                                           as PackageSize1
             --, 'goods-' + CONVERT(nvarchar, _Reference11.[_Code])                                   as Keyword
             , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])   as ncTitle
             --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
             --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".' as ncDescription
             --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])   as ncSMO_Title
             --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
             --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".' as ncSMO_Description
             , MAX(_Image._Fld1084)                                                                 as Image
             , Price._Fld482                                                                        as Price
             , Price._Fld312                                                                        as PriceMinimum
             , CONVERT(integer, _Reference11.[_Fld1165])                                            as Status
             , CONVERT(integer, IIF(_Reference11.[_Fld1165] = 1, 1, 0))                             as StockUnits
             --, 1                                                                                    as Currency
             --, 1                                                                                    as CurrencyMinimum
        FROM [TBP_WORK].[dbo].[_Reference11] as _Reference11
                 INNER JOIN [TBP_WORK].[dbo].[_InfoRg308] as Price
                            ON Price._Fld309RRef = _Reference11._IDRRef
                 LEFT JOIN [TBP_WORK].[dbo].[_Reference11] AS Parent
                           ON Parent._IDRRef = _Reference11.[_ParentIDRRef]
                 LEFT JOIN [TBP_WORK].[dbo].[_Reference336] as _Reference336
                           ON _Reference336._OwnerIDRRef = _Reference11._IDRRef
                 LEFT JOIN [TBP_WORK].[dbo].[_InfoRg1083] as _Image
                           ON _Image._Fld1085 = _Reference11._Code
                               AND _Image._Fld1086 = 1
                 LEFT JOIN [TBP_WORK].[dbo]._Reference296 as Vendor
                           ON _Reference11._Fld92RRef = Vendor._IDRRef
                 LEFT JOIN [TBP_WORK].[dbo]._Reference295 as Breand
                           ON _Reference11._Fld91RRef = Breand._IDRRef
        WHERE _Reference11.[_Fld1165] IN (1, 2, 3, 4)
        GROUP BY CONVERT(bigint, Parent._Code)
               , CONVERT(bigint, _Reference11.[_Code])
               --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               , _Reference11.[_Fld375]
               , Vendor._Description
               , Breand._Description
               --, 'goods-' + CONVERT(nvarchar, _Reference11.[_Code])
               , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
               --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".'
               --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
               --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".'
               , Price._Fld482
               , Price._Fld312
               , CONVERT(integer, _Reference11.[_Fld1165])               
               , CONVERT(integer, IIF(_Reference11.[_Fld1165] = 1, 1, 0))
        ;
        """

    local_dir = '/tmp/tmp/'
    local_dir = ''

    source.select_to_parquet(query, f'product')
