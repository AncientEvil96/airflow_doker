from base.ms import MsSQL
from sys import argv

sours_params_s = argv[1]
# sours_params_s = "[('host','192.168.0.177'),('password','Delay159'),('login','sa'),('database','TBP_WORK')]"
local_dir = '/tmp/tmp/'
# local_dir = ''

if __name__ == '__main__':
    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    source = MsSQL(
        params=sours_params
    )

    query = """
        SELECT ISNULL(CONVERT(int, Parent.id_parent), 0)                                          as parent_id
             , CONVERT(bigint, _Reference11.[_Code])                                              AS Article
             , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) as Name
             , _Reference11.[_Fld375]                                                             as Description
             , Vendor._Description                                                                as VendorTbp
             , Breand._Description                                                                as Vendor
             , MAX(_Reference336._Fld340)                                                         as Weight
             , MAX(_Reference336._Fld341)                                                         as PackageSize2
             , MAX(_Reference336._Fld342)                                                         as PackageSize3
             , MAX(_Reference336._Fld343)                                                         as PackageSize1
             , MAX(_Image._Fld1084)                                                               as Image
             , Price._Fld482                                                                      as Price
             , Price._Fld312                                                                      as PriceMinimum
             , CONVERT(integer, _Reference11.[_Fld1165])                                          as Status
             , CONVERT(integer, IIF(_Reference11.[_Fld1165] = 1, 1, 0))                           as StockUnits
        FROM [TBP_WORK].[dbo].[_Reference11] as _Reference11
                 INNER JOIN (SELECT _Reference11.[_IDRRef] as _IDRRef
                                  , Price._Fld482          as _Fld482
                                  , Price._Fld312          as _Fld312
                             FROM (SELECT max(_Period) as Period, _Fld309RRef as Fld309
                                   FROM [TBP_WORK].[dbo].[_InfoRg308] as Price
                                            INNER JOIN [TBP_WORK].[dbo].[_Reference11] as _Reference11
                                                       ON Price._Fld309RRef = _Reference11._IDRRef
                                   GROUP BY _Fld309RRef) as date_price
                                      INNER JOIN [TBP_WORK].[dbo].[_InfoRg308] as Price
                                                 ON date_price.Period = Price._Period
                                                     AND date_price.Fld309 = Price._Fld309RRef
                                      INNER JOIN [TBP_WORK].[dbo].[_Reference11] as _Reference11
                                                 ON Price._Fld309RRef = _Reference11._IDRRef
                                                     AND _Reference11.[_Fld1165] IN (1, 2, 3, 4)
                             GROUP BY _Reference11.[_IDRRef]
                                    , Price._Fld482
                                    , Price._Fld312) as Price
                            ON Price._IDRRef = _Reference11._IDRRef
                 LEFT JOIN (SELECT _InfoRg393._Fld395     as id_parent,
                                   _InfoRg406._Fld407RRef AS id_prod
                            FROM [TBP_WORK].[dbo]._InfoRg393 AS _InfoRg393
                                     INNER JOIN [TBP_WORK].[dbo]._InfoRg406 as _InfoRg406
                                                ON _InfoRg393._Fld394RRef = _InfoRg406._Fld731RRef
                            WHERE _Fld1000 = 2
                              AND _Fld508 = 1
                            GROUP BY _InfoRg393._Fld395,
                                     _InfoRg406._Fld407RRef) AS Parent
                           ON Parent.id_prod = _Reference11._IDRRef
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
        GROUP BY ISNULL(CONVERT(int, Parent.id_parent), 0)
               , CONVERT(bigint, _Reference11.[_Code])
               , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               , _Reference11.[_Fld375]
               , Vendor._Description
               , Breand._Description
               , IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317])
               , Price._Fld482
               , Price._Fld312
               , CONVERT(integer, _Reference11.[_Fld1165])
               , CONVERT(integer, IIF(_Reference11.[_Fld1165] = 1, 1, 0))
        ;
        """

    source.select_to_parquet(query, f'{local_dir}product')
