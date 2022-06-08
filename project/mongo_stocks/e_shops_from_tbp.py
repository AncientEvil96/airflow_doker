from base.ms import MsSQL
from sys import argv
from base.bases_metods import convert_dict_args

begin_dt, sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/{begin_dt}/'


def extract_shops(sourse):
    query = f"""
    SELECT CONVERT(tinyint, Stock.[_Marked])      as ismark
         , Parent._Description                    as parent
         , CONVERT(integer, Stock.[_Code])        as id
         , Stock.[_Description]                   as name
         , CONVERT(integer, Stock.[_Fld1169])     as id_bitrix
         , Stock.[_Fld820]                        as uuid_ut
         , Stock.[_Fld1200]                       as unloading_address
         , Stock.[_Fld507]                        as address_bycomplex
         , CONVERT(bigint, Stock.[_Fld783])       as code_shop
         , CONVERT(integer, Stock.[_Fld998])      as delivery_date_adjustment
         , CONVERT(integer, Stock.[_Fld1018])     as move_date_adjustment
         , Stock.[_Fld1189]                       as name_for_unloading
         , Stock.[_Fld619]                        as name_KKM
         , Organizations._Code                    as org_id
         , Organizations._Description             as org_name
         , Stock.[_Fld1201]                       as prefix_addresses
         , Stock.[_Fld1204]                       as prefix_city
         , City.[_Description]                     as city
         , Stock.[_Fld1202]                       as area
         , CONVERT(tinyint, TypeStock._EnumOrder) as warehouse_type_id
         , CASE
               WHEN TypeStock._EnumOrder = 0 THEN 'Оптовый склад'
               WHEN TypeStock._EnumOrder = 1 THEN 'Розничный магазин'
               WHEN TypeStock._EnumOrder = 2 THEN 'Постамат'
        END                                       as warehouse_type_discription
         , Stock.[_Fld1203]                       as shopping_center
         , Stock.[_Fld746]                        as email
         , CONVERT(tinyint, Stock.[_Fld1301])     as store_activity
         , Stock.[_Fld1373]                       as region
         , Stock.[_Fld1374]                       as zip_code
         , Stock.[_Fld1375]                       as subway_station
         , Stock.[_Fld1376]                       as latitude
         , Stock.[_Fld1377]                       as longitude
         , Stock.[_Fld1378]                       as id_partner
         , Stock.[_Fld1379]                       as activation_points
         , Stock.[_Fld1380]                       as time_zone
         , Stock.[_Fld1381]                       as difference_belts
         , Stock.[_Fld1382]                       as phone
         , Stock.[_Fld1383]                       as max_weight
         , Stock.[_Fld1384]                       as max_length
         , Stock.[_Fld1385]                       as max_width
         , Stock.[_Fld1386]                       as max_height
         , Stock.[_Fld1387]                       as method_of_obtaining
         , Stock.[_Fld1400]                       as region_code
         , Stock.[_Fld1401]                       as federal_district
         , Stock.[_Fld1402]                       as index_
         , Stock.[_Fld1403]                       as house
         , Stock.[_Fld1423]                       as base_source
         , CONVERT(tinyint, Stock.[_Fld1534])     as sber_market
    FROM dbo._Reference12 as Stock
             INNER JOIN dbo._Reference12 as Parent
                        ON Stock._ParentIDRRef = Parent._IDRRef
                            AND Parent.[_Folder] = 0
             INNER JOIN dbo._Reference73 as Organizations
                        ON Organizations._IDRRef = Stock._Fld125RRef
             INNER JOIN dbo._Enum74 as TypeStock
                        ON Stock._Fld77RRef = TypeStock._IDRRef
             LEFT JOIN dbo._Reference288 as City
                       ON City._IDRRef = Stock._Fld119RRef
    WHERE Stock._Folder = 1
      AND Stock._Description <> 'Для удаления'
    ;
    """

    sourse.select_to_parquet(query, f'{local_dir}df_shops_from_tbp')


if __name__ == '__main__':
    sours_params = convert_dict_args(sours_params_s)

    sourse = MsSQL(
        params=sours_params
    )
    extract_shops(sourse)
