from base.ms import MsSQL
from sys import argv

sours_params_s = argv[1]
local_dir = '/tmp/tmp/'

if __name__ == '__main__':
    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    source = MsSQL(
        params=sours_params
    )

    query = """
            SELECT CONVERT(tinyint, _Reference12._Fld1301)                                      as Checked
                 , _Reference12._Code                                                           as shop_id
                 , CONVERT(nvarchar, _Reference12._Fld1376)                                     as latitude
                 , CONVERT(nvarchar, _Reference12._Fld1377)                                     as longitude
                 , SUBSTRING(_Reference12._Fld335, 1, CHARINDEX(',', _Reference12._Fld335) - 1) as Adress
                 , SUBSTRING(_Reference12._Fld335, CHARINDEX(',', _Reference12._Fld335) + 2,
                             CHARINDEX(',', _Reference12._Fld335))                              as House
                 , _Reference12._Fld746                                                         as Email
                 , _Reference12._Description                                                    as Title
                 , City._Description                                                            as City
                 , 45                                                                           as Subdivision_ID
                 , 47                                                                           as Sub_Class_ID
            FROM _Reference12
                     INNER JOIN _Reference12 as Parent
                                ON _Reference12._ParentIDRRef = Parent._IDRRef
                                    AND Parent._Description = N'Впрок'
                     INNER JOIN _Reference288 as City
                                ON _Reference12._Fld119RRef = City._IDRRef
            WHERE _Reference12._Folder = 1
            UNION
            SELECT CONVERT(tinyint, _Reference12._Fld1301)                                      as Checked
                 , _Reference12._Code                                                           as shop_id
                 , CONVERT(nvarchar, _Reference12._Fld1376)                                     as latitude
                 , CONVERT(nvarchar, _Reference12._Fld1377)                                     as longitude
                 , SUBSTRING(_Reference12._Fld335, 1, CHARINDEX(',', _Reference12._Fld335) - 1) as Adress
                 , SUBSTRING(_Reference12._Fld335, CHARINDEX(',', _Reference12._Fld335) + 2,
                             CHARINDEX(',', _Reference12._Fld335))                              as House
                 , _Reference12._Fld746                                                         as Email
                 , _Reference12._Description                                                    as Title
                 , City._Description                                                            as City
                 , 45                                                                           as Subdivision_ID
                 , 47                                                                           as Sub_Class_ID
            FROM _Reference12
                     INNER JOIN _Reference12 as Parent
                                ON _Reference12._ParentIDRRef = Parent._IDRRef
                                    AND Parent._Description = N'Циркуль'
                     INNER JOIN _Reference288 as City
                                ON _Reference12._Fld119RRef = City._IDRRef
            WHERE _Reference12._Folder = 1;
        """

    source.select_to_parquet(query, f'{local_dir}stock')
