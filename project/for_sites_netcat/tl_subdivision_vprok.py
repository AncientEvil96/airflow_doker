from base.my import MySQL
from sys import argv
import pandas as pd
from pathlib import Path
import re

sours_params_s = argv[1]
local_dir = '/tmp/tmp/'


def get_df(key_):
    files = list(map(str, list(Path(f'{local_dir}').rglob(f'{key_}.parquet.gzip'))))
    file = [x for x in files if re.match(f'.*{key_}*', x)][0] if any(
        f'{key_}' in word for word in files) else ''

    if file == '':
        print('not file')
        exit(1)

    return pd.read_parquet(file)


def get_load_list():
    df1 = get_df('subdivision_vprok')
    df1.loc[df1['parent_id'] == 0, 'Parent_Sub_ID'] = 421
    df2 = get_df('subdivision_compass')
    df2.loc[df2['parent_id'] == 0, 'Parent_Sub_ID'] = 52
    df = pd.concat([df1, df2])
    df['Parent_Sub_ID'] = df['Parent_Sub_ID'].fillna(0)
    df['Parent_Sub_ID'] = df['Parent_Sub_ID'].astype('int')

    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':

    load_list = get_load_list()

    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    target = MySQL(
        params=sours_params
    )

    table = 'tmp_subdivision_netcat'

    target.connection_init()
    target.query_to_base(f'drop table if exists {table};')
    target.query_to_base(
        f"""
        create temporary table {table}
            (
                TBP_ID           int                                              not null,
                Subdivision_Name varchar(255) default ''                          not null,
                EnglishName      varchar(64)  default concat('category-', TBP_ID) not null,
                Parent_Sub_ID    int          default 0,
                parent_id        int,
                Catalogue_ID     int          default 0                           not null,
                Priority         int          default TBP_ID                      null,
                DisallowIndexing int          default IF(parent_id = 0,1,-1)      null,
                IncludeInSitemap int          default IF(parent_id = 0,1,-1)      null,
                menu_pic         char(255)                                        null,
                Title            varchar(255) default IF(Catalogue_ID = 1, concat(Subdivision_Name, ' | сеть магазинов "ВПРОК"'),
                                             concat(Subdivision_Name, ' | в сети "Циркуль"')) null,
                Template_ID int               default IF(Catalogue_ID = 1,2,5)    null,
                KEY {table}_TBP_ID_Catalogue_ID_ui (TBP_ID, Catalogue_ID)
        );
        """
    )

    query = f"""
            INSERT INTO {table}
                (
                    TBP_ID           ,
                    Subdivision_Name ,
                    parent_id        ,
                    Catalogue_ID     ,
                    menu_pic         ,
                    Parent_Sub_ID
                 )
            VALUES (%s, %s, %s, %s, %s, %s);
        """

    target.load_many_to_base(query, load_list)

    # удаление ненужных данных
    target.query_to_base(
        f"""
        delete
        from Subdivision
        where Subdivision.Template_ID in (2,5) and Subdivision.TBP_ID > 0
          and not Subdivision_ID in (select Subdivision_ID
                                     from (select MAX(Subdivision_ID) as Subdivision_ID
                                           FROM Subdivision
                                                    left join {table} as tt1 on tt1.TBP_ID = Subdivision.TBP_ID
                                           where tt1.TBP_ID in
                                                 (select distinct Subdivision.TBP_ID from Subdivision)
                                           group by Subdivision.TBP_ID, Subdivision.Catalogue_ID) as new);
        """
    )

    # создаем новые если такие есть
    target.query_to_base(
        f"""
            insert into Subdivision (Subdivision_Name,
                                     EnglishName,
                                     Parent_Sub_ID,
                                     Catalogue_ID,
                                     Priority,
                                     menu_pic,
                                     Title,
                                     TBP_ID,
                                     Template_ID,
                                     DisallowIndexing,
                                     IncludeInSitemap,
                                     Hidden_URL)
            select tt1.Subdivision_Name                                                          as Subdivision_Name,
                   tt1.EnglishName                                                               as EnglishName,
                   IFNULL(Parent.Subdivision_ID, IF(tt1.parent_id = 0, tt1.Parent_Sub_ID, null)) as Parent_Sub_ID,
                   tt1.Catalogue_ID                                                              as Catalogue_ID,
                   tt1.TBP_ID                                                                    as Priority,
                   tt1.menu_pic                                                                  as menu_pic,
                   tt1.Title                                                                     as Title,
                   tt1.TBP_ID                                                                    as TBP_ID,
                   tt1.Template_ID                                                               as Template_ID,
                   tt1.DisallowIndexing                                                          as DisallowIndexing,
                   tt1.IncludeInSitemap                                                          as IncludeInSitemap,
                   CONCAT(IFNULL(Parent.Hidden_URL, ''), tt1.EnglishName, '/')                   as Hidden_URL
            from {table} as tt1
                     LEFT JOIN Subdivision
                               on tt1.TBP_ID = Subdivision.TBP_ID
                                   AND tt1.Catalogue_ID = Subdivision.Catalogue_ID
                                   and tt1.Catalogue_ID in (1, 2)
                     LEFT JOIN Subdivision as Parent
                               on tt1.parent_id = Parent.TBP_ID
                                   AND tt1.Catalogue_ID = Subdivision.Catalogue_ID
                                   and tt1.Catalogue_ID in (1, 2)
            where Subdivision.Subdivision_ID is null
            group by tt1.Subdivision_Name,
                     tt1.EnglishName,
                     IFNULL(Parent.Subdivision_ID, IF(tt1.parent_id = 0, tt1.Parent_Sub_ID, null)),
                     tt1.Catalogue_ID,
                     tt1.TBP_ID,
                     tt1.menu_pic,
                     tt1.Title,
                     tt1.TBP_ID,
                     tt1.Template_ID,
                     tt1.DisallowIndexing,
                     tt1.IncludeInSitemap,
                     CONCAT(IFNULL(Parent.Hidden_URL, ''), tt1.EnglishName, '/')
            order by TBP_ID
            ;
            ;
        """
    )

    from_load = 'tmp_subdivision'

    # удаляем временную таблицу если она есть
    target.query_to_base(f'drop table if exists {from_load};')

    # проверка + обновление данных
    target.query_to_base(
        f"""
            CREATE temporary table {from_load} as
            SELECT Subdivision.Subdivision_ID as Subdivision_ID,
                   new.Subdivision_Name       as Subdivision_Name,
                   new.EnglishName            as EnglishName,
                   new.Parent_Sub_ID         as Parent_Sub_ID,
                   new.Catalogue_ID           as Catalogue_ID,
                   new.TBP_ID                 as Priority,
                   new.menu_pic               as menu_pic,
                   new.Title                  as Title,
                   new.TBP_ID                 as TBP_ID,
                   new.Template_ID            as Template_ID,
                   new.DisallowIndexing       as DisallowIndexing,
                   new.IncludeInSitemap       as IncludeInSitemap,
                   new.Hidden_URL             as Hidden_URL
            
            FROM (
                     SELECT Subdivision.Subdivision_ID                             as Subdivision_ID,
                            tt1.Subdivision_Name                            as Subdivision_Name,
                            tt1.EnglishName                                 as EnglishName,
                            Parent.Subdivision_ID                                  as Parent_Sub_ID,
                            tt1.Catalogue_ID                                as Catalogue_ID,
                            tt1.TBP_ID                                      as Priority,
                            tt1.menu_pic                                    as menu_pic,
                            tt1.Title                                       as Title,
                            tt1.TBP_ID                                      as TBP_ID,
                            tt1.Template_ID                                 as Template_ID,
                            tt1.DisallowIndexing                            as DisallowIndexing,
                            tt1.IncludeInSitemap                            as IncludeInSitemap,
                            CONCAT(Parent.Hidden_URL, tt1.EnglishName, '/') as Hidden_URL
                     FROM {table} as tt1
                              INNER JOIN Subdivision
                                         ON tt1.TBP_ID = Subdivision.TBP_ID
                                             AND tt1.Catalogue_ID = Subdivision.Catalogue_ID
                                             and tt1.Catalogue_ID in (1, 2)
                                             AND Subdivision.TBP_ID > 0
                              INNER JOIN Subdivision as Parent
                                         on tt1.parent_id = Parent.TBP_ID
                                             AND tt1.Catalogue_ID = Parent.Catalogue_ID
                                             and tt1.Catalogue_ID in (1, 2)
                                             AND Parent.TBP_ID > 0
                 ) as new
                     LEFT JOIN Subdivision
                               ON Subdivision.Subdivision_ID = new.Subdivision_ID
                                   AND Subdivision.Catalogue_ID = new.Catalogue_ID
                                   AND Subdivision.Catalogue_ID IN (1, 2)
                                   AND Subdivision.TBP_ID > 0
            WHERE NOT (
                        new.Parent_Sub_ID = Subdivision.Parent_Sub_ID AND
                        new.Subdivision_Name = Subdivision.Subdivision_Name AND
                        new.Hidden_URL = Subdivision.Hidden_URL AND
                        new.Title = Subdivision.Title AND
                        new.menu_pic = Subdivision.menu_pic
                )
            ;
        """
    )

    target.query_to_base(
        f"""
        update Subdivision
            INNER JOIN {from_load} as tt1 USING (Subdivision_ID)
        set Subdivision.Subdivision_Name = tt1.Subdivision_Name,
            Subdivision.EnglishName      = tt1.EnglishName,
            Subdivision.Parent_Sub_ID    = tt1.Parent_Sub_ID,
            Subdivision.Catalogue_ID     = tt1.Catalogue_ID,
            Subdivision.Priority         = tt1.Priority,
            Subdivision.menu_pic         = tt1.menu_pic,
            Subdivision.Title            = tt1.Title,
            Subdivision.TBP_ID           = tt1.TBP_ID,
            Subdivision.Template_ID      = tt1.Template_ID,
            Subdivision.DisallowIndexing = tt1.DisallowIndexing,
            Subdivision.IncludeInSitemap = tt1.IncludeInSitemap,
            Subdivision.Hidden_URL       = tt1.Hidden_URL
        where TRUE;
        """
    )

    target.connection_close()
