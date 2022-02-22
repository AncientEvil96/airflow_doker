from base.my import MySQL
from sys import argv
import pandas as pd

host, port, password, login, database, file = argv[1:]


def get_load_list():
    # df = pd.read_parquet(file)
    df1 = pd.read_parquet('subdivision_vprok.parquet.gzip')
    df1.loc[df1['parent_id'] == 0, 'Parent_Sub_ID'] = 421
    # df1.loc[df1['Catalogue_ID'] == 1, 'Template_ID'] = 2
    df2 = pd.read_parquet('subdivision_compass.parquet.gzip')
    df2.loc[df2['parent_id'] == 0, 'Parent_Sub_ID'] = 52
    # df2.loc[df2['Catalogue_ID'] == 2, 'Template_ID'] = 5
    df = pd.concat([df1, df2])

    # df.loc[df['Catalogue_ID'] == 2, 'Template_ID'] = 5
    # df.loc[df['Catalogue_ID'] == 1, 'Template_ID'] = 2
    # df.loc[
    #     (df['parent_id'] == 0) & (df['Catalogue_ID'] == 2),
    #     ['Parent_Sub_ID', 'DisallowIndexing', 'IncludeInSitemap']
    # ] = (52, 1, 1)
    # df.loc[
    #     (df['parent_id'] == 0) & (df['Catalogue_ID'] == 1),
    #     ['Parent_Sub_ID', 'DisallowIndexing', 'IncludeInSitemap']
    # ] = (421, 1, 1)
    # df[['DisallowIndexing', 'IncludeInSitemap']] = df[['DisallowIndexing', 'IncludeInSitemap']].fillna(-1)

    # df.loc[df['Catalogue_ID'] == 2, 'Title'] = df['Subdivision_Name'] + ' | в сети "Циркуль"'
    # df.loc[df['Catalogue_ID'] == 1, 'Title'] = df['Subdivision_Name'] + ' | сеть магазинов "ВПРОК"'

    # df2 = df.loc[df['Catalogue_ID'] == 1]
    # df2['Title'] = df2['Subdivision_Name'] + ' | в сети "Циркуль"'
    # df3 = df.loc[df['Catalogue_ID'] == 2]
    # df3['Title'] = df3['Subdivision_Name'] + ' | сеть магазинов "ВПРОК"'
    # df = pd.concat([df2, df3])

    # df['Title'] = df['Subdivision_Name'].apply(lambda x: f'{x} | в сети "Циркуль"')
    # df['Title'] = df['Subdivision_Name'].apply(lambda x: f'{x} | сеть магазинов "ВПРОК"')

    df['Parent_Sub_ID'] = df['Parent_Sub_ID'].fillna(0)
    df['Parent_Sub_ID'] = df['Parent_Sub_ID'].astype('int')

    # print(df.dtypes)
    # print(df)

    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':
    load_list = get_load_list()

    # for i in load_list:
    #     print(i)
    #
    # exit(0)

    target = MySQL(
        params={
            'host': host,
            'port': port,
            'password': password,
            'login': login,
            'database': database,
        }
    )

    table = 'subdivision_netcat'

    target.connection_init()
    target.query_to_base(f'drop table if exists {table};')
    target.query_to_base(
        f"""
        create temporary table {table}
            (
                TBP_ID           int                                              not null,
                Subdivision_Name varchar(255) default ''                          not null,
                EnglishName      varchar(64)  default concat('category-', TBP_ID) not null,
                Parent_Sub_ID    int,
                parent_id        int,
                Catalogue_ID     int          default 0                           not null,
                Priority         int          default TBP_ID                      null,
                DisallowIndexing int          default IF(parent_id = 0,1,-1)      null,
                IncludeInSitemap int          default IF(parent_id = 0,1,-1)      null,
                menu_pic         char(255)                                        null,
                Title            varchar(255) default IF(Catalogue_ID = 1, concat(Subdivision_Name, ' | сеть магазинов "ВПРОК"'),
                                             concat(Subdivision_Name, ' | в сети "Циркуль"')) null,
                Template_ID int               default IF(Catalogue_ID = 1,2,5)    null
        );
        """
    )

    query = f"""
            INSERT INTO {table}
                (
                    TBP_ID,
                    Subdivision_Name,
                    parent_id,
                    Catalogue_ID,
                    menu_pic,
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
