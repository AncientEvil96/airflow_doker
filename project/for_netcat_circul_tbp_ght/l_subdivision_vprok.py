from base.my import MySQL
from sys import argv
import pandas as pd

host, port, password, login, database, file = argv[1:]


def get_load_list():
    df = pd.read_parquet(file)
    next_step = [9, 8, 7, 8225]
    df2 = df.loc[df['TBP_ID'].isin(next_step)]
    count = 0
    while True:
        df3 = df.loc[df['parent_id'].isin(next_step)]
        df2 = pd.concat([df2, df3])
        if count == len(df2):
            break
        count = len(df2)
        next_step = df3['TBP_ID']

    df2.loc[df2['Catalogue_ID'] == 1, ['Catalogue_ID', 'Template_ID', 'Parent_Sub_ID']] = (2, 5, 52)
    df2['Title'] = df2['Subdivision_Name'].apply(lambda x: f'{x} | в сети "Циркуль"')

    df['Title'] = df['Subdivision_Name'].apply(lambda x: f'{x} | сеть магазинов "ВПРОК"')
    df.loc[df['Catalogue_ID'] == 1, 'Parent_Sub_ID'] = 421
    df = pd.concat([df, df2])
    df['Parent_Sub_ID'] = df['Parent_Sub_ID'].astype('int')

    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':
    load_list = get_load_list()

    target = MySQL(
        params={
            'host': host,
            'port': port,
            'password': password,
            'login': login,
            'database': database,
        }
    )

    target.init_connection()
    target.query_to_base('drop table if exists aaaaa_test;')
    target.query_to_base(
        """
        create table `aaaaa_test`
            (
                TBP_ID           int                                              not null,
                Subdivision_Name varchar(255) default ''                          not null,
                EnglishName      varchar(64)  default concat('category-', TBP_ID) not null,
                Parent_Sub_ID    int,
                parent_id        int,
                Catalogue_ID     int          default 0                           not null,
                Priority         int          default TBP_ID                      null,
                DisallowIndexing int          default -1                          null,
                IncludeInSitemap int          default -1                          null,
                menu_pic         char(255)                                        null,
                Title            varchar(255)                                     null,
                Template_ID      int                                              null
            );
        """
    )

    query = """
            INSERT INTO aaaaa_test
                (TBP_ID,
                 Subdivision_Name,
                 parent_id,
                 Catalogue_ID,
                 DisallowIndexing,
                 IncludeInSitemap,
                 menu_pic,
                 Template_ID,
                 Title,
                 Parent_Sub_ID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

    target.load_many_to_base(query, load_list)

