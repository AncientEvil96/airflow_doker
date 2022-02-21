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