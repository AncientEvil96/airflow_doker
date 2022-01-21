import pandas as pd
from dags.bases.operations_to_files import File

data1 = [
    {
        'id': 1,
    },
    {
        'id': 2,
    },
    {
        'id': 3,
    },
    {
        'id': 4,
    },
]

data2 = [
    {
        'id': 1,
        'name': 'a',
        'price': 1
    },
    {
        'id': 1,
        'name': 's',
        'price': 2
    },
    {
        'id': 2,
        'name': 'd',
        'price': 3
    },
    {
        'id': 2,
        'name': 'f',
        'price': 3
    },
    {
        'id': 2,
        'name': 'g',
        'price': 4
    },
    {
        'id': 3,
        'name': 'h',
        'price': 5
    }
]

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

# print(df1)
#
# def ss(x):
#
df = df2.groupby(['id']).agg(lambda x: [(x.name, i) for i in list(x)])
df = df1.merge(df, left_on='id', right_on='id')
df['new'] = df[df2.columns.drop('id').tolist()].apply(
    lambda x: [{x: y for x, y in i} for i in list(zip(x['name'], x['price']))],
    axis=1)
# print(zip(df2.columns.drop('id').tolist(), df['']))
# zip(x['name'], x['price'])
print(df)

# df = df1.join(df, lsuffix="_last", rsuffix='_other', how='left')
# df = df[['id', 'new']].to_dict('records')
# print(df)

# print(df2.columns.drop('id').tolist())
#
# df2 = df2.groupby(df2.columns.drop('id').tolist(), as_index=False).agg(dict)
#
# print(df2)
#
# df2 = df1.merge(df2)
# # df2 = df2.groupby(df2.columns.drop('name').tolist(), as_index=False).agg(list)
# print(df2)

# df = df1.join(df2)
# print(df)
# file = File('test')
# df = pd.read_parquet(file.create_file_parquet(df))
# print(df)
# print(df.to_dict('records'))


df = pd.read_csv('https://raw.githubusercontent.com/Damir214/pandas_problem/master/data.csv')

# df = df.groupby(['chknum']).agg(list)
# print(df)
# df = df.to_dict('records')
#
# # df['new'] = zip(df['person_id'], df['good_id'])
#
# print(df)
