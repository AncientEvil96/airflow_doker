import pandas as pd
from dags.bases.operations_to_files import File
i = None
if i:
    exit(0)

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

# print(df1)

# print(df2)


df = df2.groupby(['id']).agg(lambda x: [(x.name, i) for i in list(x)])
# print(df.to_dict('records'))
# print(df)
df = df1.merge(df, left_on='id', right_on='id')


# print(df)

def test(x):
    answer = []
    for couter, i in enumerate(x):
        if couter == 0:
            answer = i
            continue
        answer = list(zip(answer, i))
    return answer


# zip(x['name'], x['price'])
# test(x[df2.columns.drop('id').tolist()])
df['new'] = df[df2.columns.drop('id').tolist()].apply(
    lambda x: [{x: y for x, y in i} for i in test(x[df2.columns.drop('id').tolist()])],
    axis=1)
# print(zip(df2.columns.drop('id').tolist(), df['']))
# zip(x['name'], x['price'])
print(df)
for i in df[['id', 'new']].to_dict('records'):
    print(i)

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
