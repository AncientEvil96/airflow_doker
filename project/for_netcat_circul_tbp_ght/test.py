import pandas as pd

df1 = pd.read_csv('subdivision_vprok.csv')
df2 = pd.read_csv('subdivision_compass.csv')

df = pd.concat([df1, df2])

print(df)