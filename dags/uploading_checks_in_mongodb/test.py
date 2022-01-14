import pandas as pd
from ..bases.save_to_file import SaveFile


def transform(file):
    df = pd.read_parquet(file)
    print(df)
    sf = SaveFile()
    sf.create_file_parquet(df=df, file_name=file)

def load(file):
    df = pd.read_parquet(file)
    full_list = []
    for i, row in df.iterrows():
        line_d = {
            'uuid': row['uuid'],
            'phone': row['phone'],
            'shop_name': row['shop_name'],
            'product_name': row['product_name'],
            'created_at': str(row['created_at'].date()),
            'refund': row['refund'],
            'cancellation': row['cancellation'],
            'shop_id': row['shop_id'],
            'barcode': row['barcode'],
            'product_id': row['product_id'],
            'amount': row['amount'],
            'price': row['price']
        }
        full_list.append(line_d)
