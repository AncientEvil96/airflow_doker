from sys import argv
import pandas as pd

sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/'
local_dir = f''


def extract_change_amount():
    df_ms = pd.read_parquet(f'{local_dir}amount_ms.parquet.gzip')
    df_ms.set_index(['product_id', 'stock_name'], inplace=True)
    df_maria = pd.read_parquet(f'{local_dir}amount_maria.parquet.gzip')
    df_maria.set_index(['product_id', 'stock_name'], inplace=True)

    df_full = (
        df_maria.merge(
            df_ms,
            how='outer',
            on=['product_id', 'stock_name'],
            suffixes=['', '_new'],
            indicator=True
        )
    )

    df_full[['product_amount', 'product_amount_new']] = df_full[['product_amount', 'product_amount_new']].fillna(
        0
    ).astype('int')

    df_new = df_full.query("product_amount != product_amount_new or _merge=='right_only'")
    df_new = df_new[['product_amount_new']]
    df_new = df_new.reset_index(['product_id', 'stock_name'])
    df_new.to_csv(f'{local_dir}amount_new.csv', sep='\t', header=False, index=False)
    print('\n', f'не изменено: {len(df_new)}')
    print('\n', df_new)

    if len(df_new) == 0:
        exit(1)


if __name__ == '__main__':
    extract_change_amount()
