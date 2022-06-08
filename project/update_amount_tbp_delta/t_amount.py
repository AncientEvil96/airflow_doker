from sys import argv
import pandas as pd

sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/'


def extract_change_amount():
    df = pd.read_csv(f'{local_dir}amount.csv', sep='\t', index_col=0)
    df.drop(columns=['SYS_CHANGE_VERSION'], inplace=True)
    df['product_code'] = df['product_code'].apply(lambda x: str(x)[3:])
    df.to_csv(f'{local_dir}amount.csv', sep='\t', index=False, header=False)


if __name__ == '__main__':
    extract_change_amount()
