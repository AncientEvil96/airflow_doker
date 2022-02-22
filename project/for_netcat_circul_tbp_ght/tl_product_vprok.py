from base.my import MySQL
from sys import argv
import pandas as pd

host, port, password, login, database, file = argv[1:]


def get_load_list():
    # --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) as VariantName
    # --, 0 as User_ID
    # --, 0 as LastUser_ID
    # --, 1 as Checked
    # --, 'goods-' + CONVERT(nvarchar, _Reference11.[_Code]) as Keyword
    # --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
    # --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".' as ncDescription
    # --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) as ncSMO_Title
    # --, IIF(_Reference11.[_Fld1317] = '', _Reference11.[_Fld359], _Reference11.[_Fld1317]) +
    # --  N' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".' as ncSMO_Description
    # --, 1 as Currency
    # --, 1 as CurrencyMinimum

    df = pd.read_parquet('product.parquet.gzip')
    df['VariantName'] = df['Name']
    df[['User_ID', 'LastUser_ID', 'Checked', 'Currency', 'CurrencyMinimum']] = (0, 0, 1, 1, 1)
    df['Keyword'] = 'goods-' + str(df['Article'])
    df['ncDescription'] = df['Name'] + ' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".'
    df['ncSMO_Description'] = df['Name'] + ' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".'
    df['ncSMO_Title'] = df['Name']
    df['parent_id'] = df['parent_id'].fillna(0).astype('int')
    # df['parent_id'] = df['parent_id'].astype('int')
    # print(df.dtypes)

    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':
    load_list = get_load_list()

    # for i in load_list[:10]:
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

    table = 'product_netcat'

    target.connection_init()

    target.query_to_base(
        """
            CREATE TABLE tmp_product
(
    parent_id         bigint,
    Article           bigint,
    Name              char(255),
    Description       longtext,
    VendorTbp         char(255),
    Vendor            char(255),
    Weight            double,
    PackageSize2      double,
    PackageSize3      double,
    PackageSize1      double,
    ncTitle           varchar(255),
    Image             char(255),
    Price             double,
    PriceMinimum      double,
    Status            int,
    StockUnits        tinyint,
    VariantName       char(255),
    User_ID           int,
    LastUser_ID       int,
#     Checked           tinyint,
    Currency          int,
    CurrencyMinimum   int,
    Keyword           char(255),
    ncDescription     text,
    ncSMO_Description text,
    ncSMO_Title       varchar(255),
)

    Subdivision_ID       int                                   not null,
    Sub_Class_ID         int                                   not null,
        """
    )



    target.connection_close()