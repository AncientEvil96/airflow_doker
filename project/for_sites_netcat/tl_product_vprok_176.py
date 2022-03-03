from base.my import MySQL
from sys import argv
import pandas as pd
from pathlib import Path
import re

sours_params_s = argv[1]
# sours_params_s = "[('host','84.38.187.211'),('port',32106),('password','Zrn5qDfXGklpJ59'),('login','vprok_transfer'),('database','vprok')]"
local_dir = '/tmp/tmp/'
# local_dir = ''

table = 'tmp_product'
table_product = 'Message176'
description = ' и другие товары повседневного спроса, можно приобрести в сети магазинов "ВПРОК".'
сatalogue_id = 1


def get_load_list():
    files = sorted(
        list(map(str, list(Path(f'{local_dir}').rglob(f'*.parquet.gzip')))))

    file = [x for x in files if re.match('.*product*', x)][0] if any(
        "product" in word for word in files) else ''

    if file == '':
        print('not file')
        exit(1)

    df = pd.read_parquet(file)
    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':
    load_list = get_load_list()

    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    target = MySQL(
        params=sours_params
    )

    target.connection_init()
    target.query_to_base(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {table}
            (
                parent_id       int          default 0,
                Article         int          not null UNIQUE,
                Name            varchar(255) not null,
                Description     longtext     default '',
                VendorTbp       char(255)    default '',
                Vendor          char(255)    default '',
                Weight          double       default 0,
                PackageSize2    double       default 0,
                PackageSize3    double       default 0,
                PackageSize1    double       default 0,
                ncTitle         varchar(255) default Name,
                Image           char(255)    default '',
                Price           double       not null,
                PriceMinimum    double       not null,
                Status          int          default 0,
                StockUnits      tinyint      default 0,
                VariantName     varchar(255) default Name,
                User_ID         int          default 0,
                LastUser_ID     int          default 0,
                Checked         tinyint      default 1,
                Currency        int          default 1,
                CurrencyMinimum int          default 1,
                Keyword         char(255)    default concat('goods-', Article),
                ncSMO_Title     varchar(255) default Name,
                KEY tmp_product_parent_id_index (parent_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=UTF8
        ;
        """
    )

    query = f"""
                INSERT INTO {table}
                    (
                        parent_id       ,
                        Article         ,
                        Name            ,
                        Description     ,
                        VendorTbp       ,
                        Vendor          ,
                        Weight          ,
                        PackageSize2    ,
                        PackageSize3    ,
                        PackageSize1    ,
                        Image           ,
                        Price           ,
                        PriceMinimum    ,
                        Status          ,
                        StockUnits        
                     )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

    target.load_many_to_base(query, load_list)

    target.query_to_base(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {table}_tt1
        (
            Article    bigint unique,
            Message_ID int
        )
        SELECT Article, Message_ID
        FROM {table_product};
        """
    )

    target.query_to_base(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {table}_2
        (
            Message_ID        int,
            Article           bigint       not null UNIQUE,
            Sub_Class_ID      int,
            Subdivision_ID    int,
            Name              varchar(255) not null,
            Description       longtext     default '',
            VendorTbp         char(255)    default '',
            Vendor            char(255)    default '',
            Weight            double       default 0,
            PackageSize2      double       default 0,
            PackageSize3      double       default 0,
            PackageSize1      double       default 0,
            ncTitle           varchar(255) default Name,
            Image             char(255)    default '',
            Price             double       not null,
            PriceMinimum      double       not null,
            Status            int          default 0,
            StockUnits        tinyint      default 0,
            VariantName       varchar(255) default Name,
            User_ID           int          default 0,
            LastUser_ID       int          default 0,
            Checked           tinyint      default 1,
            Currency          int          default 1,
            CurrencyMinimum   int          default 1,
            Keyword           char(255)    default concat('goods-', Article),
            ncDescription     text         default '',
            ncSMO_Description text         default '',
            ncSMO_Title       varchar(255) default Name
        ) ENGINE=InnoDB DEFAULT CHARSET=UTF8
        SELECT Product.Message_ID                                                                          as Message_ID,
               tmp_product.Article                                                                         as Article,
               Sub_Class.Sub_Class_ID                                                                      as Sub_Class_ID,
               Subdivision.Subdivision_ID                                                                  as Subdivision_ID,
               tmp_product.Name                                                                            as Name,
               tmp_product.Description                                                                     as Description,
               tmp_product.VendorTbp                                                                       as VendorTbp,
               tmp_product.Vendor                                                                          as Vendor,
               tmp_product.Weight                                                                          as Weight,
               tmp_product.PackageSize2                                                                    as PackageSize2,
               tmp_product.PackageSize3                                                                    as PackageSize3,
               tmp_product.PackageSize1                                                                    as PackageSize1,
               tmp_product.ncTitle                                                                         as ncTitle,
               tmp_product.Image                                                                           as Image,
               tmp_product.Price                                                                           as Price,
               tmp_product.PriceMinimum                                                                    as PriceMinimum,
               tmp_product.Status                                                                          as Status,
               tmp_product.StockUnits                                                                      as StockUnits,
               tmp_product.VariantName                                                                     as VariantName,
               tmp_product.User_ID                                                                         as User_ID,
               tmp_product.LastUser_ID                                                                     as LastUser_ID,
               tmp_product.Checked                                                                         as Checked,
               tmp_product.Currency                                                                        as Currency,
               tmp_product.CurrencyMinimum                                                                 as CurrencyMinimum,
               tmp_product.Keyword                                                                         as Keyword,
               CONCAT(tmp_product.Name,'{description}')                                                    as ncDescription,
               CONCAT(tmp_product.Name,'{description}')                                                    as ncSMO_Description,
               tmp_product.ncSMO_Title                                                                     as ncSMO_Title
        FROM {table} as tmp_product
                 INNER JOIN Subdivision
                            ON tmp_product.parent_id = Subdivision.TBP_ID
                                AND Subdivision.Catalogue_ID = {сatalogue_id}
                                AND Subdivision.TBP_ID > 0
                 INNER JOIN Sub_Class
                            ON Subdivision.Subdivision_ID = Sub_Class.Subdivision_ID
                                AND Subdivision.Catalogue_ID = {сatalogue_id}
                 LEFT JOIN {table}_tt1 as Product
                           ON Product.Article = tmp_product.Article
        ;
        """
    )

    target.query_to_base(
        f"""
        INSERT INTO {table_product}
        (Article,
         Sub_Class_ID,
         Subdivision_ID,
         Name,
         Description,
         VendorTbp,
         Vendor,
         Weight,
         PackageSize2,
         PackageSize3,
         PackageSize1,
         ncTitle,
         Image,
         Price,
         PriceMinimum,
         Status,
         StockUnits,
         VariantName,
         User_ID,
         LastUser_ID,
         Checked,
         Currency,
         CurrencyMinimum,
         Keyword,
         ncDescription,
         ncSMO_Description,
         ncSMO_Title)
        SELECT Product.Article           as Article,
               Product.Sub_Class_ID      as Sub_Class_ID,
               Product.Subdivision_ID    as Subdivision_ID,
               Product.Name              as Name,
               Product.Description       as Description,
               Product.VendorTbp         as VendorTbp,
               Product.Vendor            as Vendor,
               Product.Weight            as Weight,
               Product.PackageSize2      as PackageSize2,
               Product.PackageSize3      as PackageSize3,
               Product.PackageSize1      as PackageSize1,
               Product.ncTitle           as ncTitle,
               Product.Image             as Image,
               Product.Price             as Price,
               Product.PriceMinimum      as PriceMinimum,
               Product.Status            as Status,
               Product.StockUnits        as StockUnits,
               Product.VariantName       as VariantName,
               Product.User_ID           as User_ID,
               Product.LastUser_ID       as LastUser_ID,
               Product.Checked           as Checked,
               Product.Currency          as Currency,
               Product.CurrencyMinimum   as CurrencyMinimum,
               Product.Keyword           as Keyword,
               Product.ncDescription     as ncDescription,
               Product.ncSMO_Description as ncSMO_Description,
               Product.ncSMO_Title       as ncSMO_Title
        FROM {table} as tmp_product
                 LEFT JOIN {table}_2 as Product
                           ON Product.Article = tmp_product.Article
        WHERE Product.Message_ID is null
          AND Product.Article is not null
        ;
        """
    )

    target.query_to_base(
        f"""
        update Message176
        INNER JOIN {table}_2 USING (Article)
        SET Message176.Sub_Class_ID      = tmp_product_2.Sub_Class_ID,
            Message176.Subdivision_ID    = tmp_product_2.Subdivision_ID,
            Message176.Name              = tmp_product_2.Name,
            Message176.Description       = tmp_product_2.Description,
            Message176.VendorTbp         = tmp_product_2.VendorTbp,
            Message176.Vendor            = tmp_product_2.Vendor,
            Message176.Weight            = tmp_product_2.Weight,
            Message176.PackageSize2      = tmp_product_2.PackageSize2,
            Message176.PackageSize3      = tmp_product_2.PackageSize3,
            Message176.PackageSize1      = tmp_product_2.PackageSize1,
            Message176.ncTitle           = tmp_product_2.ncTitle,
            Message176.Image             = tmp_product_2.Image,
            Message176.Price             = tmp_product_2.Price,
            Message176.PriceMinimum      = tmp_product_2.PriceMinimum,
            Message176.Status            = tmp_product_2.Status,
            Message176.StockUnits        = tmp_product_2.StockUnits,
            Message176.VariantName       = tmp_product_2.VariantName,
            Message176.User_ID           = tmp_product_2.User_ID,
            Message176.LastUser_ID       = tmp_product_2.LastUser_ID,
            Message176.Checked           = tmp_product_2.Checked,
            Message176.Currency          = tmp_product_2.Currency,
            Message176.CurrencyMinimum   = tmp_product_2.CurrencyMinimum,
            Message176.Keyword           = tmp_product_2.Keyword,
            Message176.ncDescription     = tmp_product_2.ncDescription,
            Message176.ncSMO_Description = tmp_product_2.ncSMO_Description,
            Message176.ncSMO_Title       = tmp_product_2.ncSMO_Title
        WHERE NOT (Message176.Sub_Class_ID = tmp_product_2.Sub_Class_ID and
                   Message176.Subdivision_ID = tmp_product_2.Subdivision_ID and
                   Message176.Name = tmp_product_2.Name and
                   Message176.Description = tmp_product_2.Description and
                   Message176.VendorTbp = tmp_product_2.VendorTbp and
                   Message176.Vendor = tmp_product_2.Vendor and
                   Message176.Weight = tmp_product_2.Weight and
                   Message176.PackageSize2 = tmp_product_2.PackageSize2 and
                   Message176.PackageSize3 = tmp_product_2.PackageSize3 and
                   Message176.PackageSize1 = tmp_product_2.PackageSize1 and
                   Message176.ncTitle = tmp_product_2.ncTitle and
                   Message176.Image = tmp_product_2.Image and
                   Message176.Price = tmp_product_2.Price and
                   Message176.PriceMinimum = tmp_product_2.PriceMinimum and
                   Message176.Status = tmp_product_2.Status and
                   Message176.StockUnits = tmp_product_2.StockUnits and
                   Message176.VariantName = tmp_product_2.VariantName and
                   Message176.User_ID = tmp_product_2.User_ID and
                   Message176.LastUser_ID = tmp_product_2.LastUser_ID and
                   Message176.Checked = tmp_product_2.Checked and
                   Message176.Currency = tmp_product_2.Currency and
                   Message176.CurrencyMinimum = tmp_product_2.CurrencyMinimum and
                   Message176.Keyword = tmp_product_2.Keyword and
                   Message176.ncDescription = tmp_product_2.ncDescription and
                   Message176.ncSMO_Description = tmp_product_2.ncSMO_Description and
                   Message176.ncSMO_Title = tmp_product_2.ncSMO_Title);
        """
    )

    target.connection_close()
