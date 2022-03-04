from base.my import MySQL
from sys import argv
import pandas as pd
from pathlib import Path
import re

sours_params_s = argv[1]
local_dir = '/tmp/tmp/'

def get_df(key_):
    files = list(map(str, list(Path(f'{local_dir}').rglob(f'{key_}.parquet.gzip'))))
    file = [x for x in files if re.match(f'.*{key_}*', x)][0] if any(
        f'{key_}' in word for word in files) else ''

    if file == '':
        print('not file')
        exit(1)

    return pd.read_parquet(file)


def get_load_list():
    df = get_df('stock')

    return list(df.itertuples(index=False, name=None))


if __name__ == '__main__':
    load_list = get_load_list()

    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    target = MySQL(
        params=sours_params
    )

    table = 'tmp_stock'
    target.connection_init()

    print('создаем временную таблицу')

    target.query_to_base(f'drop table if exists {table};')
    target.query_to_base(
        f"""
        CREATE OR REPLACE table {table}
        (
            Checked        tinyint,
            shop_id        int,
            latitude       char(255),
            longitude      char(255),
            Adress         char(255),
            House          char(255),
            Email          char(255),
            Title          char(255),
            City_name      varchar(255),
            City_id        int,
            Subdivision_ID int,
            Sub_Class_ID   int
        ) ENGINE = InnoDB
          DEFAULT CHARSET = UTF8;
        """
    )

    print('заполняем данными')
    query = f"""
            INSERT INTO {table}
                (
                    Checked         ,
                    shop_id         ,
                    latitude        ,
                    longitude       ,
                    Adress          ,
                    House           ,
                    Email           ,
                    Title           ,
                    City_name       ,
                    Subdivision_ID  ,
                    Sub_Class_ID    
                 )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s);
        """

    target.load_many_to_base(query, load_list)

    print('создаем города')
    target.query_to_base(
        """
        INSERT INTO Classificator_City_presence
        (City_presence_Name,
         Checked)
        SELECT City_name, max(tmp_stock.Checked)
        FROM tmp_stock
                 LEFT JOIN Classificator_City_presence as City
                           ON tmp_stock.City_name = City.City_presence_Name
        WHERE City.City_presence_ID is null
        GROUP BY City_name;
        """
    )

    print('удаление городов')
    target.query_to_base(
        """
        DELETE
        FROM Classificator_City_presence
        WHERE City_presence_Name in (
            SELECT City_presence_Name
            FROM Classificator_City_presence as City
                     LEFT JOIN tmp_stock
                               ON tmp_stock.City_name = City.City_presence_Name
            WHERE tmp_stock.City_name is null
            GROUP BY City_name
        );
        """
    )

    print('обновим ID')
    target.query_to_base(
        """
        UPDATE tmp_stock
            INNER JOIN Classificator_City_presence as tt1 ON City_name = City_presence_Name
        SET tmp_stock.City_id = tt1.City_presence_ID;
        """
    )

    print('обновим видимость городов')
    target.query_to_base(
        """
        UPDATE Classificator_City_presence
            INNER JOIN (SELECT City_id as City_id, max(tmp_stock.Checked) as Checked
                        FROM tmp_stock
                                 LEFT JOIN Classificator_City_presence as City
                                           ON tmp_stock.City_name = City.City_presence_Name
                        GROUP BY City_name) as tt1 ON City_id = City_presence_ID
        SET Classificator_City_presence.Checked = tt1.Checked
        WHERE Classificator_City_presence.Checked =! tt1.Checked;
        """
    )

    print('добавим данные по магазинам')
    target.query_to_base(
        """
        INSERT INTO Message169
        (Checked,
         shop_id,
         latitude,
         longitude,
         Adress,
         House,
         Email,
         Title,
         City,
         Subdivision_ID,
         Sub_Class_ID)
        SELECT tmp_stock.Checked,
               tmp_stock.shop_id,
               tmp_stock.latitude,
               tmp_stock.longitude,
               tmp_stock.Adress,
               tmp_stock.House,
               tmp_stock.Email,
               tmp_stock.Title,
               tmp_stock.City_id,
               tmp_stock.Subdivision_ID,
               tmp_stock.Sub_Class_ID
        FROM tmp_stock
        LEFT JOIN Message169
        ON Message169.shop_id = tmp_stock.shop_id
        WHERE Message169.Message_ID is null;
        """
    )

    print('проверка + обновление данных')
    target.query_to_base(
        """
        UPDATE Message169
            INNER JOIN tmp_stock ON tmp_stock.shop_id = Message169.shop_id
        SET Message169.Checked        = tmp_stock.Checked,
            Message169.shop_id        = tmp_stock.shop_id,
            Message169.latitude       = tmp_stock.latitude,
            Message169.longitude      = tmp_stock.longitude,
            Message169.Adress         = tmp_stock.Adress,
            Message169.House          = tmp_stock.House,
            Message169.Email          = tmp_stock.Email,
            Message169.Title          = tmp_stock.Title,
            Message169.City           = tmp_stock.City_id,
            Message169.Subdivision_ID = tmp_stock.Subdivision_ID,
            Message169.Sub_Class_ID   = tmp_stock.Sub_Class_ID
        WHERE not (Message169.Checked = tmp_stock.Checked
            AND Message169.shop_id = tmp_stock.shop_id
            AND Message169.latitude = tmp_stock.latitude
            AND Message169.longitude = tmp_stock.longitude
            AND Message169.Adress = tmp_stock.Adress
            AND Message169.House = tmp_stock.House
            AND Message169.Email = tmp_stock.Email
            AND Message169.Title = tmp_stock.Title
            AND Message169.City = tmp_stock.City_id
            AND Message169.Subdivision_ID = tmp_stock.Subdivision_ID
            AND Message169.Sub_Class_ID = tmp_stock.Sub_Class_ID)
        """
    )

    target.connection_close()
