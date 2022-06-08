import pandas as pd
from sys import argv
import geocode_yandex
from base.operations_to_files import File

begin_dt = argv[1]
local_dir = f'/tmp/tmp/{begin_dt}/'


def write_yandex_location(longitude, latitude):
    return geocode_yandex.get_yandex_xml(longitude, latitude)


if __name__ == '__main__':
    df_cheks = pd.read_parquet(f'{local_dir}df_shops_from_checks.parquet.gzip')
    df_tbp = pd.read_parquet(f'{local_dir}df_shops_from_tbp.parquet.gzip')
    df_mongo = pd.read_json(f'{local_dir}mongo_stock.json')

    df_full = df_tbp.join(df_cheks, on='id', rsuffix='_cheks')

    df_full['type'] = df_full[['warehouse_type_id', 'warehouse_type_discription']].apply(
        lambda x: {'id': x['warehouse_type_id'], 'discription': x['warehouse_type_discription']}, axis=1)

    df_full['size_restrictions'] = df_full[
        [
            'max_weight',
            'max_length',
            'max_width',
            'max_height'
        ]
    ].apply(
        lambda x: {
            'weight': x['max_weight'],
            'length': x['max_length'],
            'width': x['max_width'],
            'height': x['max_height']
        },
        axis=1
    )

    df_full['org'] = df_full[['org_id', 'org_name']].apply(
        lambda x: {key: val for key, val in {'id': x['org_id'], 'name': x['org_name']}.items() if str(val) != ''},
        axis=1)

    df_full['address_1c'] = df_full[
        [
            'unloading_address',
            'prefix_addresses',
            'prefix_city',
            'city',
            'address_bycomplex',
            'area',
            'shopping_center',
            'region',
            'zip_code',
            'subway_station',
            'region_code',
            'federal_district',
            'index_',
            'house'
        ]
    ].apply(
        lambda x: {key: val for key, val in dict(x).items() if str(val) != ''}, axis=1)

    df_full = df_full.drop(
        columns=[
            'warehouse_type_id',
            'warehouse_type_discription',
            'max_weight',
            'max_length',
            'max_width',
            'max_height',
            'org_id',
            'org_name',
            'unloading_address',
            'prefix_addresses',
            'address_bycomplex',
            'prefix_city',
            'city',
            'area',
            'shopping_center',
            'region',
            'zip_code',
            'subway_station',
            'region_code',
            'federal_district',
            'index_',
            'house'
        ]
    )

    if len(df_mongo) > 0:
        df_full = df_full.join(df_mongo, on='id', rsuffix='_mongo')
        df_full['address_yandex'] = df_full.query(
            '(id_cheks > 0) & (latitude != latitude_mongo | longitude != longitude_mongo)'
        )[
            ['latitude', 'longitude']].apply(
            lambda x: write_yandex_location(x['longitude'], x['latitude']), axis=1)
        df_full = df_full.drop(columns=['longitude_mongo', 'latitude_mongo'])
    else:
        df_full['address_yandex'] = df_full[df_full['id_cheks'] > 0][
            ['latitude', 'longitude']].apply(
            lambda x: write_yandex_location(x['longitude'], x['latitude']), axis=1)

    df_full = df_full.drop(
        columns=[
            'id_cheks'
        ]
    )

    file = File(f'{local_dir}df_full')
    file.create_file_parquet(df_full)
