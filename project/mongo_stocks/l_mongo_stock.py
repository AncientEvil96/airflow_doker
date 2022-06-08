from bson import json_util
from sys import argv
from datetime import datetime
from base.bases_metods import convert_dict_args
from base.mongo import Mongo
from json import dump
import pandas as pd

begin_dt, mongodb_s = argv[1:]
local_dir = f'/tmp/tmp/{begin_dt}/'

if __name__ == '__main__':
    t_date = datetime.strptime(begin_dt, '%Y%m%d')
    mongodb = convert_dict_args(mongodb_s)

    df_full = pd.read_parquet(f'{local_dir}df_full.parquet.gzip')

    load_list = [{key: val for key, val in i.items() if str(val) != '' and val is not None} for i in
                 df_full.to_dict('records')]

    mongo = Mongo(params=mongodb)
    result = mongo.update_mongo_v2(load_list, up_key='id')
