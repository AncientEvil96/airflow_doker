from bson import json_util
from sys import argv
from datetime import datetime
from base.bases_metods import convert_dict_args
from base.mongo import Mongo
from json import dump

begin_dt, mongodb_s = argv[1:]
local_dir = f'/tmp/tmp/{begin_dt}/'

if __name__ == '__main__':
    t_date = datetime.strptime(begin_dt, '%Y%m%d')
    mongodb = convert_dict_args(mongodb_s)

    query = {
    }

    rows = {
        "_id": 0,
        "id": 1,
        "latitude": 1,
        "longitude": 1
    }

    mongo = Mongo(params=mongodb)
    result = mongo.select(query, rows=rows)

    with open(f'{local_dir}mongo_stock.json', 'w') as f:
        dump(result, f, default=json_util.default)
