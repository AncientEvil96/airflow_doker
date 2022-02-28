from base.mongo import Mongo
from base.rebbit import Rebbit
from sys import argv

rebbitmq, mongodb = argv[1:]

if __name__ == '__main__':
    sourse = Rebbit(params=rebbitmq)
    target = Mongo(params=mongodb)
    sourse.callback_rebbit_mongo(target)
