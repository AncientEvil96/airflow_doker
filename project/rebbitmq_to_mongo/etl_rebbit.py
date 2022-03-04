from base.mongo import Mongo
from base.rebbit import Rebbit
from sys import argv

rebbitmq_s, mongodb_s = argv[1:]

if __name__ == '__main__':
    s = str(rebbitmq_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(',')
    rebbitmq = dict(zip(s[::2], s[1::2]))
    s = str(mongodb_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(',')
    mongodb = dict(zip(s[::2], s[1::2]))

    sourse = Rebbit(params=rebbitmq)
    target = Mongo(params=mongodb)
    sourse.callback_rebbit_mongo(target)
