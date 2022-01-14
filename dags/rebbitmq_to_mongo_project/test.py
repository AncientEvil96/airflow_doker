from dags.bases.rebbit import Rebbit
from dags.bases.mongo import Mongo


class DataTransfer:

    def __init__(self, **kwargs):
        self.rebbitmq = kwargs.pop('rebbitmq')
        self.mongodb = kwargs.pop('mongodb')

    def rebit_mongo(self):
        sourse = Rebbit(params=self.rebbitmq)
        target = Mongo(params=self.mongodb)
        target.update_mongo(sourse.callback_rebbit())


if __name__ == '__main__':
    pass
    # for rebbitmq in rebbit_srv:
    #     rebbitmq['password'] = rebbit_pass
    #     rebbitmq['login'] = rebbit_login
    #     for mongodb in mongo_connect:
    #         rebbitmq['queue'] = mongodb['queue']
    #         mongodb['login'] = mongo_login
    #         mongodb['password'] = mongo_pass
    #         etl = DataTransfer(rebbitmq=copy.deepcopy(rebbitmq), mongodb=copy.deepcopy(mongodb))
    #         etl.rebit_mongo()
