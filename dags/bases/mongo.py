from pymongo import MongoClient


class Mongo:

    def __init__(self, **kwargs):
        """

        mongodb:
        :param kwargs:
        host:
        password:
        login:
        port:
        schema:
        database:

        """

        mongodb = kwargs.pop('params')
        self.mongo_host = mongodb.pop('host')
        self.__mongo_password = mongodb.pop('password')
        self.__mongo_login = mongodb.pop('login')
        self.mongo_port = mongodb.pop('port')
        self.mongo_schema = mongodb.pop('schema')
        self.mongo_database = mongodb.pop('database')

    def update_mongo(self, load_list: list):
        url = f'mongodb://{self.__mongo_login}:{self.__mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_schema}'

        with MongoClient(url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            for load_doc in load_list:
                try:
                    collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, {'$set': load_doc}, upsert=True)
                except Exception as err:
                    print(err, load_doc)
