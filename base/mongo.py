from pymongo import MongoClient


class Mongo:

    def __init__(self, **kwargs):
        """

        For your comfortable work with wongodb.

        mongodb:
        :param kwargs:
        host: server DNS or IP
        password:
        login:
        port:
        schema: connections base
        database: connections collection

        """

        mongodb = kwargs.pop('params')
        self.mongo_host = mongodb.pop('host')
        self.__mongo_password = mongodb.pop('password')
        self.__mongo_login = mongodb.pop('login')
        self.mongo_port = mongodb.pop('port')
        self.mongo_schema = mongodb.pop('schema')
        self.mongo_database = mongodb.pop('database')
        self.__url = f'mongodb://{self.__mongo_login}:{self.__mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_schema}'

    def update_mongo(self, load_list: list):

        """
        modernization update for update many
        :param
        load_list: list type
        """

        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            for load_doc in load_list:
                try:
                    collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, {'$set': load_doc}, upsert=True)
                except Exception as err:
                    print(err, load_doc)

    def update_mongo_v2(self, load_list: list, up_key: str):

        """
        modernization update for update many
        :param
        load_list: list type
        """

        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            for load_doc in load_list:
                try:
                    collection.update_one({up_key: {'$eq': load_doc[up_key]}}, {'$set': load_doc}, upsert=True)
                except Exception as err:
                    print(err, load_doc)

    def insert_mongo(self, load_list: list):

        """
        insetr many for list
        load_list: list type
        """

        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            collection.insert_many(load_list)

    def delete_mongo(self, filter: str):

        """
        insetr many for list
        load_list: list type
        """

        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            collection.delete_many(filter)

    def update_mongo_rabbit(self, load_list):

        """
        modernization update for update many
        :param
        load_list: list type
        """

        ask = []

        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            for load_doc, i in load_list:
                try:
                    collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, {'$set': load_doc}, upsert=True)
                    ask.append(i)
                except Exception as err:
                    print(err, load_doc)

            return ask

    def select(self, query, limit=0, rows=None):

        """
        find row in collections
        :return:
        """
        with MongoClient(self.__url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            if limit == 0:
                return list(collection.find(query, rows))
            else:
                return list(collection.find(query, rows).limit(limit))
