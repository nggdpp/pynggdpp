from pymongo import MongoClient
import os


class Infrastructure:
    def __init__(self):
        self.mongo_uri = "mongodb://" + os.environ["MONGODB_USERNAME"] \
                    + ":" \
                    + os.environ["MONGODB_PASSWORD"] \
                    + "@" \
                    + os.environ["MONGODB_SERVER"] \
                    + "/" \
                    + os.environ["MONGODB_DATABASE"]
        self.mongo_client = MongoClient(self.mongo_uri)

    def connect_mongodb(self, collection=None):
        db = self.mongo_client.get_database(os.environ["MONGODB_DATABASE"])

        if collection is not None:
            return db[collection]
        else:
            return db

