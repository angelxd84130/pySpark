import pymongo
import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

class ConnectToMongo:
    def __init__(self):
        self._mongo_host = "192.168.8.xx"
        self._mongo_port = 0000
        self._mongo_user = "admin"
        self._mongo_password = "admin"
        self._mongo_db = "test_db"
        self._client = pymongo.MongoClient(host=self._mongo_host, port=self._mongo_port, username=self._mongo_user,
                                          password=self._mongo_password, authSource="admin")
        self._db = self._client[self._mongo_db]

    def get_db(self) -> object:
        return self._db

    def query_db(self, pipeline, collection_name) -> list:
        cursor = self._db[collection_name].aggregate(pipeline)
        result = list(cursor)
        return result

class CreateDataframe:
    def __init__(self, pipeline: list, collection_name: str):
        self.result = ConnectToMongo().query_db(pipeline, collection_name)

    def pandas_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.result)
        return df

    def spark_dataframe(self) -> pyspark.sql.DataFrame:
        rdd = spark.sparkContext.parallelize(self.result)
        df = rdd.toDF()
        return df


# condition
match = {"$match": {"animal": "cat"}}
project = {"$project": {"color": 1, "age": 1, "_id": 0}}
pipeline = [match, project]
collection_name = "pet"

# sample
c = CreateDataframe(pipeline, collection_name)
df = c.pandas_dataframe()
print(df)
df = c.spark_dataframe()
df.show()