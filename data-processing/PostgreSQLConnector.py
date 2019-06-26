"""
Class to Connect to database
"""
from pyspark.sql import DataFrameWriter

class dbConnection():

    def __init__(self):
        self.host = '10.0.0.13'
        self.db = 'gdelt'
        self.url = "jdbc:postgresql://{host}:5432/{db}".format(host=self.host, db=self.db)
        self.properties = {"user":"gdelt_user",
                      "password" : "tavares",
                      "driver": "org.postgresql.Driver"
                     }

    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, md):
        my_writer = self.get_writer(df)
        df.write.jdbc(url=self.url_connect, table=table, mode=md, properties=self.properties)