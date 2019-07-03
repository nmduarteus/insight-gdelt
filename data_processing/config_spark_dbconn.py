import logging
import os
import sys

sys.path.append(os.path.expanduser('~/insight/code/config/'))
import config


class SparkPostgreConn:
    def __init__(self):
        logging.info("Parsing configuration file....")

        db = config.config("gdelt")

        self.db_name = db['database']
        self.host = db['host']

        self.url_conn = "jdbc:postgresql://{host}:5432/{db}".format(host='10.0.0.13', db='gdelt')

        self.props = {"user":db['user'],
                      "password" : db['password'],
                      "driver": "org.postgresql.Driver"
                     }


    def write(self, df, table, md):
        df.write.option("truncate","true").jdbc(url=self.url_conn,table= table,mode=md,properties=self.props)


    def write_with_partitions(self, df, table, md, column_partition, num_partitions, min_bound, max_bound):
        df.write.option("lowerBound",min_bound).option("upperBound",max_bound).option("partitionColumn", column_partition).option("numPartitions", num_partitions).option("truncate","true").jdbc(url=self.url_conn,table= table,mode=md,properties=self.props)