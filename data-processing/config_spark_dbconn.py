import logging
import os
from configparser import ConfigParser

class SparkPostgreConn:
    def __init__(self,filename, section):
        logging.info("Parsing configuration file....")

        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(os.path.expanduser(filename))

        # get section, default to gdelt
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, os.path.expanduser(filename)))

        self.db_name = db['database']
        self.host = db['host']

        self.url_conn = "jdbc:postgresql://{host}:5432/{db}".format(host='10.0.0.13', db='gdelt')

        self.props = {"user":db['user'],
                      "password" : db['password'],
                      "driver": "org.postgresql.Driver"
                     }


    def write(self, df, table, md):
        df.write.option("numPartitions", 30).option("truncate","true").jdbc(url=self.url_conn,table= table,mode=md,properties=self.props)
