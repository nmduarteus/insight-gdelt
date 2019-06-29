# !/usr/bin/python

"""
Class to deal with Postgresql
"""
import logging
import os
import sys

import pandas.io.sql as psql
import psycopg2

sys.path.append(os.path.expanduser('~/insight/config/'))
import config

class PostgresqlObj:

    def close(self):
        logging.info("Closing connection...")
        self.conn.close()

    def connect(self):
        try:
            params = config.config("gdelt")
            self.conn = psycopg2.connect(**params)

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("ERROR: ", error)
        return self.conn

    def execQuery(self,query):
        logging.info("Executing Query...", query)
        df = psql.read_sql_query(query, self.conn)
        logging.info("Done Executing Query....")

        return df.to_json(orient='records')

    def __init__(self):
        self.conn = None
        self.connect()
