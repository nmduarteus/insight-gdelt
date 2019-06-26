# !/usr/bin/python

"""
Class to deal with Postgresql
"""
import logging

import pandas.io.sql as psql
import psycopg2
from config import config_dbconn


class PostgresqlObj:

    def close(self):
        logging.info("Closing connection...")
        self.conn.close()

    def connect(self):
        try:
            params = config_dbconn.config("gdelt")
            self.conn = psycopg2.connect(**params)

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("ERROR " + error)

        return self.conn

    def get_all_categories(self):
        logging.info("Getting cameo categories....")
        query = """select
        *
    from
        cameo_categories
    order by 1"""
        df = psql.read_sql_query(query, self.conn)
        logging.info("Done getting cameo categories....")
        return df

    def get_all_subcategories(self):
        logging.info("Getting cameo subcategories....")
        query = """select
        *
    from
        cameo_subcategories
    order by 1"""
        df = psql.read_sql_query(query, self.conn)
        logging.info("Done getting cameo subcategories....")
        return df

    def __init__(self):
        self.conn = None
        self.connect()
