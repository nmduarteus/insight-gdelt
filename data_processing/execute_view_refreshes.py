# !/usr/bin/python

"""
Class to refresh PostreSQL views
"""
import logging
import os
import sys

import psycopg2

sys.path.append(os.path.expanduser('~/insight/code/config/'))
import config

class ExecuteViewRefreshes:

    def close(self):
        logging.info("Closing connection...")
        self.cursor.close()
        self.conn.close()

    def connect(self):
        try:
            params = config.config("gdelt")
            self.conn = psycopg2.connect(**params)

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("ERROR: ", error)
        return self.conn

    def execRefresh(self):
        logging.info("Refreshing views...")
        self.cursor.execute("REFRESH MATERIALIZED VIEW avg_tone_view")
        self.cursor.execute("REFRESH MATERIALIZED VIEW news_to_show_view")

        logging.info("Done refreshing views....")

    def __init__(self):
        self.conn = None
        self.connect()
        self.cursor = self.conn.cursor()
