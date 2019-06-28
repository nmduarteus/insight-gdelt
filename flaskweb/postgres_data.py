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
            params = config_dbconn.config("gdelt", "~/insight/database.ini")
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

    def get_countries_with_most_events(self):
        logging.info("Getting top 10 customers with more events...")
        query="""select
        country_code, 
        total
        from top_events order by total desc limit 10
        
        """
        df = psql.read_sql_query(query, self.conn)
        return df.to_json(orient='records')

    def most_mentions_per_month(self):
        logging.info("Getting top 10 mentions per name...")
        query = """select
        val,
        total,
        year_month
        from results where query_name='top_mentions' order by total desc limit 10

        """
        df = psql.read_sql_query(query, self.conn)
        return df.to_json(orient='records')

    def avg_tone_per_month(self):
        logging.info("Getting avg tone per month...")
        query = """select
	year_month::text,
	avg("avg_tone") as tone
from top_mentions
group by year_month
                """
        df = psql.read_sql_query(query, self.conn)
        return df.to_json(orient='records')

    def types_across_time(self):
        logging.info("Getting types across time per month...")
        query = """select 
	cameo_categories.description,
	sum(total),
	to_char(to_timestamp (substr(year_month,5,2)::text, 'MM'), 'TMmon') as m	
from 
	top_mentions 
left join cameo_categories on substr(event_code,1,2)=cameo_categories.code
where substr(year_month,1,4)='2019' and code is not null
group by 
	cameo_categories.description,
	year_month"""
        df = psql.read_sql_query(query, self.conn)
        trans = df.set_index(['description', 'm']).transpose()
        return trans.to_json(orient='records')


    def top_channels(self):
        logging.info("Getting top channels...")
        query = """select * from top_channels limit 3"""
        df = psql.read_sql_query(query, self.conn)
        return df.to_json(orient='records')

    def top_words(self):
        logging.info("Getting top words...")
        query = """select * from news_to_show order by count desc limit 200"""
        df = psql.read_sql_query(query, self.conn)
        return df.to_json(orient='records')

    def __init__(self):
        self.conn = None
        self.connect()
