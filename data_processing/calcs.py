import logging

import pyspark.sql.functions as f
import queries
import tools
from config_spark_dbconn import SparkPostgreConn
from pyspark.ml.feature import StopWordsRemover

# TODO: remove this once moved to main script
date_words = "201906260200"


def cleanWords():
    """
    Function to clean the data from the news
    :return:
    """

    # just alphanumeric
    news_clean = news.where(news["NewsText"].isNotNull()).withColumn('NewsTextClean',
                                                                     f.regexp_replace('NewsText', "[^0-9a-zA-Z ]",
                                                                                      " "))

    # splits the new column
    news_clean_split = news_clean.select(f.split(news_clean["NewsTextClean"], " ").alias("NewsTextClean"))

    # removes stop words
    remover = StopWordsRemover(inputCol="NewsTextClean", outputCol="NoStopWords")
    news_without_stopwords = remover.transform(news_clean_split)

    # splits words in rows and count occurrence of each of them
    news_without_stopwords_str = news_without_stopwords.withColumn('NoStopWordsStr', f.concat_ws(' ', 'NoStopWords'))
    news_to_show = news_without_stopwords_str.withColumn('word', f.explode(f.col('NoStopWords'))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False) \
        .where('word <>""')

    return news_to_show


# PostgreSQL connection and writer
postgresql_conn = SparkPostgreConn()

logging.info("Creating Spark Session...")
spark = tools.spark_session()

logging.info("Loading schemas...")
events_schema, mentions_schema, news_schema, events_schema2, gkg_schema = tools.set_schemas()

logging.info("Reading data from S3...")
events = tools.read_from_s3(spark, "export", events_schema, "all")
mentions = tools.read_from_s3(spark, "mentions", mentions_schema, "all")
news = tools.read_from_s3_enriched(spark, "news", news_schema, date_words)

# registering temp tables for querying
events.registerTempTable("events")
mentions.registerTempTable("mentions")
news.registerTempTable("news")
news_to_show = cleanWords()

# get data
events_data = spark.sql(queries.events_query)
most_mentions = spark.sql(queries.mostmentions_query)
top_channels = spark.sql(queries.top_channels_query)

# write data to PostgreSQL
postgresql_conn.write(df=most_mentions, table="top_mentions", md="overwrite")
postgresql_conn.write(df=events_data, table="top_events", md="overwrite")
postgresql_conn.write(df=top_channels, table="top_channels", md="overwrite")
postgresql_conn.write(df=news_to_show, table="news_to_show", md="overwrite")

spark.stop()
