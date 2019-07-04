import argparse
import logging
import os
import sys

import pyspark.sql.functions as f
import queries
import quilt
import tools
from config_spark_dbconn import SparkPostgreConn
from execute_view_refreshes import ExecuteViewRefreshes
from newsplease import NewsPlease
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import StringType, IntegerType


def clean_words(news):
    """
    Function to clean the data from the news
    :return: Dataframe containing the list of words in the text and the total number of times they occur
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


def upload_stats(spark, schemas_dic):
    """
    Function to upload the stats to Postgres so that they are available to be consumed by the web ui
    :param spark: Spark session
    :return:
    """
    # PostgreSQL connection and writer
    postgresql_conn = SparkPostgreConn()

    # creates a UDF so we can add the data as a new column to the already existing dataframe
    events_dt_udf = f.udf(lambda z: int(z), IntegerType())

    logging.info("Reading data from S3...")
    events = tools.read_from_s3(spark, "export", schemas_dic['events'], "all")
    events_with_date = events.withColumn("year_month_int", events_dt_udf(events['MonthYear']))
    mentions = tools.read_from_s3(spark, "mentions", schemas_dic['mentions'], "all")
    news = tools.read_from_s3_enriched(spark, "news", schemas_dic['news'], cmd_opts.date)

    # registering temp tables for querying
    events_with_date.registerTempTable("events")
    mentions.registerTempTable("mentions")
    news.registerTempTable("news")
    news_to_show = clean_words(news)

    # get data
    logging.info("Doing queries for webui...")
    events_data = spark.sql(queries.events_query)
    most_mentions = spark.sql(queries.mostmentions_query)
    top_channels = spark.sql(queries.top_channels_query)

    # write data to PostgreSQL
    logging.info("Saving data to webui...")
    postgresql_conn.write(df=events_data, table="top_events", md="overwrite")
    postgresql_conn.write(df=top_channels, table="top_channels", md="overwrite")
    postgresql_conn.write(df=news_to_show, table="news_to_show", md="overwrite")

    #refreshes materialized views for UI
    refresh_conn = ExecuteViewRefreshes()
    refresh_conn.execRefresh()
    refresh_conn.close()
    #postgresql_conn.write(df=most_mentions, table="top_mentions", md="overwrite")
    #postgresql_conn.write_with_partitions(df=most_mentions, table="top_mentions", md="overwrite",
    #                                      column_partition="year_month_int", num_partitions=20, min_bound=200801, max_bound=202001)


def get_news(link):
    """
    Function to get the news for a certain URL - using library newsplease

    :param link: the URL link for the news
    :return: the content of the news for the link provided
    """
    try:
        article = NewsPlease.from_url(link)

        # we need to remove new lines and quotes, otherwise quilt will fail
        article_no_newlines = article.text.replace('\n', '')
        article_no_quotes = article_no_newlines.replace('"', "'")

        return article_no_quotes
    except:
        print("An exception occurred while scrapping the news:", link)
        pass

    return None


def upload_to_quilt(spark, schemas_dic):
    """
    Function to upload data to quilt and to append it to already existing data
    :param spark: Spark Sessuin
    :return: None
    """

    # remove old data and get new one
    logging.info("Installing quilt gdelt data...")
    quilt.rm("nmduarte/gdelt", force=True)
    quilt.install("nmduarte/gdelt", force=True)
    from quilt.data.nmduarte import gdelt

    # get the old data from quilt
    logging.info("getting data from quilt...")
    events_from_quilt = gdelt.events()
    mentions_from_quilt = gdelt.mentions()
    news_from_quilt = gdelt.news()

    # transform the data into dataframes so it can be appended
    logging.info("Creating dataframes from quilt data...")
    events_from_quilt_df = spark.createDataFrame(events_from_quilt, schema=schemas_dic['events2'])
    mentions_from_quilt_df = spark.createDataFrame(mentions_from_quilt, schema=schemas_dic['mentions'])
    news_from_quilt_df = spark.createDataFrame(news_from_quilt, schema=schemas_dic['news'])

    # mentions data - new data
    logging.info("Reading last 15min data from S3...")
    mentions_df = tools.read_from_s3_enriched(spark, "mentions", schemas_dic['mentions'], cmd_opts.date)
    events_df = tools.read_from_s3_enriched(spark, "events", schemas_dic['events2'], cmd_opts.date)
    news_df = tools.read_from_s3_enriched(spark, "news", schemas_dic['news'], cmd_opts.date)

    # concatenate already existing data with new data
    logging.info("Appending data to old quilt data...")
    mentions_concat = mentions_from_quilt_df.union(mentions_df)
    events_concat = events_from_quilt_df.union(events_df)
    news_concat = news_from_quilt_df.union(news_df)

    # build the 3 packages
    logging.info("Building quilt packages...")
    quilt.build("nmduarte/gdelt/mentions", mentions_concat.toPandas())
    quilt.build("nmduarte/gdelt/events", events_concat.toPandas())
    quilt.build("nmduarte/gdelt/news", news_concat.toPandas())

    # push the 3 packages
    logging.info("Pushing quilt info...")
    quilt.push("nmduarte/gdelt/mentions", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt/events", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt/news", is_public=True, is_team=False)


def do_crawling(spark, schemas_dic):
    """
    Function used to enrich the data with the crawled data
    :param spark: Context
    :return: None
    """

    logging.info("Getting latest 15min data from S3 and start crawling...")
    # mentions data
    mentions_df = tools.read_from_s3(spark, "mentions", schemas_dic['mentions'], cmd_opts.date)

    # events data
    events_df = tools.read_from_s3(spark, "export", schemas_dic['events'], cmd_opts.date)

    logging.debug("Mentions has %s records", mentions_df.count())
    logging.debug("Events has %s records", events_df.count())

    # creates a UDF so we can add the data as a new column to the already existing dataframe
    news_udf = f.udf(lambda z: get_news(z),
                     StringType())


    # creates a UDF for the hash value
    hash_udf = f.udf(lambda z: hash(z), StringType())

    # adds hash column based on the sourceurl so that data can be joined afterwards
    events_df_hash = events_df.withColumn("HashURL", hash_udf(events_df.SOURCEURL))

    # events alias
    events = events_df_hash.alias("events")

    # there are several entries that can have the same URL and we don't want to scrape those,
    # so we select the distinct values only
    distinct_news = events.select(events.SOURCEURL,
                                  events.HashURL, events.SQLDATE).distinct()

    logging.debug("There are % distinct links to be scrapped", distinct_news.count())

    # gets the scrapped data and add it to the dataset
    distinct_news_with_data = distinct_news.withColumn("NewsText", news_udf(distinct_news.SOURCEURL))
    logging.debug("Partitions %s", distinct_news_with_data.rdd.getNumPartitions())

    # write files to s3
    logging.info("Uploading data to S3...")
    tools.upload_to_s3(distinct_news_with_data, "news", cmd_opts.date)
    tools.upload_to_s3(events_df_hash, "events", cmd_opts.date)
    tools.upload_to_s3(mentions_df, "mentions", cmd_opts.date)


def main():
    """
    Main function
    :return:
    """

    # date created by airflow
    spark_input_file = os.path.expanduser('~/insight/spark.txt')

    # schemas
    schemas_dic = tools.set_schemas()

    # define session configurations and master
    spark = tools.spark_session()

    # if there's no date from input, let's read it from the file created by airflow
    if cmd_opts.date is None:
        logging.info("No date provided by user. Reading date from spark.txt file")
        spark_input = open(spark_input_file, "r")
        cmd_opts.date = spark_input.read()
        spark_input.close()

    print("Date for analysis and processing ", cmd_opts.date)
    logging.debug("Date for analysis and processing %s", cmd_opts.date)

    """
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.shuffle.service.enabled", "true")
    """

    if cmd_opts.action == "gather":
        logging.debug("Option Selected: Crawling Data")
        do_crawling(spark, schemas_dic)
    elif cmd_opts.action == "quilt":
        logging.debug("Option Selected: Upload to Quilt")
        upload_to_quilt(spark, schemas_dic)
    elif cmd_opts.action == "stats":
        logging.debug("Option Selected: Create Stats")
        upload_stats(spark, schemas_dic)
    elif cmd_opts.action is None:
        logging.debug("Option Selected: End to End")
        do_crawling(spark, schemas_dic)
        upload_to_quilt(spark, schemas_dic)
        upload_stats(spark, schemas_dic)

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GDELT+')

    parser.add_argument('-d', '--date',
                        help='Datetime to be processed', required=False)

    parser.add_argument('-a', '--action',
                        help='Action to be processed', required=False)

    cmd_opts = parser.parse_args()

    if cmd_opts.action not in ['gather', 'quilt', 'stats', None]:
        print("Please use one of the following actions - 'gather', 'stats' or 'quilt'")
        sys.exit()

    # set logging format
    logging.basicConfig(filename="gdelt.log", format='%(asctime)s %(levelname)s: %(message)s ', level=logging.INFO)

    main()
