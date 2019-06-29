import argparse
import sys

import quilt
import tools
from newsplease import NewsPlease
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

master = "spark://ec2-34-219-229-126.us-west-2.compute.amazonaws.com:7077"
spark_input_file = "/home/ubuntu/insight/spark.txt"


def get_news(link):
    """
    Function to get the news for a certain URL - using library newsplease

    :param link: the URL link for the news
    :return: the content of the news for the linl provided
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


def upload_to_quilt(spark):
    # downloads the data from s3
    print("Getting schemas..")
    events_schema, mentions_schema, news_schema, events_schema2, gkg_schema = tools.set_schemas()

    # remove old data and get new one
    quilt.rm("nmduarte/gdelt", force=True)
    quilt.install("nmduarte/gdelt", force=True)
    from quilt.data.nmduarte import gdelt

    # get the old data from quilt
    events_from_quilt = gdelt.events()
    mentions_from_quilt = gdelt.mentions()
    news_from_quilt = gdelt.news()

    # transform the data into dataframes so it can be appended
    events_from_quilt_df = spark.createDataFrame(events_from_quilt, schema=events_schema2)
    mentions_from_quilt_df = spark.createDataFrame(mentions_from_quilt, schema=mentions_schema)
    news_from_quilt_df = spark.createDataFrame(news_from_quilt, schema=news_schema)

    # mentions data - new data
    print("Getting mention data..")
    mentions_df = tools.read_from_s3_enriched(spark, "mentions", mentions_schema, cmd_opts.date)

    events_df = tools.read_from_s3_enriched(spark, "events", events_schema2, cmd_opts.date)

    news_df = tools.read_from_s3_enriched(spark, "news", news_schema, cmd_opts.date)

    # concatenate already existing data with new data
    mentions_concat = mentions_from_quilt_df.union(mentions_df)
    events_concat = events_from_quilt_df.union(events_df)
    news_concat = news_from_quilt_df.union(news_df)

    # build the 3 packages
    quilt.build("nmduarte/gdelt/mentions", mentions_concat.toPandas())
    quilt.build("nmduarte/gdelt/events", events_concat.toPandas())
    quilt.build("nmduarte/gdelt/news", news_concat.toPandas())

    # push the 3 packages
    quilt.push("nmduarte/gdelt/mentions", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt/events", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt/news", is_public=True, is_team=False)


def do_crawling(spark):
    """
    Function used to enrich the data with the crawled data
    :param spark: Context
    :return:
    """

    print("Getting schemas..")
    events_schema, mentions_schema, news_schema, events_schema2, gkg_schema = tools.set_schemas()

    gkg_df = tools.read_from_s3(spark, "gkg", gkg_schema, cmd_opts.date)
    print(gkg_df.show())

    # mentions data
    print("Getting mention data..")
    mentions_df = tools.read_from_s3(spark, "mentions", mentions_schema, cmd_opts.date)

    # events data
    print("Getting events data..")
    events_df = tools.read_from_s3(spark, "export", events_schema, cmd_opts.date)

    print("Mentions has ", mentions_df.count(), "records")

    print("Events has ", events_df.count(), "records")

    # creates a UDF so we can add the data as a new column to the already existing dataframe
    news_udf = udf(lambda z: get_news(z),
                   StringType())

    # creates a UDF for the hash value
    hash_udf = udf(lambda z: hash(z), StringType())

    # adds hash column based on the sourceurl so that data can be joined afterwards
    events_df_hash = events_df.withColumn("HashURL", hash_udf(events_df.SOURCEURL))

    # events alias
    events = events_df_hash.alias("events")

    # there are several entries that can have the same URL and we don't want to scrape those,
    # so we select the distinct values only
    distinct_news = events.select(events.SOURCEURL,
                                  events.HashURL, events.SQLDATE).distinct()

    print("There are ", distinct_news.count(), " distinct URLs")

    # gets the scrapped data and add it to the dataset
    distinct_news_with_data = distinct_news.withColumn("NewsText", news_udf(distinct_news.SOURCEURL))

    pan = distinct_news_with_data.toPandas()
    print(pan.head())

    print("Partitions: ", distinct_news_with_data.rdd.getNumPartitions())

    print("Loading to S3....")

    # write files to s3
    tools.upload_to_s3(distinct_news_with_data, "news", cmd_opts.date)
    tools.upload_to_s3(events_df_hash, "events", cmd_opts.date)
    tools.upload_to_s3(mentions_df, "mentions", cmd_opts.date)


def main():
    """
    Main function
    :return:
    """

    # if there's no date from input, let's read it from the file created by airflow
    if cmd_opts.date is None:
        print("No date provided by user. Reading date from spark.txt file")
        spark_input = open(spark_input_file, "r")
        cmd_opts.date = spark_input.read()
        spark_input.close()

    print(cmd_opts.date)

    # define session configurations and master
    spark = tools.spark_session()

    """
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.shuffle.service.enabled", "true")
    """

    if cmd_opts.action == "gather":
        print("Option Selected: Crawling Data")
        do_crawling(spark)
    elif cmd_opts.action == "quilt":
        print("Option Selected: Upload to Quilt")
        upload_to_quilt(spark)
    elif cmd_opts.action is None:
        print("Option Selected: End to End")
        do_crawling(spark)
        upload_to_quilt(spark)

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GDELT+')

    parser.add_argument('-d', '--date',
                        help='Datetime to be processed', required=False)

    parser.add_argument('-a', '--action',
                        help='Action to be processed', required=False)

    cmd_opts = parser.parse_args()

    if cmd_opts.action not in ['gather', 'quilt', None]:
        print("Please use one of the following actions - 'gather' or 'quilt'")
        sys.exit()

    main()
