from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType
from newsplease import NewsPlease
import argparse
import quilt
import sys

bucket = "nmduartegdelt"
prefix = "test_small"
prefixUpload= "upload_small"
master = "spark://ec2-34-219-229-126.us-west-2.compute.amazonaws.com:7077"

def set_schemas():
    """
    Function that creates the schemas for the dataframes so that each dataframe has proper column names
    :return: the existing schemas for the 3 types of files
    """
    # schemas for the input formats
    events_schema = StructType([
        StructField('GLOBALEVENTID', IntegerType(), True),
        StructField('SQLDATE', IntegerType(), True),
        StructField('MonthYear', IntegerType(), True),
        StructField('Year', IntegerType(), True),
        StructField('FractionDate', FloatType(), True),
        StructField('Actor1Code', StringType(), True),
        StructField('Actor1Name', StringType(), True),
        StructField('Actor1CountryCode', StringType(), True),
        StructField('Actor1KnownGroupCode', StringType(), True),
        StructField('Actor1EthnicCode', StringType(), True),
        StructField('Actor1Religion1Code', StringType(), True),
        StructField('Actor1Religion2Code', StringType(), True),
        StructField('Actor1Type1Code', StringType(), True),
        StructField('Actor1Type2Code', StringType(), True),
        StructField('Actor1Type3Code', StringType(), True),
        StructField('Actor2Code', StringType(), True),
        StructField('Actor2Name', StringType(), True),
        StructField('Actor2CountryCode', StringType(), True),
        StructField('Actor2KnownGroupCode', StringType(), True),
        StructField('Actor2EthnicCode', StringType(), True),
        StructField('Actor2Religion1Code', StringType(), True),
        StructField('Actor2Religion2Code', StringType(), True),
        StructField('Actor2Type1Code', StringType(), True),
        StructField('Actor2Type2Code', StringType(), True),
        StructField('Actor2Type3Code', StringType(), True),
        StructField('IsRootEvent', IntegerType(), True),
        StructField('EventCode', StringType(), True),
        StructField('EventBaseCode', StringType(), True),
        StructField('EventRootCode', StringType(), True),
        StructField('QuadClass', IntegerType(), True),
        StructField('GoldsteinScale', FloatType(), True),
        StructField('NumMentions', IntegerType(), True),
        StructField('NumSources', IntegerType(), True),
        StructField('NumArticles', IntegerType(), True),
        StructField('AvgTone', FloatType(), True),
        StructField('Actor1Geo_Type', IntegerType(), True),
        StructField('Actor1Geo_FullName', StringType(), True),
        StructField('Actor1Geo_CountryCode', StringType(), True),
        StructField('Actor1Geo_ADM1Code', StringType(), True),
        StructField('Actor1Geo_ADM2Code', StringType(), True),
        StructField('Actor1Geo_Lat', FloatType(), True),
        StructField('Actor1Geo_Long', FloatType(), True),
        StructField('Actor1Geo_FeatureID', StringType(), True),
        StructField('Actor2Geo_Type', IntegerType(), True),
        StructField('Actor2Geo_FullName', StringType(), True),
        StructField('Actor2Geo_CountryCode', StringType(), True),
        StructField('Actor2Geo_ADM1Code', StringType(), True),
        StructField('Actor2Geo_ADM2Code', StringType(), True),
        StructField('Actor2Geo_Lat', FloatType(), True),
        StructField('Actor2Geo_Long', FloatType(), True),
        StructField('Actor2Geo_FeatureID', StringType(), True),
        StructField('ActionGeo_Type', IntegerType(), True),
        StructField('ActionGeo_FullName', StringType(), True),
        StructField('ActionGeo_CountryCode', StringType(), True),
        StructField('ActionGeo_ADM1Code', StringType(), True),
        StructField('ActionGeo_ADM2Code', StringType(), True),
        StructField('ActionGeo_Lat', FloatType(), True),
        StructField('ActionGeo_Long', FloatType(), True),
        StructField('ActionGeo_FeatureID', StringType(), True),
        StructField('DATEADDED', StringType(), True),
        StructField('SOURCEURL', StringType(), True)
    ])

    events_schema2 = StructType([
        StructField('GLOBALEVENTID', IntegerType(), True),
        StructField('SQLDATE', IntegerType(), True),
        StructField('MonthYear', IntegerType(), True),
        StructField('Year', IntegerType(), True),
        StructField('FractionDate', FloatType(), True),
        StructField('Actor1Code', StringType(), True),
        StructField('Actor1Name', StringType(), True),
        StructField('Actor1CountryCode', StringType(), True),
        StructField('Actor1KnownGroupCode', StringType(), True),
        StructField('Actor1EthnicCode', StringType(), True),
        StructField('Actor1Religion1Code', StringType(), True),
        StructField('Actor1Religion2Code', StringType(), True),
        StructField('Actor1Type1Code', StringType(), True),
        StructField('Actor1Type2Code', StringType(), True),
        StructField('Actor1Type3Code', StringType(), True),
        StructField('Actor2Code', StringType(), True),
        StructField('Actor2Name', StringType(), True),
        StructField('Actor2CountryCode', StringType(), True),
        StructField('Actor2KnownGroupCode', StringType(), True),
        StructField('Actor2EthnicCode', StringType(), True),
        StructField('Actor2Religion1Code', StringType(), True),
        StructField('Actor2Religion2Code', StringType(), True),
        StructField('Actor2Type1Code', StringType(), True),
        StructField('Actor2Type2Code', StringType(), True),
        StructField('Actor2Type3Code', StringType(), True),
        StructField('IsRootEvent', IntegerType(), True),
        StructField('EventCode', StringType(), True),
        StructField('EventBaseCode', StringType(), True),
        StructField('EventRootCode', StringType(), True),
        StructField('QuadClass', IntegerType(), True),
        StructField('GoldsteinScale', FloatType(), True),
        StructField('NumMentions', IntegerType(), True),
        StructField('NumSources', IntegerType(), True),
        StructField('NumArticles', IntegerType(), True),
        StructField('AvgTone', FloatType(), True),
        StructField('Actor1Geo_Type', IntegerType(), True),
        StructField('Actor1Geo_FullName', StringType(), True),
        StructField('Actor1Geo_CountryCode', StringType(), True),
        StructField('Actor1Geo_ADM1Code', StringType(), True),
        StructField('Actor1Geo_ADM2Code', StringType(), True),
        StructField('Actor1Geo_Lat', FloatType(), True),
        StructField('Actor1Geo_Long', FloatType(), True),
        StructField('Actor1Geo_FeatureID', StringType(), True),
        StructField('Actor2Geo_Type', IntegerType(), True),
        StructField('Actor2Geo_FullName', StringType(), True),
        StructField('Actor2Geo_CountryCode', StringType(), True),
        StructField('Actor2Geo_ADM1Code', StringType(), True),
        StructField('Actor2Geo_ADM2Code', StringType(), True),
        StructField('Actor2Geo_Lat', FloatType(), True),
        StructField('Actor2Geo_Long', FloatType(), True),
        StructField('Actor2Geo_FeatureID', StringType(), True),
        StructField('ActionGeo_Type', IntegerType(), True),
        StructField('ActionGeo_FullName', StringType(), True),
        StructField('ActionGeo_CountryCode', StringType(), True),
        StructField('ActionGeo_ADM1Code', StringType(), True),
        StructField('ActionGeo_ADM2Code', StringType(), True),
        StructField('ActionGeo_Lat', FloatType(), True),
        StructField('ActionGeo_Long', FloatType(), True),
        StructField('ActionGeo_FeatureID', StringType(), True),
        StructField('DATEADDED', StringType(), True),
        StructField('SOURCEURL', StringType(), True),
        StructField('HashURL', StringType(), True)
    ])

    mentions_schema = StructType([
        StructField('GLOBALEVENTID', StringType(), True),
        StructField('EventTimeDate', StringType(), True),
        StructField('MentionTimeDate', StringType(), True),
        StructField('MentionType', StringType(), True),
        StructField('MentionSourceName', StringType(), True),
        StructField('MentionIdentifier', StringType(), True),
        StructField('SentenceID', StringType(), True),
        StructField('Actor1CharOffset', StringType(), True),
        StructField('Actor2CharOffset', StringType(), True),
        StructField('ActionCharOffset', StringType(), True),
        StructField('InRawText', StringType(), True),
        StructField('Confidence', StringType(), True),
        StructField('MentionDocLen', StringType(), True),
        StructField('MentionDocTone', StringType(), True),
        StructField('MentionDocTranslationInfo', StringType(), True),
        StructField('Extras', StringType(), True)
    ])

    news_schema = StructType([
        StructField('SOURCEURL', StringType(), True),
        StructField('HashURL', StringType(), True),
        StructField('SQLDATE', IntegerType(), True),
        StructField('NewsText', StringType(), True)
    ])

    return events_schema, mentions_schema, news_schema, events_schema2

def read_from_s3_enriched(session, location, schema, date):
    """
        Function to read data from S3 enriched data

        :param session: current Spark session
        :param name: name to be used while filtering the files in the S3 bucket
        :param schema: name of the schema to be used for this dataframe
        :param date: date to be processed
        :return: a dataframe with the S3 data for that specific subset of files (ie, mentions; events)
        """

    # to create the S3 stucture
    day = date[6:8]
    month = date[4:6]
    year = date[0:4]
    hour = date[8:10]
    minute = date[10:12]

    # creates the path for the output files
    loc = "s3a://{}/{}/{}/{}/{}/{}/{}/{}".format(bucket, prefixUpload, year, month, day, hour, minute, location)

    print("Reading enriched from path: ", loc)

    df = session.read.load(loc,
                           # reads all the files in the directory
                           format="csv",
                           sep=",",
                           quote='"',
                           escape = "\\",
                           multiLine = True,
                           header= True,
                           schema=schema)


    return df

def read_from_s3(session, name, schema, date):
    """
    Function to read data from S3

    :param session: current Spark session
    :param name: name to be used while filtering the files in the S3 bucket
    :param schema: name of the schema to be used for this dataframe
    :param date: date to be processed
    :return: a dataframe with the S3 data for that specific subset of files (ie, mentions; events)
    """

    # run history if needed
    if date=='all':
        date="*"

    s3filename = "s3a://{}/{}/{}.{}.CSV".format(bucket, prefix, date, name)

    print("Reading original S3 from path: ", s3filename)

    df = session.read.load(s3filename,
                           # reads all the files in the directory
                           format="csv",
                           sep="\t",
                           header="false",
                           schema=schema)

    return df

def uploadToS3(dataToWrite, location, date=None):
    """
    Function to upload data to S3.
    Ideally we would like to upload files in parquet format, but Quilt does not manage those kind of files, so we will have to use CSV

    :param dataToWrite: dataframe that is to be written
    :param location:  S3 bucket location to write the files
    :return: None
    """
    day = date[6:8]
    month = date[4:6]
    year = date[0:4]
    hour = date[8:10]
    minute = date[10:12]

    #creates the path for the output files
    loc = "s3a://{}/{}/{}/{}/{}/{}/{}/{}".format(bucket, prefixUpload, year, month, day, hour, minute, location)

    #TODO: even though the tool doesn't allow parquet in the current status, we'll upload data in that format to S3

    dataToWrite.write.csv(loc, header="true", mode="overwrite")

def getNews(link):
    """
    Function to get the news for a certain URL - using library newsplease

    :param link: the URL link for the news
    :return: the content of the news for the linl provided
    """
    try:
        article = NewsPlease.from_url(link)

        #we need to remove new lines and quotes, otherwise quilt will fail
        article_no_newlines = article.text.replace('\n', '')
        article_no_quotes = article_no_newlines.replace('"', "'")

        return article_no_quotes
    except:
        print("An exception occurred while scrapping the news:",link)
        pass

    return None

def uploadToQuilt(spark):

    # downloads the data from s3
    print("Getting schemas..")
    events_schema, mentions_schema, news_schema, events_schema2 = set_schemas()

    # remove old data and get new one
    quilt.rm("nmduarte/gdelt",force=True)
    quilt.install("nmduarte/gdelt",force=True)
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
    mentions_df = read_from_s3_enriched(spark, "mentions", mentions_schema, cmd_opts.date)

    events_df = read_from_s3_enriched(spark, "events", events_schema2, cmd_opts.date)

    news_df = read_from_s3_enriched(spark, "news", news_schema, cmd_opts.date)

    #concatenate already existing data with new data
    mentions_concat = mentions_from_quilt_df.union(mentions_df)
    events_concat = events_from_quilt_df.union(events_df)
    news_concat = news_from_quilt_df.union(news_df)

    # build the 3 packages
    quilt.build("nmduarte/gdelt/mentions",mentions_concat.toPandas())
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
    events_schema, mentions_schema, news_schema,events_schema2 = set_schemas()

    # mentions data
    print("Getting mention data..")
    mentions_df = read_from_s3(spark, "mentions", mentions_schema, cmd_opts.date)

    # events data
    print("Getting events data..")
    events_df = read_from_s3(spark, "export", events_schema, cmd_opts.date)

    print("Mentions has ", mentions_df.count(), "records")

    print("Events has ", events_df.count(), "records")

    # creates a UDF so we can add the data as a new column to the already existing dataframe
    news_udf = udf(lambda z: getNews(z),
                   StringType())

    # creates a UDF for the has
    hash_udf = udf(lambda z: hash(z), StringType())

    # adds hash column based on the sourceurl so that data can be joined afterwards
    events_df_hash = events_df.withColumn("HashURL", hash_udf(events_df.SOURCEURL))

    # events alias
    events = events_df_hash.alias("events")

    # there are several entries that can have the same URL and we don't want to scrape those, so we select the distinct values only
    distinct_news = events.select(events.SOURCEURL,
                                  events.HashURL, events.SQLDATE).distinct()

    print("There are ", distinct_news.count(), " distinct URLs")

    # gets the scrapped data and add it to the dataset
    distinct_news_with_data = distinct_news.withColumn("NewsText", news_udf(distinct_news.SOURCEURL))

    pan=distinct_news_with_data.toPandas()
    print(pan.head())

    print("Partitions: ", distinct_news_with_data.rdd.getNumPartitions())

    print("Loading to S3....")

    # write files to s3
    uploadToS3(distinct_news_with_data, "news", cmd_opts.date)
    uploadToS3(events_df_hash, "events", cmd_opts.date)
    uploadToS3(mentions_df, "mentions", cmd_opts.date)

def main():
    """

    :param cmd_opts: contains the date to be processed, sent by airflow
    :return:
    """

    # define session configurations and master
    spark = SparkSession.builder.appName("GDELT+").master(master).getOrCreate()  # create new spark session
    spark.conf.set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.conf.set("spark.speculation", "false")
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    spark.conf.set("spark.shuffle.service.enabled", "true")
    spark.conf.set("spark.deploy.speadOut","false")
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    if cmd_opts.action=="gather":
        do_crawling(spark)
    elif cmd_opts.action=="quilt":
        uploadToQuilt(spark)

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GDELT+')

    parser.add_argument('-d', '--date',
                        help='Datetime to be processed', required=True)

    parser.add_argument('-a', '--action',
                        help='Action to be processed', required=True)

    cmd_opts = parser.parse_args()

    if cmd_opts.action not in ['gather','quilt']:
        print("Please use one of the following actions - 'gather' or 'quilt'")
        sys.exit()

    main()

