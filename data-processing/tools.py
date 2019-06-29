import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType

sys.path.append(os.path.expanduser('~/insight/code/config/'))
import config

params = config.config("gdelt_app")

def spark_session():
    """
    Creates new spark session
    :return: spark session
    """
    spark = SparkSession.builder.appName("GDELT+").master(params['master']).getOrCreate()  # create new spark session
    spark.conf.set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.conf.set("spark.speculation", "false")
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    spark.conf.set("spark.deploy.speadOut", "false")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    return spark

def read_from_s3_enriched(session, location, schema, date):
    """
        Function to read data from S3 enriched data
        :param session: current Spark session
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
    loc = "s3a://{}/{}/{}/{}/{}/{}/{}/{}".format(params['bucket'], params['prefix_upload'], year, month, day, hour, minute, location)

    logging.info("Reading enriched from path: ", loc)

    df = session.read.load(loc,
                           # reads all the files in the directory
                           format="csv",
                           sep=",",
                           quote='"',
                           escape="\\",
                           multiLine=True,
                           header=True,
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
    if date == 'all':
        date = "*"

    # csv can be in UPCASE for gkg
    if name == "gkg":
        csv_name="csv"
    else:
        csv_name = "CSV"

    s3filename = "s3a://{}/{}/{}.{}.{}".format(params['bucket'], params['prefix'], date, name, csv_name)

    logging.info("Reading original S3 from path: ", s3filename)

    df = session.read.load(s3filename,
                           # reads all the files in the directory
                           format="csv",
                           sep="\t",
                           header="false",
                           schema=schema)

    return df


def upload_to_s3(data_to_write, location, date=None):
    """
    Function to upload data to S3.
    Ideally we would like to upload files in parquet format, but Quilt does not manage those kind of files, so we will
    have to use CSV
    :param data_to_write: dataframe that is to be written
    :param location:  S3 bucket location to write the files
    :param date:  Date of the file to be uploaded
    :return: None
    """
    day = date[6:8]
    month = date[4:6]
    year = date[0:4]
    hour = date[8:10]
    minute = date[10:12]

    # creates the path for the output files
    loc = "s3a://{}/{}/{}/{}/{}/{}/{}/{}".format(params['bucket'], params['prefix_upload'], year, month, day, hour, minute, location)

    # dataToWrite.write.mode("overwrite").parquet(loc)

    data_to_write.write.csv(loc, header="true", mode="overwrite")

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

    gkg_schema = StructType([
        StructField('GKGRECORDID', StringType(), True),
        StructField('V2_1DATE', StringType(), True),
        StructField('V2SOURCECOLLECTIONIDENTIFIER', StringType(), True),
        StructField('V2SOURCECOMMONNAME', StringType(), True),
        StructField('V2DOCUMENTIDENTIFIER', StringType(), True),
        StructField('V1COUNTS', StringType(), True),
        StructField('V2_1COUNTS', StringType(), True),
        StructField('V1THEMES', StringType(), True),
        StructField('V2ENHANCEDTHEMES', StringType(), True),
        StructField('V1LOCATIONS', StringType(), True),
        StructField('V2ENHANCEDLOCATIONS', StringType(), True),
        StructField('V1PERSONS', StringType(), True),
        StructField('V2ENHANCEDPERSONS', StringType(), True),
        StructField('V1ORGANIZATIONS', StringType(), True),
        StructField('V2ENHANCEDORGANIZATIONS', StringType(), True),
        StructField('V1_5TONE', StringType(), True),

        StructField('V2_1ENHANCEDDATES', StringType(), True),
        StructField('V2GCAM', StringType(), True),
        StructField('V2_1SHARINGIMAGE', StringType(), True),
        StructField('V2_1RELATEDIMAGES', StringType(), True),
        StructField('V2_1SOCIALIMAGEEMBEDS', StringType(), True),

        StructField('V2_1SOCIALVIDEOEMBEDS', StringType(), True),
        StructField('V2_1QUOTATIONS.', StringType(), True),
        StructField('V2_1ALLNAMES', StringType(), True),
        StructField('V2_1AMOUNTS', StringType(), True),
        StructField('V2_1TRANSLATIONINFO', StringType(), True),
        StructField('V2EXTRASXML', StringType(), True)
    ])

    return events_schema, mentions_schema, news_schema, events_schema2, gkg_schema

def set_upload_schemas():
    news_schema = StructType([
        StructField('Name', StringType(), True),
        StructField('ID', StringType(), True)
    ])

    return news_schema