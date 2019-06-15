from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType
from newsplease import NewsPlease

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
    StructField('DATEADDED', IntegerType(), True),
    StructField('SOURCEURL', StringType(), True)
])

mentions_schema = StructType([
    StructField('GLOBALEVENTID', IntegerType(), True),
    StructField('EventTimeDate', IntegerType(), True),
    StructField('MentionTimeDate', IntegerType(), True),
    StructField('MentionType', IntegerType(), True),
    StructField('MentionSourceName', StringType(), True),
    StructField('MentionIdentifier', StringType(), True),
    StructField('SentenceID', IntegerType(), True),
    StructField('Actor1CharOffset', IntegerType(), True),
    StructField('Actor2CharOffset', IntegerType(), True),
    StructField('ActionCharOffset', IntegerType(), True),
    StructField('InRawText', IntegerType(), True),
    StructField('Confidence', IntegerType(), True),
    StructField('MentionDocLen', IntegerType(), True),
    StructField('MentionDocTone', FloatType(), True),
    StructField('MentionDocTranslationInfo', StringType(), True),
    StructField('Extras', StringType(), True)
])


def main():

    # create new spark session
    spark = SparkSession.builder.appName("GDELT+").getOrCreate()

    # mentions dataframe
    mentions_df = spark.read.load("s3a://nmduartegdelt/data/20190102000000.mentions.CSV",
                                  format="csv",
                                  sep="\t",
                                  header="false",
                                  schema=mentions_schema)

    # events dataframe
    events_df = spark.read.load("s3a://nmduartegdelt/data/20190102000000.export.CSV",
                                format="csv",
                                sep="\t",
                                header="false",
                                schema=events_schema)


    # creates a UDF so we can add the data as a new column to the already existing dataframe
    news_udf = udf(lambda z: getNews(z), StringType())

    # events
    events=events_df.alias("events")
    #a=events.limit(20).select(events.GLOBALEVENTID, events.Actor1Code, events.SOURCEURL, news_udf(events.SOURCEURL).alias("NewsText"))

    # there are several entries that can have the same URL and we don't want to scrape those, so we select the distinct values only
    distinct_news = events.select(events.SOURCEURL).distinct()

    # gets the scrapped data and add it to the dataset
    distinct_news_with_data = distinct_news.select(distinct_news.SOURCEURL,news_udf(distinct_news.SOURCEURL).alias("NewsText"))

    # uploads the data to S3 using parquet format
    distinct_news_with_data.write.parquet("s3a://nmduartegdelt/news_data","overwrite")
    # c=a.withColumn("NEWSDATA","safdfdsfdsfds")

    # a.write.csv("newsdata")

    '''
    # mentions
    mentions = mentions_df.alias("mentions")
    mentions.limit(10).select(mentions.GLOBALEVENTID).show()

    b=mentions.limit(20).select(mentions.GLOBALEVENTID, mentions.MentionSourceName, mentions.GLOBALEVENTID.alias("m"))


    
    # sample join
    join = a.join(b, "GLOBALEVENTID", "left")  #.groupBy(events.Actor1Code).agg({"*": "count"})
    join.show()

    article = NewsPlease.from_url(
        'https://dupagepolicyjournal.com/stories/512586642-illinois-general-assembly-hr243-resolution-adopted')
    bbb = article.title
    print(bbb)
    '''

    spark.stop()

def getNews(link):
    """
    Function to get the news for a certain URL - using library newsplease

    :param link: the URL link for the news
    :return: the content of the news for the linl provided
    """
    try:
        article = NewsPlease.from_url(link)
        return article.title
    except:
        print("An exception occurred.")
        pass

    return None


if __name__ == "__main__":
    main()

