from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType
from newsplease import NewsPlease
from newspaper import Article
import pandas as pd
import traceback
import argparse
import quilt



bucket = "nmduartegdelt"
prefix = "test_small"
prefixUpload= "upload_small"


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
        """
        first_article = Article(url=link)
        first_article.download()
        first_article.parse()
        text=first_article.text
        """
        article = NewsPlease.from_url(link)

        #we need to remove new lines and quotes, otherwise quilt will fail
        article_no_newlines = article.text.replace('\n', '')
        article_no_quotes = article_no_newlines.replace('"', "'")
        #article = NewsPlease.from_url(link)
        #return article.text

        return article_no_quotes
    except:
        print("An exception occurred while scrapping the news:",link)
        # traceback.print_exc()
        pass

    return None

def uploadToQuilt(spark):
    #downloads the data from s3
    print("Getting schemas..")
    events_schema, mentions_schema, news_schema,events_schema2 = set_schemas()

    # quilt.install("nmduarte/gdelt3")

    # mentions data
    print("Getting mention data..")
    mentions_df = read_from_s3_enriched(spark, "mentions", mentions_schema, cmd_opts.date)
    mentions_df.show()
    mentions_df.write.csv("tmp_data/mentions", header="true", mode="overwrite")

    events_df = read_from_s3_enriched(spark, "events", events_schema2, cmd_opts.date)
    events_df.write.csv("tmp_data/events", header="true", mode="overwrite")

    news_df = read_from_s3_enriched(spark, "news", news_schema, cmd_opts.date)
    news_df.write.csv("tmp_data/news", header="true", mode="overwrite")

    #news_df.write.csv("hdfs://10.0.0.13/ubuntu/hdfs/data/example.csv")

    #news_df.show()

    #quilt.build("nmduarte/gdelt8_news")
    #from quilt.data.nmduarte import gdelt8_news
    #news1 = pd.read_csv("tmp_data/news/part-00000-0f8595b0-2bd0-4156-9254-78e7b5cfa5c9-c000.csv", engine='python', escapechar="\\")
    #gdelt8_news._set(['bar'], news1)
    #print(gdelt8_news.bar())
    #quilt.push("nmduarte/gdelt8_news", is_public=True)

    #quilt.build("nmduarte/gdelt_news","tmp_data/news")
    # put some data in it
    #from quilt.data.nmduarte import gdelt9_news
    #df = pd.DataFrame(data=[1, 2, 3])
    #gdelt9_news._set(['bar'], df)
    #print(gdelt9_news.bar())
    #quilt.push("nmduarte/gdelt_news", is_public=True)

    #print(news1.head())

    # build the 3 packages
    quilt.build("nmduarte/gdelt_mentions","tmp_data/mentions")
    quilt.build("nmduarte/gdelt_events", "tmp_data/events")
    quilt.build("nmduarte/gdelt_news", "tmp_data/news")

    # push the 3 packages
    quilt.push("nmduarte/gdelt_mentions", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt_events", is_public=True, is_team=False)
    quilt.push("nmduarte/gdelt_news", is_public=True, is_team=False)



def addtoQuilt(df_new, name):
    from quilt.data.nmduarte import gdelt3

    if name=="data_with_news":
        d = gdelt3.data.data_with_news()
    else:
        d = gdelt3.data.events()

    #print("Original data has :", original_data.count())

    df_new2= df_new.toPandas()

    print("Appending:", df_new2.count())

    print("original: ",type(d))
    print("new: ", type(df_new2))

    d = d.append(df_new2)
    print("TTOAL:", d.count())

    # gdelt3._set(["data","data_with_news"],df)

    # data_with_news['new_column'] = "aaaaaa"
    # data_with_news['new_column2'] = "bbbbb"

    quilt.build("nmduarte/gdelt3/data/"+name, d)
    quilt.push("nmduarte/gdelt3/data/"+name, is_public=True, is_team=False)

def do_crawling(spark):
    """
    Function used to enrich the data with the crawled data
    :param spark: Context
    :return:
    """

    print("Getting schemas..")
    events_schema, mentions_schema, news_schema,events_schema2 = set_schemas()

    # quilt.install("nmduarte/gdelt3")

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
    distinct_news_with_data = distinct_news.limit(10).withColumn("NewsText", news_udf(distinct_news.SOURCEURL))
    # distinct_news_with_data = distinct_news.withColumn("NewsText",lit("viagem de carro de Lucknow, capital do estado de Uttar Pradesh, para a ensurdecedora cidade de Shahjahanpur demora cerca de quatro horas. É difícil andar pelas ruas estreitas, cheias de bicicletas e vendedores de rua. Ouve-se constantemente um concerto de buzinas. Longe das ruas principais, há uma praça rodeada por pequenas casas. Entre elas, uma casa de dois quartos está escondida dos transeuntes por uma parede verde de três metros e umas portas de ferro azuis. Por trás delas, o mistério do que aconteceu ao jornalista freelancer indiano Jagendra Singh há quatro anos ainda não foi resolvido. A 1 de junho de 2015 Singh estava à espera de uma visita, mas não sabia exatamente o que esperar. Escrevia há semanas sobre o alegado envolvimento do político local Rammurti Singh Verma na extração ilegal de areia. Tinha chegado a altura de se reunirem. No entanto, no início da tarde, a polícia apareceu em casa de Singh. A família Singh diz que veio acompanhada por apoiantes de Verma. Pouco depois, Singh chegou ao hospital em agonia com queimaduras em 50% do seu corpo.“Porque é que me mataram?” disse num vídeo gravado no corredor do hospital local, para onde foi levado de urgência. “Os filhos da puta despejaram gasolina por cima de mim. Saltaram o muro e entraram. Se quisessem, podiam-me ter prendido.” Com os olhos fechados e sem conseguir olhar para a câmara, acusou os agentes da polícia e os apoiantes de Verma de lhe pegarem fogo. No vídeo, podemos ver as terríveis queimaduras. Morreu dos ferimentos, sete dias mais tarde. Tinha 46 anos.")).repartition(100)

    pan=distinct_news_with_data.toPandas()
    print(pan.head())

    print("Partitions: ", distinct_news_with_data.rdd.getNumPartitions())
    # distinct_news_with_data.show()

    # distinct_news_with_data.show()

    # events.select("SQLDATE").show()

    # print("Joining data......")
    # print(events_df_hash.count())

    # joined_data= events_df_hash.join(distinct_news_with_data, on="SOURCEURL", how="left")
    # joined_data.show()
    # joined_data= events_df_hash.join(distinct_news_with_data, Seq("HashURL"),"left")
    # select(events_df_hash("*"),distinct_news_with_data("NewsText"))

    print("Loading to S3....")

    # write files to s3
    uploadToS3(distinct_news_with_data, "news", cmd_opts.date)
    uploadToS3(events_df_hash, "events", cmd_opts.date)
    uploadToS3(mentions_df, "mentions", cmd_opts.date)
    # print("Loading data into s3.....")
    # uploadToS3(joined_data, "joined")

    # e = events.select(events.GLOBALEVENTID,events.SQLDATE,events.MonthYear,events.Year,events.FractionDate,events.Actor1Code,events.Actor1Name,events.Actor1CountryCode,events.Actor1KnownGroupCode,events.Actor1EthnicCode,events.Actor1Religion1Code,events.Actor1Religion2Code,events.Actor1Type1Code,events.Actor1Type2Code,events.Actor1Type3Code,events.Actor2Code,events.Actor2Name,events.Actor2CountryCode,events.Actor2KnownGroupCode,events.Actor2EthnicCode,events.Actor2Religion1Code,events.Actor2Religion2Code,events.Actor2Type1Code,events.Actor2Type2Code,events.Actor2Type3Code,events.IsRootEvent,events.EventCode,events.EventBaseCode,events.EventRootCode,events.QuadClass,events.GoldsteinScale,events.NumMentions,events.NumSources,events.NumArticles,events.AvgTone,events.Actor1Geo_Type,events.Actor1Geo_FullName,events.Actor1Geo_CountryCode,events.Actor1Geo_ADM1Code,events.Actor1Geo_ADM2Code,events.Actor1Geo_Lat,events.Actor1Geo_Long,events.Actor1Geo_FeatureID,events.Actor2Geo_Type,events.Actor2Geo_FullName,events.Actor2Geo_CountryCode,events.Actor2Geo_ADM1Code,events.Actor2Geo_ADM2Code,events.Actor2Geo_Lat,events.Actor2Geo_Long,events.Actor2Geo_FeatureID,events.ActionGeo_Type,events.ActionGeo_FullName,events.ActionGeo_CountryCode,events.ActionGeo_ADM1Code,events.ActionGeo_ADM2Code,events.ActionGeo_Lat,events.ActionGeo_Long,events.ActionGeo_FeatureID,events.DATEADDED,events.SOURCEURL)
    # events_df.repartition(1).write.csv("/home/ubuntu/ccc.csv")
    # events_df_hash.show()

    # addtoQuilt(distinct_news_with_data,"data_with_news")
    # addtoQuilt(events_df,"events")

    # quilt.build("nmduarte/gdelt3/data/data_with_news", d)
    # quilt.push("nmduarte/gdelt3/data/data_with_news", is_public=True, is_team=False)



def main():
    """

    :param cmd_opts: contains the date to be processed, sent by airflow
    :return:
    """
    # first configs are to speed up writ to s3
    # last congigures are to
    spark = SparkSession.builder\
        .appName("GDELT+") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")\
        .config("spark.hadoop.fs.s3a.fast.upload", "true")\
        .config("spark.sql.parquet.filterPushdown", "true")\
        .config("spark.sql.parquet.mergeSchema", "false")\
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
        .config("spark.speculation", "false") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.deploy.speadOut","false") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()  # create new spark session

    """ 
    .config("spark.dynamicAllocation.enabled", "true") \
        
    Enable Arrow-based columnar data transfers - to convert to pandas if needed for quilt"""

    #do_crawling(spark)
    uploadToQuilt(spark)

    #article = NewsPlease.from_url("https://au.news.yahoo.com/toronto-boss-exposed-sexist-shame-woman-linkedin-profile-picture-030435716.html?guccounter=1")
    #a = article.text.replace('"', r'\"')
    #print(article.text)

    #d = {'col1': [1, article.text], 'col2': [3, 4]}
    #df = pd.DataFrame(data=d)
    #print(df)
    #finalArticle = article.text.replace('\n', '')
    #l = [(finalArticle , "999999999999999999999")]
    #z= spark.createDataFrame(l)
    #z.show()
    #print(z.count())
    #z.coalesce(1).write.csv("text3.csv")

    #readit = spark.read.csv("text3.csv",sep=',', escape="\\", multiLine=True)

    #readit.show()
    #readit.select("_c0").show()
    #print(readit.count())

    # schemas to be used by the data
    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GDELT+')

    parser.add_argument('-d', '--date',
                        help='Datetime to be processed', required=True)

    cmd_opts = parser.parse_args()
    main()

