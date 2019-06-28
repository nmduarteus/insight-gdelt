import pyspark.sql.functions as f
import tools
from config_spark_dbconn import SparkPostgreConn
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession

master = "spark://ec2-34-219-229-126.us-west-2.compute.amazonaws.com:7077"
spark_input_file = "/home/ubuntu/insight/spark.txt"
date_words="201906260200"

def cleanWords():
    news_clean = news.where(news["NewsText"].isNotNull()).withColumn('NewsTextClean',
                                                                     f.regexp_replace('NewsText', "[^0-9a-zA-Z., ]",
                                                                                      " "))

    news_clean_split = news_clean.select(f.split(news_clean["NewsTextClean"], " ").alias("NewsTextClean"))

    remover = StopWordsRemover(inputCol="NewsTextClean", outputCol="NoStopWords")
    news_without_stopwords = remover.transform(news_clean_split)

    news_without_stopwords_str = news_without_stopwords.withColumn('NoStopWordsStr', f.concat_ws(' ', 'NoStopWords'))
    news_to_show = news_without_stopwords_str.withColumn('word', f.explode(f.col('NoStopWords'))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False) \
        .where('word <>""')

    return news_to_show


def spark_session():
    """
    Creates new spark session
    :return: spark session
    """
    spark = SparkSession.builder.appName("GDELT+").master(master).getOrCreate()  # create new spark session
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

a = SparkPostgreConn("~/PycharmProjects/insight-gdelt/database.ini","gdelt")

spark = spark_session()
sc = tools.set_upload_schemas()

events_schema, mentions_schema, news_schema, events_schema2, gkg_schema = tools.set_schemas()

events = tools.read_from_s3(spark, "export", events_schema, "all")
mentions = tools.read_from_s3(spark, "mentions", mentions_schema, "all")
#gkg = tools.read_from_s3(spark, "gkg", mentions_schema, "all")
news = tools.read_from_s3_enriched(spark, "news", news_schema, date_words)
events.registerTempTable("events")
mentions.registerTempTable("mentions")
news.registerTempTable("news")
news_to_show=cleanWords()

eventsquery = spark.sql("""select
	a.country_code,
	a.total_events + b.total_events as total
	from
(select
	Actor1CountryCode as country_code,
	count(*) as total_events
from events
where Actor1CountryCode is not null
group by Actor1CountryCode) a inner join
(select
	Actor2CountryCode as country_code,
	count(*) as total_events
from events
where Actor2CountryCode is not null
group by Actor2CountryCode) b
on a.country_code = b.country_code
order by a.total_events + b.total_events desc""")

mostmentions = spark.sql("""select
	events.GLOBALEVENTID,
	count(mentions.GLOBALEVENTID) as total,
	events.MonthYear as year_month,
	events.AvgTone as avg_tone,
	events.EventCode as event_code
from events
left join mentions on events.GLOBALEVENTID=mentions.GLOBALEVENTID
group by
	events.GLOBALEVENTID,
	events.MonthYear,
	events.AvgTone,
	events.EventCode
order by count(mentions.GLOBALEVENTID) desc""")

top_channels = spark.sql("""select MentionSourceName as mention_source_name, count(MentionSourceName) as total from mentions group by MentionSourceName order by count(*) desc limit 10""")



# remove rows with newstext missing

#news_to_show = news_without_stopwords_str.withColumn("list_words",f.split(f.col('NoStopWordsStr')," "))

a.write(df=mostmentions, table="top_mentions",md="overwrite")
a.write(df=eventsquery, table="top_events",md="overwrite")
a.write(df=top_channels, table="top_channels", md="overwrite")
a.write(df=news_to_show, table="news_to_show", md="overwrite")


spark.stop()


