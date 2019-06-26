from pyspark.sql import SparkSession
import argparse
import pyspark.sql.functions as f
import PostgreSQLConnector
import tools


bucket = "nmduartegdelt"
prefix = "test_small"
prefixUpload= "upload_small"
master = "spark://ec2-34-219-229-126.us-west-2.compute.amazonaws.com:7077"

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


    p = PostgreSQLConnector.dbConnection()

    a = p.read_data(spark, "cameo_categories")

    a.show()

    s3filename = "s3a://nmduartegdelt/data/*.export.CSV"

    print("Reading original S3 from path: ", s3filename)

    events_schema, mentions_schema, news_schema, events_schema2 = tools.set_schemas()

    df = spark.read.load(s3filename,
                           # reads all the files in the directory
                           format="csv",
                           sep="\t",
                           header="false",
                           schema=events_schema)

    a = df.groupBy("Actor1Code","MonthYear","Actor1CountryCode").count().select("Actor1Code", f.col('count').alias('Total'))

    df.registerTempTable("events")


    # most important events across the whole world
    mostImportantEvents = spark.sql("select Actor1Geo_Fullname, "
                                    "count(NumMentions) as tot "
                                    "from events "
                                    "group by Actor1Geo_Fullname "
                                    "sort by count(NumMentions) desc")


    # average tone for the top N elements by country
    #Actor1Geo_Fullname



    mostImportantEvents.show()

    a.show()

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GDELT+')

    parser.add_argument('-d', '--date',
                        help='Datetime to be processed', required=False)

    parser.add_argument('-a', '--action',
                        help='Action to be processed', required=False)

    cmd_opts = parser.parse_args()

    main()

