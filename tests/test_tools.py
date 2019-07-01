import os
import sys
import unittest

from pyspark.sql import SparkSession

sys.path.append(os.path.expanduser('~/insight/code/data_processing'))
print(sys.path)
import tools



class TestTools_functions(unittest.TestCase):

    def test_create_spark_session(self):
        spark = tools.spark_session()
        self.assertIsInstance(spark,SparkSession)

    def test_read_enriched_data(self):
        spark = tools.spark_session()
        schemas = tools.set_schemas()
        date_words="201906260200"
        news = tools.read_from_s3_enriched(spark, "news", schemas['news'], date_words)
        self.assertIsNotNone(news)