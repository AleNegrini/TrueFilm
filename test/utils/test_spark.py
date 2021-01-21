import unittest
from pyspark.sql import SparkSession
from src.truefilm.utils.spark import Spark
import os

class TestSpark(unittest.TestCase):

    def test_spark_configurations(self):
        sparkSession1 = Spark(master="local",
                              app_name="TrueFilmTest1",
                              log_level="WARN",
                              xml_jar_path=os.getcwd()+('/resources/jar/spark-xml_2.11-0.5.0.jar'),
                              jdbc_jar_path=os.getcwd()+('/resources/jar/postgresql-42.2.18.jar'))

        configurations1 = sparkSession1.spark.sparkContext.getConf().getAll()

        for conf in configurations1:
            if(conf[0] == 'spark.master'):
                self.assertEqual(conf[1],'local')
            if (conf[0] == 'spark.app.name'):
                self.assertEqual(conf[1], 'TrueFilmTest1')

        sparkSession1.stop_spark()

    def test_spark_session_instance_type(self):
        sparkSession = Spark(master="local",
                              app_name="TrueFilmTest",
                              log_level="WARN",
                              xml_jar_path=os.getcwd()+('/resources/jar/spark-xml_2.11-0.5.0.jar'),
                              jdbc_jar_path=os.getcwd()+('/resources/jar/postgresql-42.2.18.jar'))
        self.assertIsInstance(sparkSession.spark, SparkSession)
        sparkSession.stop_spark()


