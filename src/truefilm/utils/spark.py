from pyspark.sql import SparkSession
import logging

class Spark():

    def __init__(self, master: str, app_name:str, xml_jar_path: str, jdbc_jar_path: str, log_level: str='INFO') -> None:
        """
        Init method
        :param master: spark running mode
        :param app_name: spark app name
        :param log_level: spark logging level
        :param xml_jar_path: path where is located the jar for xml parsing
        :param jdbc_jar_path: path where is located the postgres jdbc jar
        :return: None
        """
        self.spark = SparkSession.builder\
            .master(master)\
            .appName(app_name) \
            .config("spark.jars", xml_jar_path+','+jdbc_jar_path)\
            .getOrCreate()

        self.spark.sparkContext.setLogLevel(log_level)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Spark Session has been successfully inizialized with log level %s", log_level)

    def stop_spark(self) -> None:
        """
        It stops the object spark session
        :return: None
        """
        self.spark.stop()
        self.logger.info("Spark Session has been successfully stopped")
