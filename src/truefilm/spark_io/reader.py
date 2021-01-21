from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import StructType
from typing import Optional
from typing import Dict
import logging

def read_csv_with_schema(spark: SparkSession,
                         path: str,
                         schema: StructType,
                         header: str,
                         delimiter: str,
                         quote: str = '\"',
                         escape: str = '\"',
                         encoding: str='utf-8') -> Optional[DataFrame]:
    """
    Read a CSV file in a Spark Dataframe, given the schema in input

    :param spark: Spark Session
    :param path: CSV file path
    :param schema: CSV schema
    :param header: "true" if the CSV has the header, "false" otherwise
    :param delimiter: csv delimiter char of set of characters
    :param quote: escape char
    :param escape: escape char
    :param encoding: encoding option
    :return the dataframe containing the csv file content (or None if some errors occurs during file read)
    """

    try:
        df = spark.read\
            .option("encoding", encoding)\
            .load(path,
                  format='csv',
                  sep=delimiter,
                  header=header,
                  schema=schema,
                  quote=quote,
                  escape=escape)
        logging.info("Dataframe has been correctly read from %s (CSV data format)", path)
        return df
    except Exception as error:
        logging.error("An error occurred while reading CSV dataframe: %s", error)
        return None

def read_csv_no_schema(spark: SparkSession,
                        path: str,
                        header: str,
                        delimiter: str,
                        quote: str = '\"',
                        escape: str = '\"',
                        encoding: str='utf-8') -> Optional[DataFrame]:
    """
    Read a CSV file in a Spark Dataframe, no schema given in input

    :param spark: Spark Session
    :param path: CSV file path
    :param header: "true" if the CSV has the header, "false" otherwise
    :param delimiter: csv delimiter char of set of characters
    :param quote: escape char
    :param escape: escape char
    :param encoding: encoding option
    :return the dataframe containing the csv file content (or None if some errors occurs during file read)
    """

    try:
        df = spark.read \
            .option("encoding", encoding) \
            .load(path,
                     format='csv',
                     sep=delimiter,
                     header=header,
                     quote=quote,
                     escape=escape)
        logging.info("Dataframe has been correctly read from %s (CSV data format)", path)
        return df
    except Exception as error:
        logging.error("An error occurred while reading CSV dataframe: %s", error)
        return None

def read_xml_with_schema(spark: SparkSession,
                         path: str,
                         schema: StructType,
                         row_tag: str,
                         encoding: str='utf-8') -> Optional[DataFrame]:
    """
    Read a XML file in a Spark Dataframe, with schema provided

    :param spark: Spark Session
    :param path: XML file path
    :param row_tag: tag of an xml row element
    :return the dataframe containing the xml file content (or None if some errors occurs during file read)
    """
    try:
        df = spark.read\
            .format("xml")\
            .option('rowTag',row_tag)\
            .option("encoding", encoding)\
            .schema(schema)\
            .load(path)
        logging.info("Dataframe has been correctly read from %s (XML data format)", path)
        return df
    except Exception as error:
        logging.error("An error occurred while reading XML dataframe: %s", error)
        return None

def read_xml_no_schema(spark: SparkSession,
                       path: str,
                       row_tag: str,
                       encoding: str='utf-8'):
    """
    Read a XML file in a Spark Dataframe, no schema provided

    :param spark: Spark Session
    :param path: XML file path
    :param row_tag: tag of an xml row element
    :param encoding: encoding option
    :return the dataframe containing the xml file content (or None if some errors occurs during file read)
    """
    try:
        df = spark.read\
            .format("xml")\
            .option('rowTag',row_tag)\
            .option("encoding", encoding)\
            .load(path)
        logging.info("Dataframe has been correctly read from %s (XML data format)", path)
        return df
    except Exception as error:
        logging.error("An error occurred while reading XML dataframe: %s", error)
        return None

def read_parquet(spark: SparkSession,
                 path: str,
                 encoding: str='utf-8'):
    """
    Read a set of parquet files in a Spark Dataframe, no schema provided

    :param spark: Spark Session
    :param path: Parquet file path
    :param encoding: encoding option
    :return the dataframe containing the parquet file content (or None if some errors occurs during file read)
    """
    try:
        df = spark.read\
            .option("encoding", encoding)\
            .parquet(path)
        logging.info("Dataframe has been correctly read from %s (PARQUET data format)", path)
        return df
    except Exception as error:
        logging.error("An error occurred while reading PARQUET dataframe: %s", error)
        return None


def read_jdbc(spark: SparkSession, url: str, table_name: str, user:str, pwd: str, driver: str) -> DataFrame:
    """
    Read dataframe from jdbc source
    :param spark: Spark Session
    :param url: jdbc url
    :param table_name: table to read
    :param user: username
    :param pwd: password
    :param drive: driver
    :return: read dataframe
    """
    logger = logging.getLogger(__name__)
    try:
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)
        df = sqlContext.read.format('jdbc').options(url=url,
                                                    dbtable=table_name,
                                                    driver=driver,
                                                    user=user,
                                                    password=pwd).load()
        logger.info("Dataframe has been successfully read from %s", table_name)
        return df
    except Exception as e:
        logger.error("An error occurred while reading dataframe file from %s: %s", table_name, e)