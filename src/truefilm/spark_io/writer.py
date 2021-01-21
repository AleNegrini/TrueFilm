from pyspark.sql import DataFrame
from typing import Dict
import logging

def parquet_writer(df: DataFrame, path: str, dataframe_name: str, encoding: str = 'utf-8') -> None:
    """
    Save the dataframe into parquet file
    :param df: Dataframe to save
    :param path: path to which save the df
    :param dataframe_name: dataframe name
    :param encoding: encoding options
    :return None
    """
    logger = logging.getLogger(__name__)
    try:
        df.repartition(10).write\
            .mode('overwrite') \
            .option("encoding", encoding)\
            .parquet(path)
        logger.info("%s has been successfully saved to %s", dataframe_name, path)
    except Exception as e:
        logger.error("An error occurred while saving parquet file %s to %s", e, path)


def csv_writer(df: DataFrame, path: str, sep: str, header: str, encoding: str = 'utf-8'):
    """
    Save the dataframe into csv file
    :param df: Dataframe to save
    :param path: path to which save the df
    :param sep: csv delimiter
    :param header: 'true' if the header should be included, 'false' otherwise
    :param encoding: encoding options
    :return None
    """
    logger = logging.getLogger(__name__)
    try:
        df.repartition(1).write \
            .mode('overwrite') \
            .option("encoding", encoding)\
            .option("sep", sep)\
            .option("header", header)\
            .csv(path)
        logger.info("Dataframe has been successfully saved to %s", path)
    except Exception as e:
        logger.error("An error occurred while saving csv file to %s: %s", path, e)

def jdbc_writer(df: DataFrame, table_name: str, url: str, properties: Dict[str,str], mode: str = 'overwrite'):
    """
    Save the dataframe into postgres table
    :param df: Dataframe to save
    :param table_name: postgres table
    :param url: postres url
    :param mode: df write mode
    :param properties: properties for postgres (user, pwd, driver)
    :return None
    """
    logger = logging.getLogger(__name__)
    try:
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
        logger.info("Dataframe has been successfully saved to %s", table_name)
    except Exception as e:
        logger.error("An error occurred while saving dataframe file to %s: %s", table_name, e)