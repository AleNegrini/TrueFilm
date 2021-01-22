import argparse
import logging
from typing import Dict, List
import os
from dotenv import load_dotenv
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from src.truefilm.utils.spark import Spark
from src.truefilm.spark_io.reader import read_csv_with_schema, read_xml_no_schema, read_parquet
from src.truefilm.spark_io.writer import parquet_writer, jdbc_writer
from src.truefilm.schema.movies_metadata import MoviesMetadata
from src.truefilm.utils.config import Config
from src.truefilm.handlers.movie_handler import MovieHandler
from src.truefilm.handlers.wikipedia_handler import WikipediaHandler


def setup_logging(log_level: int) -> None:
    """
    Set log format and verbosity
    :param log_level: custom verbosity log (0->NOT SET, 10->DEBUG, 20->INFO, 30->WARNING, 40->ERROR, 50->CRITICAL)
    :return: None
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=log_level, format=log_format)
    logger = logging.getLogger(__name__)

    logger.info("Initializing TrueFilm logging using verbosity level: %s", log_level)


def init() -> argparse.Namespace:
    """
    Inizialing the job arguments
    :return Namespace containing argument variables
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spark_master', type=str, default='local[*]', help='Spark running mode')
    parser.add_argument('spark_app_name', type=str, default='TrueFilm-job', help='Spark Application name')
    parser.add_argument('log_level', type=int, default=20, help='Log verbosity level')
    parser.add_argument('movies_path', type=str, default='movies_metadata.csv',
                        help='Path for movies metadata dataset')
    parser.add_argument('wikipedia_path', type=str, default='enwiki-latest-abstract.xml',
                        help='Path for wikipedia dataset')

    args = parser.parse_args()

    setup_logging(args.log_level)

    return args


def init_env(list:List[str]) -> Dict[str, str]:
    """
    Getting the environment vars
    :param list: list of environment variables
    :return key-value with env var name and value
    """
    vars = {}
    logger = logging.getLogger(__name__)
    for item in list:
        try:
            vars[item] = os.environ[item]
            logger.info("Detected environment variabile %s with value %s", item, os.environ[item])
        except KeyError:
            logger.error("%s environment variable not found!")
            raise KeyError

    return vars

def jdbc_url_builder(host: str, port: str, db: str):
    """
    Builds the postresql jdbc URL
    :param host: postresql hostname
    :param port: postresql port
    :param db: db_name
    :return: postresql jdbc URL
    """
    return 'jdbc:postgresql://'+host+':'+port+'/'+db

if __name__ == '__main__':

    # init
    args = init()
    env_vars = init_env(['POSTGRES_HOST','POSTGRES_PORT'])

    spark_session = Spark(master=args.spark_master,
                          app_name=args.spark_app_name,
                          xml_jar_path=os.getcwd()+('/resources/jar/spark-xml_2.11-0.5.0.jar'),
                          jdbc_jar_path=os.getcwd()+('/resources/jar/postgresql-42.2.18.jar'),
                          log_level='WARN')

    final_resources_path = Config().final_resources_check([args.movies_path, args.wikipedia_path])
    raw_resources_path = Config().raw_resources_check([args.movies_path+'.csv', args.wikipedia_path+'.xml'])


    ######################################################################################################
    # THIS STEP WAS NECESSARY IN ORDER TO REDUCE DATAFRAME SIZE (ESPECIALLY THE WIKIPEDIA ONE), SINCE    #
    # ONLY THE USEFUL COLUMNS WERE KEPT (WIKI: FROM 6GB OF AN UNCOMPRESSED XML FILE TO 650MB IN PARQUET) #
    # THIS STEP TAKES A FEW MINUTES, BUT RUNS JUST THE FIRST TIME                                        #
    ######################################################################################################

    if(final_resources_path[args.movies_path] is None):
        logging.info("Detected a RAW version of movies dataset")
        logging.info("Starting the conversion to parquet format: it could take some minutes")
        raw_movies = read_csv_with_schema(spark=spark_session.spark,
                                          path=raw_resources_path[args.movies_path+'.csv'],
                                          schema=MoviesMetadata().schema,
                                          header='true',
                                          quote='\"',
                                          delimiter=',',
                                          escape='\"')
        # save back df in parquet format
        parquet_writer(df=raw_movies, path=raw_resources_path[args.movies_path+'.csv']
                       .replace('.csv','')
                       .replace('raw_ok','final_ok'),
                       dataframe_name='movies')
    else:
        logging.info("Detected a CLEANED version of movies dataset")

    if (final_resources_path[args.wikipedia_path] is None):
        logging.info("Detected a RAW version of Wikipedia dataset")
        logging.info("Starting the conversion to parquet format: it could take some minutes")
        raw_wikipedia = read_xml_no_schema(spark=spark_session.spark,
                                           path=raw_resources_path[args.wikipedia_path+'.xml'],
                                           row_tag='doc')
        # save back the useful columns in parquet
        parquet_writer(df=raw_wikipedia.select('title','url','abstract'),
                       path=raw_resources_path[args.wikipedia_path + '.xml']
                       .replace('.xml', '')
                       .replace('raw_ok', 'final_ok'),
                       dataframe_name='wikipedia')
    else:
        logging.info("Detected a CLEANED version of Wikipedia dataset")

    resources_path = Config().final_resources_check([args.movies_path, args.wikipedia_path])

    # In order to increase the performances, the wikipedia dataset has been cleaned by removing the useless columns
    movies = read_parquet(spark=spark_session.spark,
                          path=resources_path[args.movies_path])
    wikipedia = read_parquet(spark=spark_session.spark,
                             path=resources_path[args.wikipedia_path])

    movie_processor = MovieHandler(movies)
    wikipedia_processor = WikipediaHandler(wikipedia)

    movies_1k_top = movie_processor.top_movies(rows=1000)
    movies_1k_top.cache()

    movies_1k_top_enriched = wikipedia_processor.wikipedia_matcher(movies_1k_top)

    # join with the original movies dataframe, in order to get the original film titles
    movies_1k_top_enriched = movies_1k_top_enriched\
        .join(movies.select('id','title'), ['id'], how='inner')\
        .withColumn('application',F.lit('truefilm'))
    windowSpec = Window.partitionBy('application').orderBy('ratio')
    movies_1k_top_enriched = movies_1k_top_enriched\
        .withColumn('rating', F.row_number().over(windowSpec))\
        .select('id',
               'title',
               'budget',
               'rating',
               'revenue',
               'ratio',
               'profit',
               'year',
               'url',
               'production_companies',
               'genres',
               'abstract')

    a = load_dotenv(os.getcwd()+'/database.env')
    url = jdbc_url_builder(host=env_vars['POSTGRES_HOST'],port=env_vars['POSTGRES_PORT'],db=os.environ['POSTGRES_DB'])
    properties = {"user": os.environ['POSTGRES_USER'],
                  "password": os.environ['POSTGRES_PASSWORD'],
                  "driver": "org.postgresql.Driver"}
    jdbc_writer(df=movies_1k_top_enriched, table_name='movies',url=url, properties=properties)

    spark_session.stop_spark()