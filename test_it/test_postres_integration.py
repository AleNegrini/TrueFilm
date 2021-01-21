import unittest
import os
from dotenv import load_dotenv

from src.truefilm.utils.spark import Spark
from src.truefilm.spark_io.writer import jdbc_writer
from src.truefilm.spark_io.reader import read_jdbc
from src.truefilm.handlers.movie_handler import MovieHandler
from src.truefilm.run import jdbc_url_builder


class TestPostreSQL(unittest.TestCase):

    def test_write_to_postresql(self):
        TEST_PS_HOST = '127.0.0.1'
        TEST_PS_PORT = '5433'

        sparkSession = Spark(master="local",
                              app_name="TrueFilmTest1",
                              log_level="WARN",
                              xml_jar_path=os.getcwd() + ('/resources/jar/spark-xml_2.11-0.5.0.jar'),
                              jdbc_jar_path=os.getcwd() + ('/resources/jar/postgresql-42.2.18.jar'))

        input_data_movies = [('Silicon graphics image', '1999-01-01', 1, 1550000, 2000000, '{}', '{}'),
                             ('Harry Potter : il calice di fuoco', '2008-01-01', 4, 4000000, 2000000, '{}', '{}'),
                             ('THE hobbit!', '2016-01-01', 7, 1000000, 2000000, '{}', '{}'),
                             ('New film', '2010-01-01', 18, 1550000, 4000000, '{}', '{}'),
                             ('Top perf', '2010-01-01', 19, 100, 4000000, '{}', '{}')]

        schema_movies = ['title', 'release_date', 'id', 'budget', 'revenue', 'production_companies', 'genres']

        movies = MovieHandler(movies=sparkSession.spark.createDataFrame(data=input_data_movies, schema=schema_movies))
        load_dotenv(os.getcwd()+'/database_dev.env')
        test_properties = {"user": os.environ['POSTGRES_USER'],
                           "password": os.environ['POSTGRES_PASSWORD'],
                           "driver": "org.postgresql.Driver"}
        top_movies = movies.top_movies(3)
        url = jdbc_url_builder(host=TEST_PS_HOST, port=TEST_PS_PORT, db=os.environ['POSTGRES_DB'])
        jdbc_writer(df=top_movies,
                    table_name='test_movies',
                    url=url,
                    properties=test_properties)

        df = read_jdbc(spark=sparkSession.spark,
                       table_name='test_movies',
                       url=url,
                       driver=test_properties['driver'],
                       user=test_properties['user'],
                       pwd=test_properties['password'])

        self.assertListEqual(df.orderBy('id').collect(), top_movies.orderBy('id').collect())
