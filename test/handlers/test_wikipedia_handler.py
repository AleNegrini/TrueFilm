import unittest
import logging
from pyspark.sql import SparkSession

from src.truefilm.handlers.wikipedia_handler import WikipediaHandler

class WikipediaProcessorTest(unittest.TestCase):

    def _suppress_py4j_logging(self) -> None:
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    def _create_testing_pyspark_session(self) -> SparkSession:
        return SparkSession.builder\
            .master('local')\
            .appName('pyspark-test') \
            .getOrCreate()

    def _set_up(self):
        self._suppress_py4j_logging()
        self.spark = self._create_testing_pyspark_session()

    def _tear_down(self) -> None:
        self.spark.stop()

    def test_clean_title_field(self):

        self._set_up()

        input_data = [('Wikipedia: Silicon Graphics Image',1),
                      ('Wikipedia: Aaron Hicklin',2),
                      ('Wikipedia: Duan Chengshi',3),
                      ('Wikipedia: Relative Values (film)',4),
                      ('Wikipedia: Dark Alliance (book)',5),
                      ('Wikipedia: The Food of the Gods (short story)',6),
                      ('Wikipedia: Robin Hood (2003 film)',7)]

        expected_data = [('silicon graphics image',1,False),
                         ('aaron hicklin',2,False),
                         ('duan chengshi',3,False),
                         ('relative values',4,True),
                         ('dark alliance (book)',5,False),
                         ('the food of the gods (short story)',6,False),
                         ('robin hood',7, True)]

        input_schema = ['title','id']
        expected_schema = ['title','id','movie_flag']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = WikipediaHandler._clean_title_field(wikipedia=input_df)

        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_film_year_field_extractor(self):

        self._set_up()

        input_data = [('Wikipedia: Silicon Graphics Image',1),
                      ('Wikipedia: Relative Values (film)',4),
                      ('Wikipedia: Robin Hood (2003 film)',7)]

        expected_data = [('Wikipedia: Silicon Graphics Image',1,None),
                         ('Wikipedia: Relative Values (film)',4,None),
                         ('Wikipedia: Robin Hood (2003 film)',7, 2003)]

        input_schema = ['title','id']
        expected_schema = ['title','id','year']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = WikipediaHandler._film_year_field_extractor(wikipedia=input_df)

        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_wikipedia_matcher(self):

        self._set_up()

        input_data_movies = [('silicon graphics image',1999,1,2000000,2000000,1,False,'{}','{}'),
                             ('harry potter il calice di fuoco',2008,4,2000000,2000000,1,False,'{}','{}'),
                             ('the hobbit',2016,7,2000000,2000000,1,False,'{}','{}'),
                             ('new film',2010,18,2000000,2000000,1,False,'{}','{}')]

        input_data_wikipedia = [('silicon graphics image',True, 1999, 1, 'http://1'),
                                ('Harry Potter: il calice di fuoco',True,None,4, 'http://4'),
                                ('THE hobbit',False,None, 7, 'http://7'),
                                ('dr. who',False,None, 10, 'http://10'),
                                ('aaaaaa',True,1989, 11, 'http://11')]

        schema_movies = ['title','year','id','budget','revenue','ratio_revenue_budget','profit','production_companies','genres']
        schema_wikipedia = ['title', 'flag_movie','year','id','url']

        wiki_df = WikipediaHandler(self.spark.createDataFrame(data=input_data_wikipedia, schema=schema_wikipedia))
        movie = self.spark.createDataFrame(data=input_data_movies, schema=schema_movies)

        output_df = wiki_df.wikipedia_matcher(movie)

        expected = [(1, 2000000, 2000000, 1, False, 1999, 'http://1', '{}', '{}'),
                       (4, 2000000, 2000000, 1, False, 2008, 'http://4', '{}', '{}'),
                       (7, 2000000, 2000000, 1, False, 2016, 'http://7', '{}', '{}'),
                       (18, 2000000, 2000000, 1, False, 2010, None, '{}', '{}')]

        schema_expected = ['id', 'budget', 'revenue', 'ratio_revenue_budget', 'profit', 'year',
                           'url', 'production_companies', 'genres']

        expected_df = self.spark.createDataFrame(data=expected, schema=schema_expected)

        self.assertListEqual(output_df.orderBy('id').collect(), expected_df.orderBy('id').collect())