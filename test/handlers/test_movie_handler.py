import unittest
import logging
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, \
    FloatType, BooleanType, DecimalType, DateType, ArrayType, StringType
from pyspark.sql import SparkSession

from src.truefilm.handlers.movie_handler import MovieHandler

class MovieProcessorTest(unittest.TestCase):

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

    def test_get_revenue_budget_ratio(self):

        self._set_up()

        input_data = [(1,500,200.0),
                      (2,0,200.0),
                      (3,0,0.0),
                      (4,1000,0.0),
                      (5,None,34.0),
                      (6,None,None),
                      (7,34,None),
                      (8,10,20.0)]

        expected_data = [(1, 500, 200.0, Decimal(0.4), True),
                         (2, 0, 200.0, None, None),
                         (3, 0, 0.0, None, None),
                         (4, 1000, 0.0, Decimal(0), True),
                         (5, None, 34.0, None, None),
                         (6, None, None, None, None),
                         (7, 34, None, None, None),
                         (8, 10, 20.0, Decimal(2), False)]

        input_schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('revenue', LongType(), True),
            StructField('budget', FloatType(), True)
        ])
        expected_schema = StructType([
            StructField('id',IntegerType(),True),
            StructField('revenue', LongType(), True),
            StructField('budget', FloatType(), True),
            StructField('ratio_revenue_budget', DecimalType(15,4), True),
            StructField('profit', BooleanType(), True)
        ])

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._get_revenue_budget_ratio(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_year_revenue_budget_ratio(self):

        self._set_up()

        input_data = [(1,datetime.strptime('2019-12-01','%Y-%m-%d')),
                      (2,datetime.strptime('2020-12-01','%Y-%m-%d'))]

        expected_data = [(1,datetime.strptime('2019-12-01','%Y-%m-%d'),2019),
                         (2,datetime.strptime('2020-12-01','%Y-%m-%d'), 2020)]

        input_schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('release_date', DateType(), True)
        ])
        expected_schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('release_date', DateType(), True),
            StructField('year', IntegerType(), True)
        ])

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._get_year_column(input_df)


        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_title_standardizer(self):

        self._set_up()

        input_data = [('Silicon Graphics Image', 1),
                      ('Relative Values', 4),
                      ('Robin Hood', 7)]

        expected_data = [('silicon graphics image', 1),
                         ('relative values', 4),
                         ('robin hood', 7)]

        input_schema = ['title','id']
        expected_schema = ['title','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._title_standardizer(input_df)


        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_remove_punctuation_title(self):

        self._set_up()

        input_data = [('Silicon: Graphics - Image', 1),
                      ('Relative - Values', 4),
                      ('Robin Hood!', 7)]

        expected_data = [('Silicon Graphics  Image', 1),
                         ('Relative  Values', 4),
                         ('Robin Hood', 7)]

        input_schema = ['title','id']
        expected_schema = ['title','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._remove_punctuation_title(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_remove_double_space_title(self):

        self._set_up()

        input_data = [('Silicon Graphics  Image', 1),
                      ('Relative  Values', 4),
                      ('Robin Hood', 7)]

        expected_data = [('Silicon Graphics Image', 1),
                         ('Relative Values', 4),
                         ('Robin Hood', 7)]

        input_schema = ['title','id']
        expected_schema = ['title','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._remove_double_space_title(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_fix_budget_column(self):

        self._set_up()

        input_data = [(1, 1),
                      (2000000, 5),
                      (1000000, 4),
                      (999999, 7)]

        expected_data = [(2000000, 5), (1000000, 4)]

        input_schema = ['budget','id']
        expected_schema = ['budget','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._fix_budget_column(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_parse_companies_col(self):

        self._set_up()

        input_data = [("[{'id': 16, 'name': 'AAA'}, {'id': 35, 'name': 'BBB'}, {'id': 10751, 'name': 'CCCC'}]", 1),
                      ("[{'id': 110, 'name': 'DDDD'}]", 5),
                      (None, 4)]

        expected_data = [(['AAA','BBB','CCCC'], 1),
                         (['DDDD'], 5),
                         (None, 4)]

        input_schema = ['production_companies','id']
        expected_schema = ['production_companies','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._parse_companies_col(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_genres(self):

        self._set_up()

        input_data = [("[{'id': 16, 'name': 'HORROR'}, {'id': 35, 'name': 'FANTASY'}, {'id': 10751, 'name': 'THRILLER'}]", 1),
                      ("[{'id': 110, 'name': 'THRILLER'}]", 5),
                      (None, 4)]

        expected_data = [(['HORROR','FANTASY','THRILLER'], 1),
                         (['THRILLER'], 5),
                         (None, 4)]

        input_schema = ['genres','id']
        expected_schema = ['genres','id']

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = MovieHandler._parse_genres_col(input_df)

        self.assertTrue(expected_df.schema == output_df.schema)
        self.assertListEqual(expected_df.orderBy('id').collect(), output_df.orderBy('id').collect())

        self._tear_down()

    def test_top_movies(self):
        self._set_up()
        input_data_movies = [('Silicon graphics image', '1999-01-01', 1, 1550000, 2000000, '{}', '{}'),
                             ('Harry Potter : il calice di fuoco', '2008-01-01', 4, 4000000, 2000000, '{}', '{}'),
                             ('THE hobbit!', '2016-01-01', 7, 1000000, 2000000, '{}', '{}'),
                             ('New film', '2010-01-01', 18, 1550000, 4000000, '{}', '{}'),
                             ('Top perf', '2010-01-01', 19, 100, 4000000, '{}', '{}')]

        expected_movies = [(18, 'new film', 1550000, 4000000, Decimal(0.3875), True, 2010, [None], [None]),
                           (7, 'the hobbit', 1000000, 2000000, Decimal(0.5000), True, 2016, [None], [None]),
                           (1, 'silicon graphics image', 1550000, 2000000, Decimal(0.7750), True, 1999, [None], [None])]

        schema_movies = ['title', 'release_date', 'id', 'budget', 'revenue', 'production_companies', 'genres']

        schema_expected = StructType([
            StructField('id', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('budget', LongType(), True),
            StructField('revenue', LongType(), True),
            StructField('ratio_revenue_budget', DecimalType(15, 4), True),
            StructField('profit', BooleanType(), True),
            StructField('year', IntegerType(), True),
            StructField('production_companies', ArrayType(StringType()), True),
            StructField('genres', ArrayType(StringType()), True)
        ])

        movies = MovieHandler(movies=self.spark.createDataFrame(data=input_data_movies, schema=schema_movies))
        top_movies = movies.top_movies(5)

        exp_df = self.spark.createDataFrame(data=expected_movies, schema=schema_expected)

        self.assertListEqual(top_movies.orderBy('id').collect(), exp_df.orderBy('id').collect())

        self._tear_down()