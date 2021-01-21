import unittest
from pyspark.sql.types import StructField, StructType, BooleanType, StringType, \
    FloatType, IntegerType, ArrayType, LongType, DateType
from src.truefilm.schema.movies_metadata import MoviesMetadata

class TestCompany(unittest.TestCase):
    """Test Company unittest"""

    def test_singleton_class(self):
        """
        Testing whether the singleton constructor works.
        Do the three objects point the same object?
        """
        mm_1 = MoviesMetadata()
        mm_2 = MoviesMetadata()
        mm_3 = MoviesMetadata()

        self.assertEqual(id(mm_1), id(mm_3))
        self.assertEqual(id(mm_2), id(mm_3))

    def test_company_schema(self):
        """Testing if the content of the Company object is the one expected"""
        mm_1 = MoviesMetadata()
        expected_schema = StructType([
            StructField('adult', BooleanType(), True),
            StructField('belongs_to_collection', StringType(), True),
            StructField('budget',FloatType(), True),
            StructField('genres', StringType(), True),
            StructField('homepage', StringType(), True),
            StructField('id', IntegerType(), True),
            StructField('imdb_id', StringType(), True),
            StructField('original_language', StringType(), True),
            StructField('original_title', StringType(), True),
            StructField('overview', StringType(), True),
            StructField('popularity', FloatType(), True),
            StructField('poster_path', StringType(), True),
            StructField('production_companies', StringType(), True),
            StructField('production_countries', StringType(), True),
            StructField('release_date', DateType(), True),
            StructField('revenue', LongType(), True),
            StructField('runtime', FloatType(), True),
            StructField('spoken_languages', StringType(), True),
            StructField('status', StringType(), True),
            StructField('tagline', StringType(), True),
            StructField('title', StringType(), True),
            StructField('video', BooleanType(), True),
            StructField('vote_average', FloatType(), True),
            StructField('vote_count', IntegerType(), True)
        ])

        self.assertEqual(expected_schema, mm_1.schema)
