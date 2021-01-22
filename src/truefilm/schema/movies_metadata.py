from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, BooleanType, StringType, \
    FloatType, IntegerType, LongType, DateType

from src.truefilm.utils.singleton_meta import SingletonMeta

@dataclass
class MoviesMetadata(metaclass=SingletonMeta):
    """
        Singleton class that contains the MoviesMetadata StructType
    """
    schema = StructType([
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
