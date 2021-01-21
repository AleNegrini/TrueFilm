import unittest
from src.truefilm.utils.spark import Spark
from src.truefilm.utils.config import Config
from src.truefilm.spark_io.reader import read_csv_with_schema
from src.truefilm.schema.movies_metadata import MoviesMetadata

class TestSparkReader(unittest.TestCase):
    """Test SparkReader unittest"""

    def test_csv_df_schema(self):
        spark = Spark("local", "test").spark
        config = Config(['test/test_resources'])

        paths = config.raw_resources_check(['movies_metadata_test.csv'])
        schema = MoviesMetadata().schema
        output_df = read_csv_with_schema(spark=spark,
                             path=paths['movies_metadata_test.csv'],
                             schema=schema,
                             header='true',
                             delimiter=',')

        print(output_df.schema())


        spark.stop_spark()

    def test_spark_session_instance_type(self):
        spark = Spark("local", "test")

        spark.stop_spark()


