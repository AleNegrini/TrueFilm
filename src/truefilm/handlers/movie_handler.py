from pyspark.sql import DataFrame
import logging
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, StructField, ArrayType, StructType, StringType, IntegerType

class MovieHandler:
    """Helper class for handling with movies"""

    def __init__(self, movies: DataFrame):
        """
        Init method
        :param movies: dataframe
        """
        self.movies = movies
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _greater_than_X_logic(col: str, val: int):
        """Logic for selecting only the rows having 'col' value greater than a value passed via parameter
        :param col: col name to apply logic
        :param val: threshold value"""
        return F.col(col) >= val

    @staticmethod
    def _revenue_budget_ratio_logic():
        """Logic for revenue_budget_ratio column evaluation"""
        return F.when(F.col('revenue') > 0,
                      (F.col('budget')/F.col('revenue')).cast(DecimalType(15,4))).otherwise(None)

    @staticmethod
    def _profit_logic():
        """Logic for profit column evaluation"""
        return F.when(MovieHandler._revenue_budget_ratio_logic()>=1, False)\
            .otherwise(F.when(MovieHandler._revenue_budget_ratio_logic()<1, True)
                       .otherwise(None))

    @staticmethod
    def _get_year_logic():
        """Logic for getting year from the column release_date"""
        return F.year('release_date')

    @staticmethod
    def _get_revenue_budget_ratio(movies: DataFrame) -> DataFrame:
        """
        For each film, evaluate the ratio between the revenue and the budget
        :param movies: movies dataframe
        :return: dataframe containing an additional column containing the ratio between the revenue and
                 the budget column
        """
        return movies\
            .withColumn('ratio_revenue_budget', MovieHandler._revenue_budget_ratio_logic())\
            .withColumn('profit', MovieHandler._profit_logic())


    @staticmethod
    def _get_year_column(movies: DataFrame) -> DataFrame:
        """
        For each movie, get the year from the release_date column
        :param movies: movies dataframe
        :return: dataframe containing an additional column containing the release year
        """
        return movies\
            .withColumn('year', MovieHandler._get_year_logic())

    @staticmethod
    def _title_standardizer(movies: DataFrame) -> DataFrame:
        """
        For each movie, trim and lowerize the title
        :param movies: movies dataframe
        :return: dataframe with title trimmed and with small letters
        """
        return movies\
            .withColumn('title',F.trim(F.lower(F.col('title'))))

    @staticmethod
    def _fix_budget_column(movies: DataFrame) -> DataFrame:
        """
        Select only the rows having the the budget greater than a fixed value.
        Normally movies budget are in the order of the M$, therefore any lower value can be considered "dirty"
        :return: dataframe only with rows having budget greater than X (in this case 1000000)
        """
        return movies \
            .filter(MovieHandler._greater_than_X_logic('budget', 1000000))

    @staticmethod
    def _remove_punctuation_title(movies: DataFrame) -> DataFrame:
        """
        For each movie, remove the most common punctuation chars
        :param movies: movies dataframe
        :return: dataframe with movie title without punctuation
        """
        pattern = '[!":;,.-]'
        return movies.withColumn('title', F.regexp_replace(F.col('title'), pattern,''))

    @staticmethod
    def _remove_double_space_title(movies: DataFrame) -> DataFrame:
        """
        Removing punctuation character leads to possibile double blank chars generation.
        Therefore for each movie, replace them
        :param movies: movies dataframe
        :return: dataframe with movie title with a blank char only
        """
        pattern = '(  )'
        return movies.withColumn('title', F.regexp_replace(F.col('title'), pattern, ' '))

    @staticmethod
    def _parse_companies_col(movies: DataFrame) -> DataFrame:
        """
        Extract and parse the production companies associated with each movie
        :param movies: movies dataframe
        :return: dataframe with the content of column production_companies slightly modified containing the list of
        prod companies
        """
        production_company_schema = ArrayType(
            StructType([StructField('name', StringType(), False),
                        StructField('id', IntegerType(), False)]))

        return movies \
            .withColumn('production_companies', F.from_json(F.col('production_companies'), production_company_schema)) \
            .withColumn('production_companies', F.col('production_companies.name'))

    @staticmethod
    def _parse_genres_col(movies: DataFrame) -> DataFrame:
        """
        Extract and parse the genres associated with each movie
        :param movies: movies dataframe
        :return: dataframe with the content of column genres slightly modified containing the list of genres
        """
        genres_schema = ArrayType(
            StructType([StructField('name', StringType(), False),
                        StructField('id', IntegerType(), False)]))

        return movies \
            .withColumn('genres', F.from_json(F.col('genres'), genres_schema)) \
            .withColumn('genres', F.col('genres.name'))

    def top_movies(self, rows: int) -> DataFrame:
        """
        Returns the final movie dataframe:
            - apply a set of transformation steps
            - select the only useful columns
            - order in ascending order (since the ratio is budget/revenue,
                    the smaller the ratio is the higher the revenues are)
            - limit to #'rows' rows
        :param rows: number of rows the df should be limited
        :return: the final processed movies dataframe
        """

        final_movies = self.movies

        trasformation_steps = [
            self._fix_budget_column,
            self._title_standardizer,
            self._get_revenue_budget_ratio,
            self._get_year_column,
            self._remove_punctuation_title,
            self._remove_double_space_title,
            self._parse_companies_col,
            self._parse_genres_col
        ]

        for step in trasformation_steps:
            final_movies = step(final_movies)

        return final_movies\
            .select('id','title','budget','revenue', 'ratio_revenue_budget', 'profit', 'year','production_companies','genres')\
            .filter((F.col('budget') != 0) & (F.col('revenue') != 0))\
            .orderBy(F.col('ratio_revenue_budget'), ascending=True)\
            .limit(rows)