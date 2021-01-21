from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import logging

class WikipediaHandler:
    """Helper class for handling with wikipedia"""

    def __init__(self, wikipedia: DataFrame):
        """
        Init method
        :param wikipedia: dataframe
        """
        self.wikipedia = wikipedia
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _remove_prefix_title_logic():
        """Logic for replacing the wikipedia title field the prefix 'Wikipedia: '"""
        return F.regexp_replace('title','Wikipedia: ','')

    @staticmethod
    def _remove_suffix_title_logic():
        """Logic for removing from the title field suffix (only for explicit movies resources)"""
        return F.regexp_replace('title', '\((.*film)\)', '')

    @staticmethod
    def _film_flag_extractor_logic():
        """If the resource type contains the word 'film', True otherwise False"""
        return F.when(F.regexp_extract('title', '\((.*film)\)', 1) != '', True).otherwise(False)

    @staticmethod
    def _year_extractor_logic():
        """Logic for film year extraction"""
        return F.when(F.regexp_extract('title', '\((\d{4}) film\)', 1) != '',
                      F.regexp_extract('title', '\((\d{4}) film\)', 1).cast(IntegerType())).otherwise(None)

    @staticmethod
    def _clean_title_field(wikipedia: DataFrame) -> DataFrame:
        """
        First it extracts the (film) tag by creating the column 'movie_flag'
        Clean the title field by applying the following transformations:
            - remove prefix
            - remove suffix
            - remove punctuations and double spaces
            - trim and make title lower
        :param wikipedia: wikipedia dataframe
        :return: original dataframe with the title column cleaned and the new movie flag column
        """
        pattern = '[!":;,.-]'
        pattern_ds = '(  )'
        return wikipedia \
            .withColumn('movie_flag', WikipediaHandler._film_flag_extractor_logic())\
            .withColumn('title', WikipediaHandler._remove_prefix_title_logic())\
            .withColumn('title', WikipediaHandler._remove_suffix_title_logic()) \
            .withColumn('title', F.regexp_replace(F.col('title'), pattern, '')) \
            .withColumn('title', F.regexp_replace(F.col('title'), pattern_ds, ' ')) \
            .withColumn('title', F.trim(F.lower(F.col('title'))))

    @staticmethod
    def _film_year_field_extractor(wikipedia: DataFrame) -> DataFrame:
        """
        Get the film year. Sometimes the year is included inside the parenthesis
        e.g. (2003 film)
        :param wikipedia: wikipedia dataframe
        :return: original dataframe with the year column
        """
        return wikipedia.\
            withColumn('year',WikipediaHandler._year_extractor_logic())

    def wikipedia_matcher(self, clean_movies: DataFrame) -> DataFrame:
        """
        The match logic between movies and wikipedia datasets is composed by three steps:
        - join movies with the subset of the wikipedia dataset having both year and film tag populated
          (join key: title, year)
        - join the remaining movies with the subset of the wikipedia dataset having only film tag populated
          (join key: title)
        - join the remaining movies with the remaining subset of wikipedia
          (join key: title)

        :param clean_movies: movies metadata dataset to join with
        """

        clean_wiki = self.wikipedia

        trasformation_steps = [
            self._film_year_field_extractor,
            self._clean_title_field
        ]

        for step in trasformation_steps:
            clean_wiki = step(clean_wiki)

        # caching dataframes before joining

        clean_wiki = clean_wiki.select('title','movie_flag','year','url','abstract')

        self.logger.info("PRE-JOIN: Movies dataframe counts a total of %d rows", clean_movies.dropDuplicates(['id']).count())
        self.logger.info("PRE-JOIN: Wikipedia dataframe counts a total of %d rows", clean_wiki.count())

        # 1° STEP
        wiki_subset_1 = clean_wiki\
            .filter((F.col('movie_flag') == True) & (F.col('year').isNotNull()))
        joined_movies_1 = clean_movies\
            .join(wiki_subset_1,['title','year'],how='inner')\
            .dropDuplicates(['id'])
        self.logger.info("POST-JOIN-1: TrueFilm matched a total of %d rows", joined_movies_1.count())
        joined_movies_1.cache()

        # 2° STEP
        wiki_subset_2 = clean_wiki \
            .filter((F.col('movie_flag') == True) & (F.col('year').isNull()))
        joined_movies_2 = clean_movies\
            .join(joined_movies_1.select('id'),['id'],how='left_anti') \
            .join(wiki_subset_2.drop(F.col('year')), ['title'], how='inner')\
            .dropDuplicates(['id'])
        self.logger.info("POST-JOIN-2: TrueFilm matched a total of %d rows", joined_movies_2.count())
        joined_movies_2.cache()

        # 3° STEP
        wiki_subset_3 = clean_wiki \
            .filter((F.col('movie_flag') == False))
        joined_movies_3 = clean_movies \
            .join(joined_movies_1.select('id'), ['id'], how='left_anti') \
            .join(joined_movies_2.select('id'), ['id'], how='left_anti') \
            .join(wiki_subset_3.drop(F.col('year')), ['title'], how='inner') \
            .dropDuplicates(['id'])
        self.logger.info("POST-JOIN-3: TrueFilm matched a total of %d rows", joined_movies_3.count())
        joined_movies_3.cache()

        join = joined_movies_1.select('id','budget','revenue','ratio_revenue_budget','profit','year','url','production_companies','genres','abstract')\
            .union(joined_movies_2.select('id','budget','revenue','ratio_revenue_budget','profit','year','url','production_companies','genres','abstract'))\
            .union(joined_movies_3.select('id','budget','revenue','ratio_revenue_budget','profit','year','url','production_companies','genres','abstract'))


        join_anti = clean_movies.join(join, ['id'], how='left_anti')
        join_anti = join_anti.withColumn('url', F.lit(None))
        join_anti = join_anti.withColumn('abstract', F.lit(None))

        return join.select('id','budget','revenue',F.col('ratio_revenue_budget').alias('ratio'),'profit','year','url','production_companies','genres','abstract')\
            .union(join_anti.select('id','budget','revenue',F.col('ratio_revenue_budget').alias('ratio'),'profit','year','url','production_companies','genres','abstract'))