from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

from src.truefilm.utils.singleton_meta import SingletonMeta

@dataclass()
class Wikipedia(metaclass=SingletonMeta):
    """
        Singleton class that contains the Wikipedia StructType
    """
    schema = StructType([
        StructField('abstract', StringType(), True),
        StructField('links', StructType(
            ArrayType(
                StructType([
                    StructField('_linktype', StringType(), True),
                    StructField('anchor', StringType(), True),
                    StructField('link', StringType(), True)
                ])
            )
        ), True),
        StructField('title', StringType(), True),
        StructField('url', StringType(), True)
    ])
