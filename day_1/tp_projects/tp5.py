from pyspark.ml.feature import StringIndexer
from configparser import ConfigParser
import logging
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split
import numpy as np

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    config = SparkConf() \
        .setAppName("tp1" + str(time.time()))
    
    session = SparkSession.builder \
            .config(conf=config) \
            .getOrCreate()
    
    session.sparkContext.setLogLevel("OFF")

    df_noms = session.createDataFrame([("Alfa",), ("Bravo",), ("Charlie",), ("Delta",), \
                                        ('Echo',), ('Foxtrot',), ('Golf',), ('Hotel',), \
                                        ('India',), ('Juliet',), ('Kilo',), ('Lima',), \
                                        ('Mike',), ('November',), ('Oscar',), ('Papa',), \
                                        ('Quebec',), ('Romeo',), ('Sierra',), ('Tango',), \
                                        ('Uniform',), ('Victor',), ('Whiskey',), ('X-ray',), \
                                        ('Yankee',), ('Zulu',)], ["nom"])
    
    indexer = StringIndexer(inputCol="nom", outputCol="nom_index")
    model_indexer = indexer.fit(df_noms)
    df_final = model_indexer.transform(df_noms)
    df_final.show()