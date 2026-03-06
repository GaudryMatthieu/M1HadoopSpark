import logging 
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc


logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("popurcentage licence started")
    config = SparkConf() \
        .setAppName("pourcentage_licence"+str(time.time())) \
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .setMaster("local[*]")
    
    session = SparkSession.builder \
        .config(conf=config) \
        .getOrCreate()
    
    session.sparkContext.setLogLevel("WARN")

    sportDF = session.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("encoding", "UTF-8") \
        .option("inferSchema", "true") \
        .option("locale", "fr_FR") \
        .csv("hdfs://namenode:9000/lic-data-2023.csv")
    
    # Rechercher la distribution des licencies en foot par departement 
    sportDF.createOrReplaceTempView("sports")
    resultSQL = session.sql(""" 
        SELECT Departement, COUNT(*)
""")
 