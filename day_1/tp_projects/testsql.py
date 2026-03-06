import logging 
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("testsql started")
    config = SparkConf() \
        .setAppName("testsql"+str(time.time())) \
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
    
    sportDF.printSchema()

    # Rechercher les fédérations sportives les plus représentées 
    sportDF.createOrReplaceTempView("sports")
    resultDF = session.sql(""" 
        SELECT Code, `Fédération`, SUM(Total) AS Total
        FROM sports
        GROUP BY Code, `Fédération`
        ORDER BY Total DESC
    """)

    # resultDF.coalesce(1).write \
    #     .option("header", "true") \
    #     .option("delimiter", ";") \
    #     .option("encoding", "UTF-8") \
    #     .option("locale", "fr_FR") \
    #     .mode("overwrite") \
    #     .csv("hdfs://namenode:9000/sqlresult")

    # Ajouter le fichier dans ma base Mongo
    resultDF.write \
        .format("com.mongodb.spark.sql") \
        .option("uri", "mongodb://mongodb:27017/hexagonesports") \
        .option("database", "hexagonesports") \
        .option("collection", "sports") \
        .mode("overwrite") \
        .save()

    logging.info("Résultat SQL enregistré dans la base Mongo")
    session.stop()
    logging.info("testsql ended")

