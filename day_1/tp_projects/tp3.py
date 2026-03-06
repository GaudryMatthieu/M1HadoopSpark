import logging 
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import desc, asc


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

    shema = StructType([ StructField("transaction_id", IntegerType(), True),
                         StructField("vendeur_id", IntegerType(), True),
                         StructField("vendeur_nom", StringType(), True), 
                         StructField("montant", DoubleType(), True), 
                         StructField("date", DateType(), True) 
                    ])

    sportDF = session.read.csv("hdfs://namenode:9000/ventes.csv", header=True, schema=shema)
    
    sportDF.printSchema()

    # Rechercher les fédérations sportives les plus représentées

    resultDF = sportDF.select("vendeur_id", "vendeur_nom", "montant") \
        .groupBy("vendeur_id", "vendeur_nom") \
        .sum("montant") \
        .withColumnRenamed("sum(montant)", "montant") \
        .orderBy(desc("montant"), asc("vendeur_id"))

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
        .option("uri", "mongodb://mongodb:27017/ventes") \
        .option("database", "ventes") \
        .option("collection", "ventes") \
        .mode("overwrite") \
        .save()

    logging.info("Résultat SQL enregistré dans la base Mongo")
    session.stop()
    logging.info("testsql ended")

