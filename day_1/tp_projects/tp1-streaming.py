from configparser import ConfigParser
import logging
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split



logging.basicConfig(level=logging.INFO)

# Config parser
config_parser = ConfigParser()
config_parser.read("tp1-stream.ini")

if __name__ == '__main__':
    logging.info("tp1 started")
    # Création du SparkConf : configuration de la commande de lancement
    config = SparkConf() \
        .setAppName("tp1" + str(time.time())) \
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        #.setMaster("local[*]")
    
    # Création de la session SparkSQL
    session = SparkSession.builder \
            .config(conf=config) \
            .config("spark.mongodb.write.connection.uri", config_parser["output"]["uri"]) \
            .getOrCreate()
    
    session.sparkContext.setLogLevel("OFF")
    # Création de notre table infinie

    lines = session.readStream \
        .format(config_parser["input"]["format"]) \
        .option("subscribe", config_parser["input"]["subscribe"]) \
        .option("kafka.bootstrap.servers", config_parser["input"]["kafka.bootstrap.servers"]) \
        .option("startingOffsets", "earliest") \
        .load()

    # Compter les mots
    words = lines.select(explode(split(col("value").cast("string"), " ")).alias("word"))    
    counter = words.groupBy("word").count()

    # Afficher dans la console
    query = counter.writeStream \
    .outputMode("complete") \
    .format(config_parser["output"]["format"]) \
    .option("checkpointLocation", "/tmp/pyspark/checkpoint") \
    .option("database", config_parser["output"]["database"]) \
    .option("collection", config_parser["output"]["collection"]) \
    .start()

    counter.writeStream.outputMode("complete").format("console").start()
    # Démarrer la boucle d'évènements
    query.awaitTermination(timeout = 100)
    query.stop()