import logging
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import col, split, desc, asc, avg, count

# On retire ici une partie des messages 
logging.basicConfig(level=logging.INFO)

# On fait référence à notre fichier de configuration
config_parser = ConfigParser()
config_parser.read("eval.ini")

if __name__ == '__main__':
    # Création du SparkConf : configuration de la commande de lancement
    config = SparkConf() \
        .setAppName("eval" + str(time.time()))
    
    # Création de la session SparkSQL
    session = SparkSession.builder \
            .config(conf=config) \
            .getOrCreate()
    session.sparkContext.setLogLevel("OFF")

    # Création de notre table infinie
    input_df = session.readStream \
            .format(config_parser["input"]["format"]) \
            .option("subscribe", config_parser["input"]["subscribe"]) \
            .option("kafka.bootstrap.servers", config_parser["input"]["kafka.bootstrap.servers"]) \
            .option("startingOffsets", "earliest") \
            .load()
    
    # On récupère les données de Kafka sur un dataframe pour les traiter
    df = input_df.selectExpr("CAST(value AS STRING)") \
            .select(split(col('value'), ';').getItem(0).alias("timestamp"), \
                    split(col('value'), ';').getItem(1).alias("scooter_id"), \
                    split(col('value'), ';').getItem(2).alias("zone_id"), \
                    split(col('value'), ';').getItem(3).cast("float").alias("battery_level")) \
                    .dropna()
    
    # Ici on fait notre travail sur les colonnes de notre topic pour grouper les données par zone
    # Puis on utilise la fonction agg pour faire en simultané la moyenne de batterie et le nombre de trottinette par zone
    resultDF = df.groupBy("zone_id") \
        .agg(
            avg("battery_level").alias("battery_level"),
            count("scooter_id").alias("nb_scooter")
        ) \
        .orderBy(desc("battery_level"), asc("zone_id"))

    # On envoie les données vers MongoDB 
    query = resultDF.select("zone_id", "battery_level", "nb_scooter") \
        .writeStream \
        .outputMode("complete") \
        .format(config_parser["output"]["format"]) \
        .option("checkpointLocation", "/tmp/checkpoint_eval_final") \
        .option("connection.uri", config_parser["output"]["uri"]) \
        .option("database", config_parser["output"]["database"]) \
        .option("collection", config_parser["output"]["collection"]) \
        .start()

    # Démarrer la boucle d'évènements 
    query.awaitTermination(timeout = 2*60)
    query.stop()