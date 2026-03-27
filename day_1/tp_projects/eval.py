import logging
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import col, split
from pyspark.sql.functions import desc, asc

logging.basicConfig(level=logging.INFO)

config_parser = ConfigParser()
config_parser.read("eval.ini")

if __name__ == '__main__':
    config = SparkConf() \
        .setAppName("eval" + str(time.time()))
    
    session = SparkSession.builder \
            .config(conf=config) \
            .getOrCreate()
    
    session.sparkContext.setLogLevel("OFF")

    input_df = session.readStream \
            .format(config_parser["input"]["format"]) \
            .option("subscribe", config_parser["input"]["subscribe"]) \
            .option("kafka.bootstrap.servers", config_parser["input"]["kafka.bootstrap.servers"]) \
            .option("startingOffsets", "earliest") \
            .load()
    
    df = input_df.selectExpr("CAST(value AS STRING)") \
            .select(split(col('value'), ';').getItem(0).alias("timestamp"), \
                    split(col('value'), ';').getItem(1).alias("scooter_id"), \
                    split(col('value'), ';').getItem(2).alias("zone_id"), \
                    split(col('value'), ';').getItem(3).cast("float").alias("battery_level")) \
                    .dropna()
    
    resultDF = df.select("timestamp", "scooter_id", "zone_id", "battery_level") \
        .groupBy("zone_id") \
        .mean("battery_level") \
        .withColumnRenamed("avg(battery_level)", "battery_level") \
        .orderBy(desc("battery_level"), asc("zone_id"))


    query = resultDF.select("zone_id", "battery_level") \
        .writeStream \
        .outputMode("complete") \
        .format(config_parser["output"]["format"]) \
        .option("checkpointLocation", "/tmp/checkpoint_eval_test") \
        .option("connection.uri", config_parser["output"]["uri"]) \
        .option("database", config_parser["output"]["database"]) \
        .option("collection", config_parser["output"]["collection"]) \
        .start()

    logging.info("Résultat SQL enregistré dans la base Mongo")

    query.awaitTermination(timeout = 2*60)
    query.stop()