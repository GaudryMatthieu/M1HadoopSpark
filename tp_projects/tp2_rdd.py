import logging 
import time
from pyspark import SparkConf, SparkContext
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("tp2 started")
    # 1. creation de la commande de lancement de l'applciation 
    config = SparkConf() \
        .setAppName("tp1"+str(time.time())) \
        .setMaster("local[*]")

    # 2. creation du contexte spark
    context = SparkContext(conf=config)
    context.setLogLevel("WARN")

    # 3. creation du RDD
    messages = context.textFile("logs.txt")
    errors = messages.filter(lambda msg: "ERROR" in msg) \
        .map(lambda msg: (msg.split(":")[3], 1)) \
        .reduceByKey(lambda a, b: a + b)

    for error in errors.collect():
        logging.info("error"+str(error))

    context.stop()
    logging.info("tp2 ended")