import logging 
import time
from pyspark import SparkConf, SparkContext
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("tp1 started")
    # 1. creation de la commande de lancement de l'applciation 
    config = SparkConf() \
        .setAppName("tp1"+str(time.time())) \
        .setMaster("local[*]")

    # 2. creation du contexte spark
    context = SparkContext(conf=config)
    context.setLogLevel("WARN")

    # 3. creation du RDD
    data = [('Alpha', 34), ('Beta', 1), ('Charlie', 23), ('Delta', 100)]
    rdd = context.parallelize(data)

    # 4. transformation du RDD 
    logging.info("nb alt = "+str(rdd.count()))
    context.stop()
    logging.info("tp1 ended")