import logging 
import time
import sys
from pyspark import SparkConf, SparkContext
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logging.error("Syntaxe error : word_count <input_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[0]
    output_dir = sys.argv[1]

    # 1. creation de la commande de lancement de l'applciation 
    config = SparkConf() \
        .setAppName("anagramme"+str(time.time())) \
        .setMaster("local[*]")

    # 2. creation du contexte spark
    context = SparkContext(conf=config)
    context.setLogLevel("WARN")

    # 3. creation du RDD
    words = context.textFile("liste_mots_francais.txt")

    word_sorted = words.map(lambda word: ("".join(sorted(word)), word)) \
        .groupByKey() \
        .mapValues(list) \
        .filter(lambda x: len(x[1]) > 1) \
        .sortBy(lambda x: len(x[1]), ascending=False)


    for word in word_sorted.take(200):
        logging.info(word)

    word_sorted.coalesce(1) \
        .saveAsTextFile(output_dir)

    context.stop()
    logging.info("Ended")