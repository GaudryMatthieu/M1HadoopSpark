import logging 
import time
import sys
from pyspark import SparkConf, SparkContext

logging.basicConfig(level=logging.INFO)

def checkNoEmptyItems(item):
    return len(item) > 0

# Nettoie les guillemets autour des mots dans stopwords.txt
def removeLimits(item: str):
    if len(item) >= 2:
        return item[1:-1]
    return item

if __name__ == '__main__':
    logging.info("word count started")
    
    if len(sys.argv) < 4:
        logging.error("Syntaxe error : word_count <input_file> <stopwords_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    stopwords_file = sys.argv[2]
    output_dir = sys.argv[3]

    config = SparkConf() \
        .setAppName("word count " + str(time.time())) \
        .setMaster("local[*]") 

    context = SparkContext(conf=config)
    context.setLogLevel("ERROR")

    stopwords = context.textFile(stopwords_file) \
        .map(removeLimits) \
        .flatMap(lambda line: line.split(",")) \
        .map(removeLimits)

    lines = context.textFile(input_file) \
        .map(lambda line: line.lower()) \
        .filter(checkNoEmptyItems)

    words = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: word.strip()) \
        .filter(checkNoEmptyItems)

    counters = words.flatMap(lambda line: line.split(" ")) \
        .filter(checkNoEmptyItems) \
        .subtract(stopwords) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    sorted_counted = counters.sortBy(lambda a: a[1], ascending=False)

    for word in sorted_counted.take(20):
        logging.info("word : " + str(word))

    sorted_counted.coalesce(1) \
        .saveAsTextFile(output_dir)

    context.stop()
    logging.info("word count ended")