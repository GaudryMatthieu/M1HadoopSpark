import logging 
import time
from pyspark import SparkConf, SparkContext
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("test_hdfs started")
    config = SparkConf() \
        .setAppName("test_hdfs"+str(time.time())) \
        .setMaster("local[*]")

    context = SparkContext(conf=config)
    context.setLogLevel("WARN")

    # Lire le fichier lic-data-2023.csv depuis hdfs
    lines = context.textFile("hdfs://namenode:9000/lic-data-2023.csv")

    # Rechercher les fédérations sportives les plus représentées
    # Lire toutes les lignes sauf la ligne d'entete 
    header = lines.first()
    heads = header.split(";")
    data = lines.filter(lambda line: line != header)

    # Conserver les lignes intitulées "Code", "Fédération" et "Totadocker cp lic-data-2023.csv namenode:/tmp/lic-data-2023.csvl"
    triplets = data.map(lambda line: line.split(";")) \
                   .map(lambda fields: (fields[7], (fields[8], int(fields[len(heads) - 1][1:-1]))))

    # Faire le cumul des "Total" pour chaque fédération
    triplets_sum = triplets.reduceByKey(lambda a, b: (a[0], a[1] + b[1]))

    # Trier par ordre décroissant des totaux
    triplets_sorted = triplets_sum.coalesce(1).sortBy(lambda a: a[1][1], ascending=False)

    # Afficher et sauvegarder le résultat dans hdfs
    triplets_sorted.saveAsTextFile("hdfs://namenode:9000/triplets_sorted")

    context.stop()
    logging.info("test_hdfs ended")

