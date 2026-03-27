
import logging
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession, Column
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    config = SparkConf() \
        .setAppName("train model" + str(time.time()))
    
    session = SparkSession.builder \
            .config(conf=config) \
            .getOrCreate()
    
    session.sparkContext.setLogLevel("OFF")

    ventes_schema = StructType([ StructField("transaction_id", IntegerType(), True),
                         StructField("vendeur_nom", StringType(), True), 
                         StructField("montant", DoubleType(), True), 
                         StructField("date", DateType(), True) 
                    ])

    df = session.read.csv("hdfs://namenode:9000/tp5/ventes_2.csv", header=True, schema=ventes_schema)

    data = df.withColumn("label", when(df.montant > 150, 1).otherwise(0))

    logging.info("nb line " + str(df.count()))

    indexer = StringIndexer(inputCol="vendeur_nom", outputCol="vendeur_index", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["vendeur_index", "montant"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

    pipeline = Pipeline(stages=[indexer, assembler, lr])
    model = pipeline.fit(data)

    model.write().overwrite().save("hdfs://namenode:9000/tp5/model_premium")
    logging.info("Saved model to HDFS")