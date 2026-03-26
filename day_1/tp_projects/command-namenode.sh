root@0ed35370a2c0:/# history 
    1  ls
    2  exit
    3  ls
    4  exit
    5  ls
    6  hdfs dfs -ls /
    7  hdfs dfs -put ./lic-data-2023.csv ./
    8  hdfs dfs -put ./lic-data-2023.csv /.
    9  hdfs dfs -ls / 
   10  exit
   11  hdfs dfs -ls /
   12  hdfs dfs -rmr /triplets_sorted
   13  hdfs dfs -ls /
   14  exit
   15  hdfs dfs -ls /
   16  hdfs dfs -ls /
   17  hdfs dfs -rm -r /triplets_sorted
   18  exit
   19  ls
   20  hdfs dfs -ls /
   21  hdfs dfs -head /triplets_sorted
   22  hdfs dfs -head /triplets_sorted/part-00000
   23  hist
   24  history


   # Pour lancer un programme faire 
   $ docker-compose up -d
   # Aller dans le bash du master  
   docker exec -it nomdudocker bash
   \# ../bin/spark-submit --master spark://spark-master:7077 code.py
   \# ../bin/spark-submit --master spark://spark-master:7077 --packages  mongo-spark-connector_2.12-3.0.2.jar tp1-streaming.py

   \# ../bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 tp1-streaming.py
   \# ../bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 tp1-streaming.py