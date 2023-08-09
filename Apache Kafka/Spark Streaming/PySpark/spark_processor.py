from pyspark.sql import SparkSession
from pyspark.sql.functions import *

kafka_topic_name_input = "input_event"
kafka_topic_name_output = "output_event"
kafka_bootstrap_servers = "localhost:9092"


#Step 1
spark = SparkSession\
    .builder\
    .appName("StructuredKafkaWordCount")\
    .getOrCreate()

#Create a DataSet representing the stream of input lines from Kafka
lines = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
    .option("subscribe", kafka_topic_name_input)\
    .load()\
    .selectExpr("CAST(value AS STRING)")

#Split the lines into words
#explode turns each item in an array into a separate row
words = lines.select(explode(split(lines.value, ' ')).alias('word'))

#Generate a running word count
wordCounts = words.groupBy('word').count()

#Run the query that prints the running counts to the console
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()

