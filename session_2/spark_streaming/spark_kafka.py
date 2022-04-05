from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


conf = SparkConf().set('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.0')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)



kvs = KafkaUtils.createDirectStream(ssc, ["quickstart-events"], {"metadata.broker.list": "localhost:9092"})
lines = kvs.map(lambda x: json.loads(x[1])["index"])
lines.pprint(num=10000000)
ssc.start()
ssc.awaitTermination()