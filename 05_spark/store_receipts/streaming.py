from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaRawMessageViewer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "receipts") \
    .load()

# Преобразуем value в строку
debug_df = raw_df.selectExpr("CAST(value AS STRING) AS message")

# Вывод в консоль
debug_query = debug_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

spark.streams.awaitAnyTermination()