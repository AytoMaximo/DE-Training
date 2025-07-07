from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType

# appName задаёт имя приложения
# master("local[*]") говорит Spark использовать все доступные ядра на компьютере.
# spark.jars.packages подключает нужный JAR для работы с Kafka.

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

# Ожидается JSON-объект с одним полем value, которое является целым числом.
schema = StructType().add("value", IntegerType())

# Здесь Spark подключается к Kafka:
# format("kafka") указывает, откуда поступают данные.
# bootstrap.servers — адрес Kafka-брокера.
# subscribe("test-topic") — подписка на конкретный топик Kafka.

# На этом этапе Spark начинает получать поток сообщений, где каждое сообщение имеет ключ и значение в бинарном виде.

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Kafka возвращает данные в байтах, поэтому:
# Сначала мы превращаем value в строку.
# Затем применяем from_json(...) — разбираем JSON по указанной схеме.
# В итоге получаем DataFrame, где остаётся только нужное поле value.

parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.value")

# Наконец, мы запускаем потоковую обработку:
# writeStream говорит Spark, что делать с обработанными данными.
# format("console") — просто вывод в терминал.
# outputMode("append") — каждый новый результат будет добавляться к выводу.
# awaitTermination() удерживает приложение в живом состоянии, пока оно обрабатывает поток.

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()