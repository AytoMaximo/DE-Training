from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#import os

#os.environ['PYSPARK_PYTHON'] = r"W:\Courses\Stepik-DE-Training\DE-Training\05_spark\.venv\Scripts\python.exe"
#os.environ['PYSPARK_DRIVER_PYTHON'] = r"W:\Courses\Stepik-DE-Training\DE-Training\05_spark\.venv\Scripts\python.exe"

spark = SparkSession.builder \
    .appName("RDD to DataFrame") \
    .getOrCreate()

# Распространённый приём: запустить простую задачу на исполнителе
def which_python(_):
    import sys
    return sys.executable

executor_path = spark.sparkContext.runJob(
    spark.sparkContext.parallelize([0], 1),
    which_python
)[0]

print("Executor Python executable:", executor_path)

rdd = spark.sparkContext.parallelize([
    ("Alice", 34, "New York"),
    ("Bob", 45, "San Francisco"),
    ("Catherine", 23, "Los Angeles")
])

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

df = spark.createDataFrame(rdd, schema)

df.show()
df.printSchema()

spark.stop()

