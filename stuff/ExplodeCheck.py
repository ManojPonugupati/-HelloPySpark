from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Create a Spark session
spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

# Sample DataFrame with an array column
data = [(1, ["apple", "banana", "cherry"]), (2, ["grape", "kiwi"]), (3, ["orange"])]
columns = ["id", "fruits"]

df = spark.createDataFrame(data, columns)

# Explode the "fruits" array column into separate rows
exploded_df = df.select("id", explode("fruits").alias("fruit"))

exploded_df.show()
