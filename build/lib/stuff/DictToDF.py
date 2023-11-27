from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize a Spark session
spark = SparkSession.builder.appName("DictionaryToTextFile").getOrCreate()

# Define your Python dictionary
app_id = spark.sparkContext.applicationId
URL = spark.sparkContext.uiWebUrl
start_time = spark.sparkContext.startTime
out_data = {
    "applicationId : ": app_id,
    "URL : ": URL,
    "start_time : ": start_time
}


# Define the schema for the DataFrame (required)
schema = StructType([
    StructField("Attribute", StringType(), True),
    StructField("value", StringType(), True)
])

# Create a DataFrame from the dictionary and schema
df = spark.createDataFrame(list(zip(out_data.keys(), out_data.values())), schema=schema)

# Save the DataFrame to a text file
output_path = "C:\\Data\\dicttofile"
df.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv(output_path)

# Stop the Spark session
spark.stop()
