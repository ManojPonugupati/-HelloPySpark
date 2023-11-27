from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit
from stuff.Constants import *
# Initialize a Spark session
spark = SparkSession.builder.appName("ValueCheck").getOrCreate()

# Create a DataFrame
data = [("A",), ("B",), ("C",), ("D",)]
columns = ["my_column"]
df = spark.createDataFrame(data, columns)



# Use the 'when' function to check if the column contains the values
# and assign a new column based on the condition
df = df.withColumn("is_in_values", when(df["my_column"].isin(*values_to_check), lit(True)).otherwise(lit(False)))

df.show()
