from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    URL = spark.sparkContext.uiWebUrl
    path = "C:\\Data\\"
    # Define the filename using a variable
    # Open the file for writing
    with open(path + app_id, "w") as file:
        # Write data to the file
        file.write(f"URL is {URL}.\n")
        file.write("You can write more lines as needed.\n")

    # The file will automatically be closed when the "with" block exits

    categories = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\categories")
    customers = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\customers")
    departments = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\departments")
    order_items = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\order_items")
    orders = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\orders")
    products = spark.read.json("C:\\Data\\data-master\\data-master\\retail_db_json\\products")
    categories.show()
    logger = Log4J(spark)
    logger.info("Starting the spark job")
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    logger.info(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    # your code goes here

    logger.info("end of spark job")

    # spark.stop()
