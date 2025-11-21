from pyspark.sql import SparkSession

if __name__ == "__main__":
    #  SparkSession
    spark = SparkSession.builder \
        .appName("TestPySpark") \
        .master("local[*]") \
        .getOrCreate()

    print("SparkSession started successfully!")
    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)

    spark.stop()
    print("SparkSession stopped.")