from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

if __name__ == "__main__":
    
    # 1. Create SparkSession
    spark = SparkSession.builder \
        .appName("Lab1 Hello Spark") \
        .master("local[*]") \
        .getOrCreate()

    print("\n=== Spark Environment Info ===")
    print("Spark Version:", spark.version)
    print("Application Name:", spark.sparkContext.appName)
    print("Master:", spark.sparkContext.master)
    print("================================\n")

    # 2. Create DataFrame with sample static data
    data = [
        ("Alice", 25, "Paris", 3000.0),
        ("Bob", 35, "London", 4500.0),
        ("Charlie", 30, "Paris", 5000.0),
        ("Dina", 40, "Berlin", 6000.0)
    ]

    columns = ["name", "age", "city", "salary"]

    df = spark.createDataFrame(data, schema=columns)

    # 3. Print schema
    print("=== DataFrame Schema ===")
    df.printSchema()

    # 4. Show data
    print("\n=== Data Preview ===")
    df.show()

    # 5. Filter example
    print("\n=== People older than 30 ===")
    df.filter(col("age") > 30).show()

    # 6. Aggregation example
    print("\n=== Average salary by city ===")
    df.groupBy("city").agg(avg("salary").alias("avg_salary")).show()

    # 7. Count rows
    print("\n=== Row Count ===")
    print("Total rows:", df.count())

    # 8. Stop session

    print("\n=== Spark Application Finished ===\n")
    input("Press Enter to exit...")
    spark.stop()