from pyspark.sql import SparkSession
from pyspark.sql.functions import upper
import os

# -------------------------
# SparkSession
# -------------------------
spark = (
    SparkSession.builder
    .appName("Day1-Transformations")
    .master("local[*]")
    .getOrCreate()
)

print("\n=== Loading customers.csv ===\n")

# Use a path relative to this script so it works regardless of the user/home name
base_dir = os.path.dirname(os.path.abspath(__file__))
customers_path = os.path.join(base_dir, "spark-data", "ecommerce", "customers.csv")
print(f"Loading customers from: {customers_path}")
customers = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(customers_path)
)

customers.printSchema()   # <--- AJOUTE ÇA


print("\nTransformation 1: Filter Enterprise customers → Defined, NOT executed")
enterprise = customers.filter(customers.customerSegment == "Enterprise")


print("\nTransformation 2: Select columns → Defined, NOT executed")
enterprise_sel = enterprise.select("customerName", "phone", "city", "country")

print("\nTransformation 3: Add upper-case name → Defined, NOT executed")
enterprise_with_upper = enterprise_sel.withColumn(
    "customerNameUpper", upper(enterprise_sel.customerName)
)

print("\n=== Execution Plan ===")
enterprise_with_upper.explain()

# -------------------------
# Actions
# -------------------------

print("\n=== Action 1: COUNT ===")
print("Enterprise count =", enterprise_with_upper.count())

print("\n=== Action 2: SHOW ===")
enterprise_with_upper.show(5, truncate=False)

print("\n=== Action 3: COLLECT (safe small limit) ===")
rows = enterprise_with_upper.limit(3).collect()
for r in rows:
    print(r)

# -------------------------
# Key Insights
# -------------------------
print("\n=== KEY INSIGHTS ===")
print("""
• A transformation builds a new DataFrame but Spark DOES NOT execute it immediately.
• An action triggers execution (count, show, collect…).
• Lazy evaluation allows Spark to optimize the whole pipeline before executing it.
""")

spark.stop()