# lab3_transformation_practice.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Créer une SparkSession
spark = SparkSession.builder \
    .appName("Day1-TransformationPractice") \
    .master("local[*]") \
    .getOrCreate()

# 2. Charger orders.csv
print("\n--- 2. Chargement des commandes ---")
orders_df = spark.read.csv(
    "spark-data/ecommerce/orders.csv",
    header=True,
    inferSchema=True
)

# 3. Implémentation des exercices

# Exercise 1 - Filter
print("\n--- Exercise 1: Filter (totalAmount > 5000) ---")
high_value = orders_df.filter(F.col("totalAmount") > 5000)  # [cite: 70, 71]
print(f"Number of such orders: {high_value.count()} ")  # [cite: 73]
high_value.show(5)  # [cite: 74]

# Exercise 2 - Select & Rename
print("\n--- Exercise 2: Select & Rename ---")
order_summary = orders_df.select(
    F.col("orderNumber").alias("id"),  # [cite: 78]
    F.col("orderDate").alias("date"),  # [cite: 79]
    F.col("totalAmount").alias("amount"),  # [cite: 82]
    "status"  # [cite: 81]
)
print("Simplified view of orders:")
order_summary.show(5)  # [cite: 83]

# Exercise 3 - Add a Computed Column (Classify orders)
print("\n--- Exercise 3: Add 'orderSize' Computed Column ---")
orders_categorized = orders_df.withColumn(
    "orderSize",
    F.when(F.col("totalAmount") < 1000, "Small")  # [cite: 87]
    .when((F.col("totalAmount") >= 1000) & (F.col("totalAmount") < 5000), "Medium")  # [cite: 88]
    .otherwise("Large")  # [cite: 89]
)
orders_categorized.select("orderNumber", "totalAmount", "orderSize").show(5)  # [cite: 90]

# Compute and display the distribution
print("Distribution by orderSize:")
orders_categorized.groupBy("orderSize").count().orderBy("orderSize").show()  # [cite: 92]

# Exercise 4 - Chain Multiple Transformations (Shipped Orders)
print("\n--- Exercise 4: Chain Multiple Transformations (Shipped Pipeline) ---")
processed_orders = orders_df \
    .filter(F.col("status") == "Shipped") \
    .withColumn("amountRounded", F.round("totalAmount", 0)) \
    .withColumn("priority",
        F.when(F.col("totalAmount") > 5000, "High")  # [cite: 99]
        .when(F.col("totalAmount") > 2000, "Medium")  # [cite: 100]
        .otherwise("Low")  # [cite: 101]
    ) \
    .select("orderNumber", "orderDate", "amountRounded", "priority", "status")  # [cite: 102]

print(f"Number of shipped orders: {processed_orders.count()} ")  # [cite: 104]
processed_orders.show(10)  # [cite: 104]

# Exercise 5 - Drop Unnecessary Columns
print("\n--- Exercise 5: Drop Unnecessary Columns ---")
print(f"Number of original columns: {len(orders_df.columns)} [cite: 109]")
orders_reduced = orders_df.drop("requiredDate", "paymentMethod")  # [cite: 107]
print(f"Number of columns after dropping: {len(orders_reduced.columns)} ")  # [cite: 110]
orders_reduced.show(5)  # [cite: 111]

# Exercise 6 - Distinct Values
print("\n--- Exercise 6: Distinct Values ---")
print("Distinct values of status:")
orders_df.select("status").distinct().show(truncate=False)  # [cite: 114]
print("Distinct values of paymentMethod:")
orders_df.select("paymentMethod").distinct().show(truncate=False)  # [cite: 115]

# Task B.2 - "Your Turn" Practice Questions (Required Deliverable)
print("\n--- Task B.2: Your Turn Practice Questions ---")

# 1. Filter orders from '2024-06-01' onwards.
orders_from_june = orders_df.filter(F.col("orderDate") >= F.lit("2024-06-01"))  # [cite: 118]
print(f"Orders from 2024-06-01 onwards: {orders_from_june.count()}")

# 2. Create a Boolean column isLargeOrder (true if totalAmount > 3000).
orders_with_bool = orders_df.withColumn(
    "isLargeOrder",
    F.col("totalAmount") > 3000
)  # [cite: 119]
orders_with_bool.select("orderNumber", "totalAmount", "isLargeOrder").show(5)

# 3. Select only orders with status "Processing" or "Shipped".
processing_or_shipped = orders_df.filter(F.col("status").isin("Processing", "Shipped"))  # [cite: 120]
print(f"Processing or Shipped orders: {processing_or_shipped.count()}")

# 4. Add a column orderCode formatted as "ORDER-00001" based on orderNumber.
orders_with_code = orders_df.withColumn(
    "orderCode",
    F.concat(
        F.lit("ORDER-"),
        F.lpad(F.col("orderNumber"), 5, "0") # Utilisation de lpad pour ajouter des zéros à gauche
    )
)  # [cite: 121, 122]
orders_with_code.select("orderNumber", "orderCode").show(5)

# 5. Find the top 10 most expensive orders by totalAmount.
top_10_expensive = orders_df.orderBy(F.col("totalAmount").desc()).limit(10)  # [cite: 123]
print("Top 10 Most Expensive Orders:")
top_10_expensive.show(truncate=False)

# Arrêter Spark
spark.stop()