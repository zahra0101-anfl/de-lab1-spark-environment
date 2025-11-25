from pyspark.sql import SparkSession
from pyspark.sql.functions import upper
import os
import sys

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
#### AI MODIFICATION START #####
# -------------------------
# Write output (same mechanism as lab2_explore_data)
# -------------------------

print("\n=== 3. Exportation CSV (same logic as Lab 2) ===")

output_dir = os.path.join(base_dir, "spark-data", "ecommerce", "enterprise_with_upper")
print(f"Écriture du CSV vers: {output_dir}")

# --- WINDOWS HANDLING / HADOOP_HOME logic identical to Lab 2 ---

write_with_python = False
winutils_path = None

if sys.platform == "win32":
    HADOOP_HOME = "C:\\hadoop"
    winutils_path = os.path.join(HADOOP_HOME, "bin", "winutils.exe")

    # If winutils.exe is missing → skip Spark write to avoid Java exception
    if not os.path.exists(winutils_path):
        print("winutils.exe not found — skipping Spark CSV write and using fallback.")
        write_with_python = True

# --- Try Spark write first (only if winutils exists OR not Windows) ---

if not write_with_python:
    try:
        enterprise_with_upper.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
        print("✓ Exporté avec succès via Spark.")
    except Exception as e:
        print(f"⚠️ Spark CSV write failed: {e}")
        write_with_python = True

# --- Fallback CSV write (exact same as LAB 2) ---

if write_with_python:
    print("Fallback: écriture locale du CSV via le module csv de Python...")

    import csv
    print("writing with csv instead....")
    os.makedirs(output_dir, exist_ok=True)
    local_file = os.path.join(output_dir, "enterprise_with_upper.csv")

    rows = enterprise_with_upper.collect()

    with open(local_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customerName", "phone", "city", "country", "customerNameUpper"])

        for r in rows:
            try:
                writer.writerow([r["customerName"], r["phone"], r["city"], r["country"], r["customerNameUpper"]])
            except Exception:
                writer.writerow(list(r))

    print(f"✓ CSV exporté localement via fallback: {local_file}")
#### AI MODIFICATION END #####
    # -------------------------
    # Write output (like lab2_explore_data)
    # -------------------------
    output_dir = os.path.join(base_dir, "spark-data", "ecommerce", "enterprise_with_upper")
    print(f"\n=== Writing enterprise_with_upper to CSV: {output_dir} ===")
    try:
        enterprise_with_upper.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")
        print(f"✓ Exporté avec succès via Spark dans {output_dir}")
    except Exception as e:
        print(f"⚠️ Spark CSV write failed: {e}")
        # Fallback: write CSV using Spark (no manual Python fallback)
        try:
            enterprise_with_upper.write.csv(output_dir + "_fallback", header=True, mode="overwrite")
            print(f"Fallback: écrit localement dans {output_dir}_fallback (CSV)")
        except Exception as e2:
            print(f"Fallback write also failed: {e2}")

    spark.stop()