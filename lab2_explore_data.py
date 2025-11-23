import os
import sys
import io

# --- FIX WINDOWS & HADOOP ---
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    # On pointe vers le dossier où vous avez mis winutils.exe
    # Assurez-vous que le fichier est bien dans: C:\hadoop\bin\winutils.exe
    HADOOP_HOME = "C:\\hadoop"
    os.environ['HADOOP_HOME'] = HADOOP_HOME

    # Configuration Python pour Spark
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as _sum, avg, min, max, desc, to_date, month, round as _round

def main():
    print("="*80)
    print("LAB 2 (PART B & C): E-COMMERCE EXPLORATION")
    print("="*80)

    # 1. Initialisation SparkSession [cite: 62-71]
    spark = SparkSession.builder \
        .appName("Day1-DataExploration") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Chargement des données [cite: 72-83]
    # Les fichiers doivent avoir été générés par generate_data.py au préalable
    print("\n[PART B] Chargement des datasets...")
    base_path = "spark-data/ecommerce/"
    
    try:
        customers = spark.read.option("header", "true").option("inferSchema", "true").csv(base_path + "customers.csv")
        products = spark.read.option("header", "true").option("inferSchema", "true").csv(base_path + "products.csv")
        orders = spark.read.option("header", "true").option("inferSchema", "true").csv(base_path + "orders.csv")
        print("✓ Chargement réussi.")
    except Exception as e:
        print(f"ERREUR: Impossible de lire les fichiers CSV dans {base_path}")
        print("Avez-vous lancé generate_data.py avant ?")
        print(f"Détail: {e}")
        return

    # 3. Inspection des Schémas [cite: 85-90]
    print("\n--- 2.3 Inspection des Schémas ---")
    print("CUSTOMERS:")
    customers.printSchema()
    print("PRODUCTS:")
    products.printSchema()
    print("ORDERS:")
    orders.printSchema()

    # 4. Statistiques de base [cite: 91-96]
    print("\n--- 2.4 Statistiques de taille ---")
    print(f"Total Customers: {customers.count()}")
    print(f"Total Products: {products.count()}")
    print(f"Total Orders: {orders.count()}")

    # 5. Aperçu [cite: 97-98]
    print("\n--- 2.5 Aperçu (Orders) ---")
    orders.show(5)

    # 6. Contrôles Qualité [cite: 99-106]
    print("\n--- 2.6 Contrôles Qualité ---")
    print("Valeurs nulles dans Customers:")
    customers.select([count(when(col(c).isNull(), c)).alias(c) for c in customers.columns]).show()
    
    dup_cust = customers.count() - customers.select("customerNumber").distinct().count()
    dup_ord = orders.count() - orders.select("orderNumber").distinct().count()
    print(f"Doublons IDs Clients: {dup_cust}")
    print(f"Doublons IDs Commandes: {dup_ord}")

    # 7. Analyse Exploratoire [cite: 107-118]
    print("\n--- 2.7 Analyse Exploratoire ---")
    
    print("-> Clients par segment:")
    customers.groupBy("customerSegment").count().orderBy(desc("count")).show()

    print("-> Top 10 Pays:")
    customers.groupBy("country").count().orderBy(desc("count")).show(10)

    print("-> Commandes par Statut:")
    orders.groupBy("status").count().show()
    
    print("-> Produits par Catégorie:")
    products.groupBy("productCategory").count().show()

    # 8. Analyse Numérique [cite: 119-126]
    print("\n--- 2.8 Analyse Numérique ---")
    print("-> Stats Montant Commandes:")
    orders.select(
        count("totalAmount").alias("count"),
        min("totalAmount").alias("min"), 
        max("totalAmount").alias("max"), 
        _round(avg("totalAmount"), 2).alias("avg"), 
        _round(_sum("totalAmount"), 2).alias("sum")
    ).show()

    print("-> Limite de crédit par segment:")
    customers.groupBy("customerSegment").agg(
        count("*").alias("count"), 
        _round(avg("creditLimit"), 2).alias("avg_credit"), 
        max("creditLimit").alias("max_credit")
    ).show()

    print("-> Stats Prix Produits (BuyPrice & MSRP):")
    products.select(
        min("buyPrice").alias("min_buy"), max("buyPrice").alias("max_buy"), _round(avg("buyPrice"), 2).alias("avg_buy"),
        min("MSRP").alias("min_msrp"), max("MSRP").alias("max_msrp"), _round(avg("MSRP"), 2).alias("avg_msrp")
    ).show()

    # 9. Rapport de Synthèse [cite: 127-138]
    print("\n--- 2.9 Exportation du rapport ---")
    
    # Calcul des valeurs scalaires
    total_rev = orders.agg(_sum("totalAmount")).collect()[0][0]
    avg_val = orders.agg(avg("totalAmount")).collect()[0][0]
    
    summary_data = [
        ("Total Customers", str(customers.count())),
        ("Total Products", str(products.count())),
        ("Total Orders", str(orders.count())),
        ("Total Revenue", f"{total_rev:.2f}"),
        ("Average Order Value", f"{avg_val:.2f}")
    ]
    
    summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
    summary_df.show(truncate=False)
    
    output_path = base_path + "summary/"
    print(f"Écriture du CSV vers: {output_path}")
    
    # C'est ici que l'erreur winutils se produisait. Avec le fix et le fichier .exe, ça doit passer.
    summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print("✓ Rapport exporté avec succès.")

    # --- PARTIE C: BUSINESS QUESTIONS  ---
    print("\n" + "="*40)
    print("[PART C] BUSINESS QUESTIONS")
    print("="*40)

    # Q1 [cite: 149-153]
    print("\nQ1: Pays avec le plus haut total de limite de crédit:")
    customers.groupBy("country") \
        .agg(_sum("creditLimit").alias("total_credit")) \
        .orderBy(desc("total_credit")) \
        .show(1)

    # Q2 [cite: 154-157]
    print("Q2: Statut de commande le plus fréquent:")
    orders.groupBy("status").count().orderBy(desc("count")).show(1)

    # Q3 [cite: 158-162]
    print("Q3: Catégorie de produit avec le plus de stock:")
    products.groupBy("productCategory") \
        .agg(_sum("quantityInStock").alias("total_stock")) \
        .orderBy(desc("total_stock")) \
        .show(1)

    # Q4 [cite: 163-172]
    print("Q4: Pourcentage de clients 'Enterprise':")
    total_c = customers.count()
    enterprise_c = customers.filter(col("customerSegment") == "Enterprise").count()
    if total_c > 0:
        percentage = (enterprise_c / total_c) * 100
        print(f"  Enterprise: {enterprise_c} / Total: {total_c}")
        print(f"  Pourcentage: {percentage:.2f}%")

    # Q5 [cite: 173-178]
    print("\nQ5: Distribution des commandes par mois:")
    # Conversion de la date string en type Date, puis extraction du mois
    orders_with_month = orders.withColumn("date_obj", to_date(col("orderDate"), "yyyy-MM-dd")) \
                              .withColumn("Month", month("date_obj"))
    
    orders_with_month.groupBy("Month") \
        .count() \
        .orderBy("Month") \
        .show()

    spark.stop()
    print("Script terminé avec succès.")

if __name__ == "__main__":
    main()