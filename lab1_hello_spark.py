import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, avg, round, lit
from pyspark.sql import types as T

# --- Configuration et Initialisation ---

# Définition des chemins de fichiers (ils supposent que le répertoire 'spark-data' est monté/accessible)
CUSTOMER_PATH = "spark-data/ecommerce/customers.csv"
PRODUCT_PATH = "spark-data/ecommerce/products.csv"
ORDER_PATH = "spark-data/ecommerce/orders.csv"
SUMMARY_OUTPUT_PATH = "spark-data/ecommerce/summary/exploration_summary.csv"

# 2.1 Create a SparkSession
print("--- 2.1: Initialisation de la SparkSession ---")
try:
    spark = (
        SparkSession.builder
        .appName("Day1-DataExploration")
        .master("spark://localhost:7077")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    # Configure le niveau de log pour être moins verbeux après l'initialisation
    spark.sparkContext.setLogLevel("WARN") 
    print("SparkSession créée avec succès.")
except Exception as e:
    print(f"[ERREUR] Échec de la création de la SparkSession: {e}")
    sys.exit(1)


# --- 2.2: Chargement des Données ---
print("\n--- 2.2: Chargement des trois datasets CSV ---")

try:
    # Fonction utilitaire pour charger un DataFrame
    def load_data(path, name):
        df = (
            spark.read.csv(
                path,
                header=True,       # Utiliser l'en-tête du fichier
                inferSchema=True,  # Inférer les types de colonnes
                sep=';'            # Si le séparateur n'est pas la virgule, ajustez-le ici (souvent 'sep=',' ou 'sep=';')
            )
            .withColumnRenamed("customerNumber", "customer_Number") # Renommer pour éviter les problèmes de casse/espace
            .withColumnRenamed("orderNumber", "order_Number")
            .withColumnRenamed("productCode", "product_Code")
            .withColumnRenamed("buyPrice", "buy_Price")
            .withColumnRenamed("creditLimit", "credit_Limit")
            .withColumnRenamed("totalAmount", "total_Amount")
            .withColumnRenamed("customerSegment", "customer_Segment")
            .withColumnRenamed("paymentMethod", "payment_Method")
            .withColumnRenamed("productCategory", "product_Category")
            .cache()
        )
        print(f"Dataset '{name}' chargé et mis en cache avec succès.")
        return df

    customers = load_data(CUSTOMER_PATH, "customers")
    products = load_data(PRODUCT_PATH, "products")
    orders = load_data(ORDER_PATH, "orders")

except Exception as e:
    print(f"\n[ERREUR] Échec du chargement des données. Vérifiez les chemins et les montages Docker. Erreur: {e}")
    # Arrêter Spark en cas d'erreur critique de chargement
    spark.stop()
    sys.exit(1)


# --- 2.3: Inspection des Schémas ---
print("\n--- 2.3: Inspection des Schémas ---")
print("\nCUSTOMERS Schema:")
customers.printSchema()

print("\nPRODUCTS Schema:")
products.printSchema()

print("\nORDERS Schema:")
orders.printSchema()


# --- 2.4: Statistiques de Base (Taille) ---
print("\n--- 2.4: Statistiques de Base (Taille) ---")
print(f"Total number of customers: {customers.count()}")
print(f"Total number of products: {products.count()}")
print(f"Total number of orders: {orders.count()}")


# --- 2.5: Aperçu des Données ---
print("\n--- 2.5: Data Preview (5 premières lignes) ---")
print("\nCustomers Sample:")
customers.show(5, truncate=False)

print("\nProducts Sample:")
products.show(5, truncate=False)

print("\nOrders Sample:")
orders.show(5, truncate=False)


# --- 2.6: Contrôles de Qualité des Données ---
print("\n--- 2.6: Contrôles de Qualité des Données ---")

# Calcul des Nulls par colonne pour CUSTOMERS
print("\n** Nombre de valeurs Nulls par colonne (CUSTOMERS) **")
customers_null_checks = customers.select(
    *[
        count(when(col(c).isNull(), c)).alias(c) 
        for c in customers.columns
    ]
).collect()[0].asDict()
for col_name, null_count in customers_null_checks.items():
    if null_count > 0:
        print(f"  - {col_name}: {null_count} nulls")
    else:
        print(f"  - {col_name}: OK")

# Calcul des Nulls par colonne pour ORDERS
print("\n** Nombre de valeurs Nulls par colonne (ORDERS) **")
orders_null_checks = orders.select(
    *[
        count(when(col(c).isNull(), c)).alias(c) 
        for c in orders.columns
    ]
).collect()[0].asDict()
for col_name, null_count in orders_null_checks.items():
    if null_count > 0:
        print(f"  - {col_name}: {null_count} nulls")
    else:
        print(f"  - {col_name}: OK")

# Vérification des doublons d'ID

# Customers Duplicates
customer_count = customers.count()
distinct_customer_count = customers.select("customer_Number").distinct().count()
customer_duplicates = customer_count - distinct_customer_count
print(f"\nTotal Customers: {customer_count}")
print(f"Distinct Customer IDs: {distinct_customer_count}")
print(f"Number of duplicate Customer IDs: {customer_duplicates}")

# Orders Duplicates
order_count = orders.count()
distinct_order_count = orders.select("order_Number").distinct().count()
order_duplicates = order_count - distinct_order_count
print(f"\nTotal Orders: {order_count}")
print(f"Distinct Order IDs: {distinct_order_count}")
print(f"Number of duplicate Order IDs: {order_duplicates}")


# --- 2.7: Analyse Exploratoire ---
print("\n--- 2.7: Analyse Exploratoire ---")

# Customers by segment
print("\n** Clients par Segment (Top 5) **")
customers.groupBy("customer_Segment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(5, truncate=False)

# Top 10 countries by customer count
print("\n** Top 10 Pays par Nombre de Clients **")
customers.groupBy("country") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10, truncate=False)

# Orders by status
print("\n** Commandes par Statut **")
orders.groupBy("status") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# Orders by payment method
print("\n** Commandes par Méthode de Paiement **")
orders.groupBy("payment_Method") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# Products by category
print("\n** Produits par Catégorie **")
products.groupBy("product_Category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)


# --- 2.8: Analyse Numérique ---
print("\n--- 2.8: Analyse Numérique ---")

# Order amount statistics (from orders.totalAmount)
print("\n** Statistiques sur le Montant Total des Commandes (total_Amount) **")
orders.select(
    count("total_Amount").alias("Count"),
    round(sum("total_Amount"), 2).alias("Total_Sum"),
    round(min("total_Amount"), 2).alias("Minimum"),
    round(max("total_Amount"), 2).alias("Maximum"),
    round(avg("total_Amount"), 2).alias("Average")
).show(truncate=False)

# Credit limit statistics by segment (grouped by customerSegment)
print("\n** Statistiques sur la Limite de Crédit par Segment Client **")
customers.groupBy("customer_Segment") \
    .agg(
        count("credit_Limit").alias("Count"),
        round(avg("credit_Limit"), 2).alias("Average_Credit"),
        round(max("credit_Limit"), 2).alias("Max_Credit")
    ) \
    .orderBy(col("Average_Credit").desc()) \
    .show(truncate=False)

# Product price statistics: For buyPrice and MSRP
print("\n** Statistiques sur les Prix des Produits (buyPrice & MSRP) **")
products.select(
    round(min("buy_Price"), 2).alias("Min_Buy_Price"),
    round(max("buy_Price"), 2).alias("Max_Buy_Price"),
    round(avg("buy_Price"), 2).alias("Avg_Buy_Price"),
    round(min("MSRP"), 2).alias("Min_MSRP"),
    round(max("MSRP"), 2).alias("Max_MSRP"),
    round(avg("MSRP"), 2).alias("Avg_MSRP")
).show(truncate=False)


# --- 2.9: Export du Rapport de Synthèse ---
print("\n--- 2.9: Export du Rapport de Synthèse ---")

# 1. Calcul des métriques
total_customers = customers.count()
total_products = products.count()
total_orders = orders.count()

total_revenue = orders.select(sum("total_Amount")).collect()[0][0]
average_order_value = orders.select(avg("total_Amount")).collect()[0][0]

# 2. Création du DataFrame de résumé
summary_schema = T.StructType([
    T.StructField("Metric", T.StringType(), True),
    T.StructField("Value", T.StringType(), True) # Utilisation de String pour formater l'affichage
])

summary_data = [
    ("Total Customers", str(total_customers)),
    ("Total Products", str(total_products)),
    ("Total Orders", str(total_orders)),
    ("Total Revenue", f"{total_revenue:,.2f} USD"),
    ("Average Order Value", f"{average_order_value:,.2f} USD"),
]

summary_df = spark.createDataFrame(summary_data, summary_schema)

print("\nContenu du DataFrame de résumé:")
summary_df.show(truncate=False)

# 3. Écriture du fichier CSV
(
    summary_df.coalesce(1) # S'assurer d'avoir un seul fichier de sortie
    .write.mode("overwrite")
    .option("header", "true")
    .csv(SUMMARY_OUTPUT_PATH)
)
print(f"\n[SUCCÈS] Rapport de synthèse exporté vers: {SUMMARY_OUTPUT_PATH}")

# --- 3. Arrêt de Spark ---
print("\n--- Fin du script : Arrêt de la SparkSession ---")
spark.stop()
print("SparkSession arrêtée.")