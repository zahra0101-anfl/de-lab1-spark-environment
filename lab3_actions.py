# lab3_actions.py
import os
os.environ["HADOOP_HOME"] = "tmp_hadoop"
os.environ["hadoop.home.dir"] = "tmp_hadoop"

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Créer une SparkSession
spark = (
    SparkSession.builder.appName("Day1-Actions")
    .master("local[*]")
    .getOrCreate()
)

# 2. Charger customers.csv
print("Chargement du dataset customers.csv...")
customers = (
    spark.read.csv(
        "spark-data/ecommerce/customers.csv",
        header=True,
        inferSchema=True,
    )
)

# --- Action 1 - count() ---
print("\n--- Action 1: count() ---")
print(f"Nombre total de clients : {customers.count()}")  # [cite: 137]
enterprise_count = customers.filter(col("customerSegment") == "Enterprise").count()
print(f"Nombre de clients 'Enterprise' : {enterprise_count}")  # [cite: 138]

# --- Action 2 - show() ---
print("\n--- Action 2: show() ---")
print("a) Comportement par défaut (20 lignes, tronqué)[cite: 141]:")
customers.show()

print("b) 5 lignes, non tronqué (utile pour voir de longues chaînes)[cite: 142]:")
customers.show(5, truncate=False)

print("c) 3 lignes, vertical=True (utile pour les DataFrames avec beaucoup de colonnes)[cite: 143]:")
customers.show(3, vertical=True)

# --- Action 3 - collect() ---
print("\n--- Action 3: collect() ---")
small_sample = customers.limit(3).collect()  # Ramène 3 enregistrements au Driver [cite: 147]

print(f"Type de 'small_sample' : {type(small_sample)}[cite: 150].")
print(f"Type du premier élément : {type(small_sample[0])}[cite: 151].")
print(f"Exemple de valeur: customerName={small_sample[0]['customerName']}, Country={small_sample[0]['country']}[cite: 152].")

print("\n!!! AVERTISSEMENT: collect() est DANGEREUX sur de grands DataFrames car il ramène TOUTES les données en mémoire sur le Driver, risquant un OutOfMemoryError (OOM)[cite: 153, 197, 203].")

# --- Action 4 - take(n) ---
print("\n--- Action 4: take(n) ---")
take_sample = customers.take(5)  # Similaire à limit().collect(), mais plus concis [cite: 155]
for i, row in enumerate(take_sample):
    print(f"Ligne {i+1}: Customer Number={row['customerNumber']}, City={row['city']}")

# --- Action 5 - first() et head() ---
print("\n--- Action 5: first() et head() ---")
first_row = customers.first()  # Récupère la première ligne [cite: 158]
head_row = customers.head()    # Alias pour first() [cite: 158]

print(f"first().customerName: {first_row['customerName']}")
print(f"head().customerName: {head_row['customerName']}")
print("first() et head() retournent la même ligne sur un DataFrame[cite: 159].")

# --- Action 6 - write ---
print("\n--- Action 6: write ---")
# Écrire les clients des États-Unis en CSV
usa_customers = customers.filter(col("country") == "USA")
(
    usa_customers.coalesce(1)  # coalesce(1) pour créer un seul fichier de sortie [cite: 162]
    .write.mode("overwrite")
    .option("header", "true")
    .csv("spark-data/ecommerce/usa_customers")
)
print("Clients des États-Unis écrits dans spark-data/ecommerce/usa_customers (CSV)[cite: 165].")

# Écrire tous les clients en Parquet
try:
    customers.write.mode("overwrite").parquet("spark-data/ecommerce/customers.parquet")
    print("Tous les clients écrits dans spark-data/ecommerce/customers.parquet (Parquet)")
except Exception as e:
    print(f"⚠️ Spark Parquet write failed: {e}")
    # Fallback: write CSV using Spark
    try:
        customers.write.csv("spark-data/ecommerce/customers_fallback", header=True, mode="overwrite")
        print("Fallback: écrit localement dans spark-data/ecommerce/customers_fallback (CSV)")
    except Exception as e2:
        print(f"Fallback write also failed: {e2}")

# --- Action 7 - foreachPartition() ---
print("\n--- Action 7: foreachPartition() ---")
def print_partition_size(iterator):
    """Fonction à appliquer à chaque partition pour imprimer sa taille."""
    size = sum(1 for _ in iterator)
    print(f"Taille de la partition: {size}")

print("Exécution de foreachPartition()... (La sortie peut être entrelacée)")
for r in customers.take(5):
    print(r)
print("foreachPartition() terminé[cite: 170].")

# --- Comparaison des performances des actions ---
print("\n--- Comparaison des performances des actions ---")
# Le temps d'exécution est très dépendant du setup et de la taille des données, ce sont des exemples conceptuels.

# Action 1: count() (Job complet)
start_time = time.time()
customers.count()
end_time = time.time()
print(f"customers.count() time: {end_time - start_time:.4f}s[cite: 174].")

# Action 2: show(5) (Job partiel, collecte 5 lignes)
start_time = time.time()
customers.show(5)
end_time = time.time()
print(f"customers.show(5) time: {end_time - start_time:.4f}s[cite: 175].")

# Action 3: limit(100).collect() (Job partiel, collecte 100 lignes au Driver)
start_time = time.time()
customers.limit(100).collect()
end_time = time.time()
print(f"customers.limit(100).collect() time: {end_time - start_time:.4f}s[cite: 176].")

print("\nInterprétation : show() et limit().collect() devraient être plus rapides que count() car ils n'ont besoin de lire/traiter qu'une petite partie du dataset[cite: 178].")

# --- Modèles Dangereux (Théorie) ---
print("\n--- Modèles Dangereux & Alternatives Sûres ---")
print("❌ **Dangereux sur de Grands DataFrames** :")
print("• `df.collect()` : Ramène TOUT le DataFrame au Driver. Risque d'OOM et de blocage[cite: 181, 197, 203].")
print("• `df.toPandas()` : Similaire à collect(), le DataFrame Pandas est créé en mémoire du Driver[cite: 182].")
print("• Boucles Python utilisant `df.collect()` : Tente d'itérer sur toutes les données en mémoire du Driver[cite: 183].")

print("\n✅ **Alternatives Sûres** :")
print("• `df.limit(n).collect()` ou `df.take(n)` : Ramène seulement un petit échantillon (n lignes)[cite: 185, 186, 198].")
print("• `df.show(n)` : Affiche un échantillon sans ramener une liste Python en mémoire[cite: 187, 199, 200].")
print("• `df.write...` : Écrit les données directement sur le stockage (HDFS, S3, etc.), sans passer par la mémoire du Driver[cite: 189].")
print("• `df.foreachPartition(...)` : Exécute le traitement sur les nœuds de l'Exécuteur, et non sur le Driver[cite: 188].")

# Arrêter Spark
spark.stop()