# lab4_log_analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp, hour, desc, when, count, 
    min, max, avg, countDistinct, sum, lit, round
)

# Constantes
LOG_FILE_PATH = "spark-data/ecommerce/web_logs.txt"
PARQUET_OUTPUT_PATH = "spark-data/ecommerce/processed_logs"
SUMMARY_OUTPUT_PATH = "spark-data/ecommerce/log_summary"

# 1. Créer une SparkSession
print("1. Création de la SparkSession...")
spark = (
    SparkSession.builder.appName("Day1-LogAnalysis")
    .master("local[*]")
    .config("spark.driver.memory", "2g") # Configuration demandée
    .getOrCreate()
)

# 2. Charger les logs bruts
print(f"\n2. Chargement des logs bruts à partir de {LOG_FILE_PATH}...")
raw_logs = spark.read.text(LOG_FILE_PATH)

total_logs_count = raw_logs.count()
print(f"Nombre de lignes de logs (bruts) : {total_logs_count}.")
print("Échantillon de logs bruts:")
raw_logs.show(3, truncate=False)

# 3. Parser le format de log en utilisant une expression régulière
print("\n3. Parsing des logs en colonnes structurées...")
# Le pattern d'expression régulière pour le format:
# IP [timestamp] "METHOD ENDPOINT HTTP/1.1" STATUS RESPONSETIME
# Note: (\S+)       -> 1 (IP)
# \[([\w:/]+\s[+\-]\d{4})\] -> 2 (timestamp)
# "(\S+) (\S+) (\S+)" -> 3 (METHOD), 4 (ENDPOINT), 5 (PROTOCOL)
# (\d+)             -> 6 (STATUS)
# (\d+)             -> 7 (RESPONSE_TIME)
log_pattern = r'^(\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d+) (\d+)$'

parsed_logs = (
    raw_logs.select(
        regexp_extract(col("value"), log_pattern, 1).alias("ip"),
        regexp_extract(col("value"), log_pattern, 2).alias("timestamp"),
        regexp_extract(col("value"), log_pattern, 3).alias("method"),
        regexp_extract(col("value"), log_pattern, 4).alias("endpoint"),
        regexp_extract(col("value"), log_pattern, 6).cast("int").alias("status"), # Cast to int [cite: 71, 247]
        regexp_extract(col("value"), log_pattern, 7).cast("int").alias("response_time_ms"), # Cast to int [cite: 72, 247]
    ).where(col("ip") != "") # Filtrage initial des lignes non matchées
)

print("Schéma des logs parsés:")
parsed_logs.printSchema()
print("Premières 10 lignes des logs parsés:")
parsed_logs.show(10, truncate=False)

# 4. Vérification de la qualité des données
print("\n4. Vérification de la qualité des données...")
valid_logs_count = parsed_logs.count() # Nous avons déjà filtré les IPs vides lors du parsing
invalid_logs_count = total_logs_count - valid_logs_count
logs = parsed_logs.filter(col("ip") != "") # Le DataFrame nettoyé

print(f"Total des logs bruts : {total_logs_count}")
print(f"Total des logs valides : {valid_logs_count}")
print(f"Logs invalides/non matchés : {invalid_logs_count}")
print(f"DataFrame nettoyé 'logs' créé avec {logs.count()} lignes.")

# 5. Analyses de base
print("\n5. Analyses de base:")
print("a) Distribution du code de statut:")
(
    logs.groupBy("status").count().orderBy(desc("count"))
    .show()
)

print("b) Distribution de la méthode HTTP:")
(
    logs.groupBy("method").count().orderBy(desc("count"))
    .show()
)

print("c) Top 10 des pages les plus visitées (endpoint):")
(
    logs.groupBy("endpoint").count().orderBy(desc("count"))
    .limit(10).show(truncate=False)
)

# 6. Analyse des erreurs
print("\n6. Analyse des erreurs:")
# 4xx et 5xx
error_logs = logs.filter((col("status") >= 400) & (col("status") < 600))
total_errors = error_logs.count()
errors_4xx = logs.filter((col("status") >= 400) & (col("status") < 500)).count()
errors_5xx = logs.filter(col("status") >= 500).count()

print(f"Total des erreurs (4xx et 5xx) : {total_errors}")
print(f"Nombre d'erreurs 4xx : {errors_4xx}")
print(f"Nombre d'erreurs 5xx : {errors_5xx}")

print("\nTop 404 pages:")
(
    logs.filter(col("status") == 404)
    .groupBy("endpoint").count()
    .orderBy(desc("count"))
    .show(10, truncate=False)
)

print("\nPages causant des erreurs 5xx:")
(
    logs.filter(col("status") >= 500)
    .groupBy("endpoint", "status").count()
    .orderBy(desc("count"))
    .show(10, truncate=False)
)

# 7. Analyse de la performance
print("\n7. Analyse de la performance (response_time_ms):")
print("a) Statistiques globales de temps de réponse:")
# 'avg' n'est pas reconnu par DataFrame.summary; utiliser 'mean' à la place
logs.select("response_time_ms").summary("count", "min", "max", "mean").show()

# b) Endpoints les plus lents (par latence moyenne, >10 requêtes)
min_requests_threshold = 10
print(f"\nEndpoints les plus lents (Avg Latency, >{min_requests_threshold} requêtes):")
(
    logs.groupBy("endpoint")
    .agg(
        count(lit(1)).alias("request_count"),
        avg("response_time_ms").alias("avg_latency_ms")
    )
    .filter(col("request_count") > min_requests_threshold)
    .orderBy(desc("avg_latency_ms"))
    .show(10, truncate=False)
)

# c) Top 10 des requêtes individuelles les plus lentes
print("\nTop 10 des requêtes individuelles les plus lentes:")
logs.orderBy(desc("response_time_ms")).show(10, truncate=False)

# 8. Modèles de trafic
print("\n8. Modèles de trafic:")
# Ajouter la colonne parsed_timestamp
logs_with_time = logs.withColumn(
    "parsed_timestamp",
    to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"), # Format spécifié [cite: 105, 240]
)

print("a) Trafic par heure:")
traffic_by_hour = (
    logs_with_time.withColumn("hour", hour(col("parsed_timestamp"))) # Extraire l'heure [cite: 108]
    .groupBy("hour").count()
    .orderBy("hour")
)
traffic_by_hour.show()

# 9. Comportement de l'utilisateur
print("\n9. Comportement de l'utilisateur:")
# a) Top 10 des IPs les plus actives
print("a) Top 10 des IPs les plus actives:")
requests_per_ip = logs.groupBy("ip").count().withColumnRenamed("count", "request_count")
requests_per_ip.orderBy(desc("request_count")).show(10)

# b) Statistiques descriptives
print("b) Statistiques descriptives de requests_per_ip:")
requests_per_ip.select("request_count").describe().show()

# c) IPs potentiellement bot (count > 100)
bot_threshold = 100
print(f"\nc) IPs potentiellement bot (count > {bot_threshold}):")
(
    requests_per_ip.filter(col("request_count") > bot_threshold)
    .orderBy(desc("request_count"))
    .show(truncate=False)
)

# 10. Sauvegarde des données traitées et du résumé
print("\n10. Sauvegarde des données traitées et du résumé...")
# a) Sauvegarder logs_with_time en Parquet [cite: 116]
try:
    logs_with_time.write.mode("overwrite").parquet(PARQUET_OUTPUT_PATH)
    print(f"Logs traités sauvegardés en Parquet à: {PARQUET_OUTPUT_PATH}")
except Exception as e:
    print(f"⚠️ Spark Parquet write failed: {e}")
    # Fallback 1: essayer d'écrire en CSV via Spark
    try:
        logs_with_time.coalesce(1).write.csv(PARQUET_OUTPUT_PATH + "_csv_fallback", header=True, mode="overwrite")
        print(f"Fallback: Logs écrits en CSV dans {PARQUET_OUTPUT_PATH}_csv_fallback")
    except Exception as e2:
        print(f"⚠️ Spark CSV fallback failed: {e2}")
        # Fallback 2: écrire localement via le module csv (attention à la mémoire pour de grands jeux)
        try:
            import csv, os
            os.makedirs(PARQUET_OUTPUT_PATH + "_local_fallback", exist_ok=True)
            local_file = os.path.join(PARQUET_OUTPUT_PATH + "_local_fallback", "logs_fallback.csv")
            rows = logs_with_time.collect()
            with open(local_file, "w", newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(logs_with_time.columns)
                for r in rows:
                    writer.writerow([r[c] for c in logs_with_time.columns])
            print(f"Fallback local: écrit {local_file}")
        except Exception as e3:
            print(f"Fallback local also failed: {e3}")

# b) Construire le DataFrame de résumé
total_requests = logs_with_time.count()
unique_ips = logs_with_time.select(countDistinct("ip")).collect()[0][0]
unique_pages = logs_with_time.select(countDistinct("endpoint")).collect()[0][0]

summary_data = [
    ("Total Requests", total_requests),
    ("Valid Requests", total_requests),
    ("Unique IPs", unique_ips),
    ("Unique Pages", unique_pages),
    ("4xx Errors", errors_4xx),
    ("5xx Errors", errors_5xx),
]

summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])

print("\nRésumé des métriques:")
summary_df.show(truncate=False)

# Écrire le résumé en CSV (fichier unique) [cite: 126]
try:
    (
        summary_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(SUMMARY_OUTPUT_PATH)
    )
    print(f"Résumé sauvegardé en CSV (fichier unique) à: {SUMMARY_OUTPUT_PATH}")
except Exception as e:
    print(f"⚠️ Spark CSV write for summary failed: {e}")
    # Fallback: write local CSV via Python
    try:
        import csv, os
        os.makedirs(SUMMARY_OUTPUT_PATH, exist_ok=True)
        local_file = os.path.join(SUMMARY_OUTPUT_PATH, "summary.csv")
        rows = summary_df.collect()
        with open(local_file, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(summary_df.columns)
            for r in rows:
                writer.writerow([r[c] for c in summary_df.columns])
        print(f"Fallback local summary written to: {local_file}")
    except Exception as e2:
        print(f"Fallback local write also failed: {e2}")

# 11. Arrêter Spark
spark.stop()