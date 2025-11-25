# Suite de lab4_log_analysis.py ou lab4_challenges.py

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, count, lit, avg, sum, window, date_format, round, desc, asc,
    regexp_extract, to_timestamp, hour, when, countDistinct
)

# --- Setup: create SparkSession and parse logs (so this file is self-contained) ---
from pyspark.sql import SparkSession

LOG_FILE_PATH = "spark-data/ecommerce/web_logs.txt"

spark = (
    SparkSession.builder.appName("Day1-Challenges")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

# Read raw logs and parse using the same pattern as lab4_log_analysis
raw_logs = spark.read.text(LOG_FILE_PATH)
log_pattern = r'^(\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d+) (\d+)$'
parsed_logs = (
    raw_logs.select(
        regexp_extract(col("value"), log_pattern, 1).alias("ip"),
        regexp_extract(col("value"), log_pattern, 2).alias("timestamp"),
        regexp_extract(col("value"), log_pattern, 3).alias("method"),
        regexp_extract(col("value"), log_pattern, 4).alias("endpoint"),
        regexp_extract(col("value"), log_pattern, 6).cast("int").alias("status"),
        regexp_extract(col("value"), log_pattern, 7).cast("int").alias("response_time_ms"),
    ).where(col("ip") != "")
)

# Add parsed timestamp column and prepare logs_with_time and traffic_by_hour
logs_with_time = parsed_logs.withColumn(
    "parsed_timestamp",
    to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"),
)

traffic_by_hour = (
    logs_with_time.withColumn("hour", hour(col("parsed_timestamp")))
    .groupBy("hour").count()
    .orderBy("hour")
)


# ... (Assurez-vous que logs_with_time est chargé) ...
print("\n" + "="*50)
print("=== 4. Partie D - Problèmes de Challenge ===")
print("="*50)

# Challenge 1 - Bounce Rate
print("\nChallenge 1: Taux de Rebond (Bounce Rate)")
ip_counts = logs_with_time.groupBy("ip").count().withColumnRenamed("count", "request_count")
total_unique_ips = ip_counts.count()
bounced_ips = ip_counts.filter(col("request_count") == 1).count() # IPs avec un seul request [cite: 196]
bounce_rate = (bounced_ips / total_unique_ips) * 100 if total_unique_ips > 0 else 0

print(f"Total des IPs uniques : {total_unique_ips}")
print(f"IPs rebondissantes (1 requête) : {bounced_ips}")
print(f"Taux de Rebond : {bounce_rate:.2f}%")

# Challenge 2 - Entonnoir de Conversion (Conversion Funnel)
print("\nChallenge 2: Entonnoir de Conversion (Unique IPs per Step)")

# Définir les conditions de l'entonnoir
funnel_steps = {
    "/home": col("endpoint") == "/home",
    "/products": col("endpoint") == "/products",
    "/product/*": col("endpoint").startswith("/product/"),
    "/cart": col("endpoint") == "/cart",
    "/checkout": col("endpoint") == "/checkout",
}

funnel_counts = []
for step_name, condition in funnel_steps.items():
    # Compter le nombre d'IPs uniques qui ont rencontré l'endpoint [cite: 215]
    unique_ips_count = (
        logs_with_time.filter(condition)
        .select(countDistinct("ip")).collect()[0][0]
    )
    funnel_counts.append((step_name, unique_ips_count))

funnel_df = spark.createDataFrame(funnel_counts, ["Step", "Unique_IPs"])
funnel_df.show(truncate=False)

# Challenge 3 - Heure de Pointe du Trafic
print("\nChallenge 3: Heure de Pointe du Trafic")
# Utilise traffic_by_hour de la Partie B (8.a)
total_requests = traffic_by_hour.select(sum("count")).collect()[0][0]

if traffic_by_hour.count() == 0:
    print("Aucune donnée de trafic par heure disponible (traffic_by_hour vide).")
    peak_hour = None
else:
    traffic_with_percentage = (
        traffic_by_hour.withColumn(
            "percentage", round((col("count") / total_requests) * 100, 2)
        )
        .orderBy("hour")
    )

    print("Distribution du trafic par heure (avec pourcentage):")
    traffic_with_percentage.show()
    peak_hour = traffic_with_percentage.orderBy(desc("count")).first()
    if peak_hour:
        print(f"L'heure de pointe est {peak_hour['hour']} avec {peak_hour['count']} requêtes ({peak_hour['percentage']}%)")
    else:
        print("Impossible de déterminer l'heure de pointe (peak_hour vide).")

# Challenge 4 - Activité Suspecte
print("\nChallenge 4: Activité Suspecte")

# a) IPs avec >50 requêtes par minute
print("a) IPs avec >50 requêtes par minute (IPs abusives):")
# Fenêtre glissante de 1 minute [cite: 230]
requests_per_min = (
    logs_with_time.groupBy(
        "ip",
        window(col("parsed_timestamp"), "1 minute").alias("time_window")
    )
    .agg(count(lit(1)).alias("requests_per_min"))
    .filter(col("requests_per_min") > 50)
    .select("ip", "requests_per_min", date_format(col("time_window.start"), "HH:mm:ss").alias("minute_start"))
    .orderBy(desc("requests_per_min"))
)
requests_per_min.show(truncate=False)

# b) IPs avec un taux élevé de 404 (>50% de leurs requêtes)
print("\nb) IPs avec un taux élevé de 404 (>50%):")
ip_error_summary = (
    logs_with_time.groupBy("ip")
    .agg(
        count(lit(1)).alias("total_requests"),
        sum(when(col("status") == 404, 1).otherwise(0)).alias("404_requests")
    )
    .withColumn(
        "404_rate", round((col("404_requests") / col("total_requests")) * 100, 2)
    )
    .filter((col("total_requests") > 10) & (col("404_rate") > 50)) # >10 requêtes pour éviter le bruit
    .orderBy(desc("404_rate"))
)
ip_error_summary.show(truncate=False)

# Challenge 5 - Goulots d'Étranglement de Performance
print("\nChallenge 5: Goulots d'Étranglement de Performance")
avg_latency_by_endpoint = (
    logs_with_time.groupBy("endpoint")
    .agg(
        count(lit(1)).alias("hit_count"),
        avg("response_time_ms").alias("avg_latency_ms")
    )
    .filter((col("hit_count") > 10) & (col("avg_latency_ms") > 2000)) # >10 hits et >2000ms [cite: 233]
    .orderBy(desc("avg_latency_ms"))
)
print("Endpoints avec Avg Latency > 2000ms et > 10 hits:")
avg_latency_by_endpoint.show(truncate=False)

# Analyse relation avec le code de statut [cite: 234]
if avg_latency_by_endpoint.count() > 0:
    slow_endpoints = avg_latency_by_endpoint.select("endpoint").rdd.flatMap(lambda x: x).collect()
    print("\nAnalyse Latency par Endpoint et Status pour les endpoints lents:")
    (
        logs_with_time.filter(col("endpoint").isin(slow_endpoints))
        .groupBy("endpoint", "status")
        .agg(avg("response_time_ms").alias("avg_latency_ms"))
        .orderBy(desc("avg_latency_ms"))
        .show(truncate=False)
    )

# BONUS : Performance par Heure
print("\nBONUS : Performance moyenne par Heure de la Journée:")
(
    logs_with_time.withColumn("hour", hour(col("parsed_timestamp")))
    .groupBy("hour")
    .agg(
        avg("response_time_ms").alias("avg_latency_ms"),
        count(lit(1)).alias("total_requests")
    )
    .orderBy(desc("avg_latency_ms"))
    .show()
)