# lab4_caching_demo.py
import time
from pyspark.sql import SparkSession

LOG_FILE_PATH = "spark-data/ecommerce/web_logs.txt"

spark = (
    SparkSession.builder.appName("Day1-CachingDemo")
    .master("local[*]")
    .getOrCreate()
)

logs = spark.read.text(LOG_FILE_PATH)

print("--- Sans Caching ---")
start1 = time.time()
logs.count() # 1er count - Lecture à partir du disque
end1 = time.time()
print(f"1er count (sans cache) : {end1 - start1:.4f}s")

start2 = time.time()
logs.count() # 2ème count - Lecture à partir du disque
end2 = time.time()
print(f"2ème count (sans cache) : {end2 - start1:.4f}s")
time_without_cache = end2 - start1

print("\n--- Avec Caching ---")
logs_cached = logs.cache() # Marquer pour le cache
logs_cached.count() # 1ère action après cache - Exécution et chargement en mémoire

start3 = time.time()
logs_cached.count() # 1er count (avec cache)
end3 = time.time()
print(f"1er count (avec cache) : {end3 - start3:.4f}s")
time_cached_1 = end3 - start3

start4 = time.time()
logs_cached.count() # 2ème count (avec cache) - Lecture depuis la mémoire
end4 = time.time()
print(f"2ème count (avec cache) : {end4 - start4:.4f}s")
time_cached_2 = end4 - start4

speedup = time_cached_1 / time_cached_2 if time_cached_2 > 0 else float('inf')
print(f"\nTemps sans cache (2e exécution): {time_without_cache:.4f}s")
print(f"Temps avec cache (2e exécution): {time_cached_2:.4f}s")
print(f"Speedup théorique (1e temps cache / 2e temps cache) : {speedup:.2f}x")

input("\nAppuyez sur Entrée pour continuer et inspecter l'onglet Storage...")

logs_cached.unpersist()
spark.stop()