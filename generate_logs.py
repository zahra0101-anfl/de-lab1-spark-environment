# generate_logs.py
import random
import datetime
import os

# Configuration pour la génération
NUM_LOG_LINES = 10000
OUTPUT_PATH = "spark-data/ecommerce/web_logs.txt"
LOG_DIR = os.path.dirname(OUTPUT_PATH)

# Listes des valeurs aléatoires
IPS = [f"192.168.1.{i}" for i in range(1, 20)]
METHODS = ["GET", "POST", "PUT", "DELETE"]
URLS = [
    "/home", "/products", "/products/123", "/cart", "/checkout",
    "/api/data", "/images/logo.png", "/error_page", "/search?q=spark"
]
STATUS_CODES = [200, 200, 200, 304, 404, 500, 503] # Pondération pour les 200
RESPONSE_TIMES = list(range(10, 300)) + list(range(300, 500)) * 2 + list(range(500, 2000)) # Pondération pour les latences faibles
USER_AGENTS = ["Desktop", "Mobile", "Tablet"]

def generate_log_line():
    """Génère une ligne de log dans le format Apache-like."""
    ip = random.choice(IPS)
    
    # Génère un timestamp pour le log (format dd/MMM/yyyy:HH:mm:ss +0000)
    # Nous utilisons un mois spécifique (Nov) pour simplifier le parsing to_timestamp
    dt = datetime.datetime(2025, 11, random.randint(1, 20), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    timestamp = dt.strftime("%d/%b/%Y:%H:%M:%S +0000") 
    
    method = random.choice(METHODS)
    endpoint = random.choice(URLS)
    status = random.choice(STATUS_CODES)
    response_time = random.choice(RESPONSE_TIMES)
    user_agent = random.choice(USER_AGENTS) # Non inclus dans le pattern final, mais peut être utile

    # Format de log final: IP [timestamp] "METHOD ENDPOINT HTTP/1.1" STATUS RESPONSETIME
    log_line = f'{ip} [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {response_time}'
    return log_line

# Créer le répertoire si non existant
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Écriture des logs dans le fichier
with open(OUTPUT_PATH, 'w') as f:
    for _ in range(NUM_LOG_LINES):
        f.write(generate_log_line() + '\n')

print(f"✅ Fichier de logs créé: {OUTPUT_PATH}")
print(f"✅ {NUM_LOG_LINES} lignes de logs générées.")

# Exécuter la commande: python generate_logs.py