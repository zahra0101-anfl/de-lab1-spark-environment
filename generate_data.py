import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# --- Configuration des Volumes de Données ---
NUM_CUSTOMERS = 1000  # 1000 clients
NUM_PRODUCTS = 100    # 100 produits
NUM_ORDERS = 5000     # 5000 commandes
OUTPUT_DIR = "."      # Le script s'exécute dans spark-data/ecommerce/

# Initialisation de Faker pour des données réalistes
fake = Faker('en_US')

print(f"Démarrage de la génération de {NUM_CUSTOMERS} clients, {NUM_PRODUCTS} produits, et {NUM_ORDERS} commandes...")

# ====================================================================
## 1. Génération des Données Clients (customers.csv)
# ====================================================================

customer_data = []
customer_numbers = [10000 + i for i in range(NUM_CUSTOMERS)]
segments = ['Enterprise', 'SMB', 'Individual', 'Mid-Market'] 

for i in range(NUM_CUSTOMERS):
    customer_data.append({
        'customerNumber': customer_numbers[i], 
        'customerName': fake.company(),
        'contactFirstName': fake.first_name(),
        'contactLastName': fake.last_name(),
        'phone': fake.phone_number(),
        'addressLine1': fake.street_address(),
        'city': fake.city(),
        'state': fake.state_abbr() if random.random() < 0.9 else None, 
        'country': fake.country(),
        'creditLimit': round(random.uniform(10000, 250000), 2),
        'customerSegment': random.choice(segments)
    })

customers_df = pd.DataFrame(customer_data)
customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False) # Ligne corrigée
print(f"-> customers.csv généré avec {len(customers_df)} enregistrements.")

# ====================================================================
## 2. Génération des Données Produits (products.csv)
# ====================================================================

product_data = []
product_codes = [f"P_{100 + i}" for i in range(NUM_PRODUCTS)]
categories = ['Electronics', 'Home Goods', 'Apparel', 'Books', 'Tools', 'Services'] 

for i in range(NUM_PRODUCTS):
    buy_price = round(random.uniform(5, 750), 2)
    msrp = round(buy_price * random.uniform(1.2, 2.5), 2)

    product_data.append({
        'productCode': product_codes[i],
        'productName': fake.catch_phrase(),
        'productCategory': random.choice(categories),
        'quantityInStock': random.randint(0, 7000),
        'buyPrice': buy_price,
        'MSRP': msrp
    })

products_df = pd.DataFrame(product_data)
products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False) # Ligne corrigée
print(f"-> products.csv généré avec {len(products_df)} enregistrements.")


# ====================================================================
## 3. Génération des Données Commandes (orders.csv)
# ====================================================================

order_data = []
order_numbers = [200000 + i for i in range(NUM_ORDERS)] 
statuses = ['Shipped', 'Delivered', 'In Process', 'Disputed', 'Cancelled']
payments = ['Credit Card', 'Bank Transfer', 'COD', 'Invoice']
start_date = datetime(2024, 1, 1)

for i in range(NUM_ORDERS):
    order_date = start_date + timedelta(days=random.randint(0, 300), hours=random.randint(0, 24))
    required_date = order_date + timedelta(days=random.randint(2, 10))
    
    payment_method = random.choice(payments) if random.random() < 0.98 else None 

    order_data.append({
        'orderNumber': order_numbers[i],
        'orderDate': order_date.strftime('%Y-%m-%d'),
        'requiredDate': required_date.strftime('%Y-%m-%d'),
        'status': random.choice(statuses),
        'customerNumber': random.choice(customer_numbers),
        'totalAmount': round(random.uniform(50, 7500), 2),
        'paymentMethod': payment_method
    })

orders_df = pd.DataFrame(order_data)
orders_df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False) # Ligne corrigée
print(f"-> orders.csv généré avec {len(orders_df)} enregistrements.")

print("\nOpération de génération de données terminée. Les 3 fichiers sont prêts pour Spark.")