import os
import random
import snowflake.connector
from faker import Faker
from dotenv import load_dotenv

# Load secrets
load_dotenv()

# Initialize Faker (The fake data generator)
fake = Faker()

def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    return conn

# --- DATA GENERATION FUNCTIONS ---

def generate_suppliers(n=50):
    suppliers = []
    for i in range(1, n + 1):
        suppliers.append({
            'ID': i,
            'NAME': fake.company(),
            'COUNTRY': random.choice(['USA', 'United States', 'US', 'Germany', 'China', 'Mexico', 'Canada']),
            'CREATED_AT': fake.date_between(start_date='-5y', end_date='today').isoformat()
        })
    return suppliers

def generate_products(n=500, supplier_count=50):
    products = []
    for i in range(1, n + 1):
        cost = round(random.uniform(10, 100), 2)
        price = round(cost * random.uniform(1.1, 2.0), 2) # Price is usually 10-100% higher than cost
        
        # Intentional "Dirty Data": 5% of products have price LOWER than cost (Loss leaders or errors)
        if random.random() < 0.05:
            price = round(cost * 0.8, 2)

        products.append({
            'ID': i,
            'NAME': fake.word().capitalize() + " " + fake.word().capitalize(),
            'SUPPLIER_ID': random.randint(1, supplier_count),
            'COST': cost,
            'PRICE': price,
            'IS_ACTIVE': random.choice([True, True, True, False]) # Mostly active
        })
    return products

def generate_orders(n=10000, product_count=500):
    orders = []
    for i in range(1, n + 1):
        orders.append({
            'ID': i,
            'PRODUCT_ID': random.randint(1, product_count),
            'QUANTITY': random.randint(1, 10),
            'ORDER_DATE': fake.date_between(start_date='-1y', end_date='today').isoformat(),
            'STATUS': random.choice(['completed', 'completed', 'pending', 'returned', 'shipped'])
        })
    return orders

# --- LOADING FUNCTIONS ---

def load_data_to_snowflake(data, table_name, create_sql):
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        print(f"Creating table {table_name}...")
        cursor.execute(create_sql)
        
        print(f"Loading {len(data)} rows into {table_name}...")
        
        # Dynamic insert SQL generator
        placeholders = ", ".join(["%s"] * len(data[0]))
        columns = ", ".join(data[0].keys())
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert list of dicts to list of tuples for Snowflake
        values = [list(row.values()) for row in data]
        
        cursor.executemany(sql, values)
        print(f"Successfully loaded {table_name}!")
        
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    print("Generating Fake Data...")
    
    # 1. Suppliers
    suppliers_data = generate_suppliers(50)
    load_data_to_snowflake(
        suppliers_data, 
        "RAW_SUPPLIERS", 
        """
        CREATE OR REPLACE TABLE RAW_SUPPLIERS (
            ID INT, 
            NAME STRING, 
            COUNTRY STRING, 
            CREATED_AT DATE
        )
        """
    )

    # 2. Products
    products_data = generate_products(500, 50)
    load_data_to_snowflake(
        products_data, 
        "RAW_PRODUCTS", 
        """
        CREATE OR REPLACE TABLE RAW_PRODUCTS (
            ID INT, 
            NAME STRING, 
            SUPPLIER_ID INT, 
            COST FLOAT, 
            PRICE FLOAT, 
            IS_ACTIVE BOOLEAN
        )
        """
    )

    # 3. Orders (Batching this because 10k is large)
    orders_data = generate_orders(10000, 500)
    load_data_to_snowflake(
        orders_data, 
        "RAW_ORDERS", 
        """
        CREATE OR REPLACE TABLE RAW_ORDERS (
            ID INT, 
            PRODUCT_ID INT, 
            QUANTITY INT, 
            ORDER_DATE DATE, 
            STATUS STRING
        )
        """
    )
    
    print("All data ingestion complete!")