import pymysql
import csv

# Load config from file
def load_config(file_path):
    config = {}
    with open(file_path, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip()
    return config

config = load_config("conn.txt")

# Establish connection
conn = pymysql.connect(
    host=config["db_host"],
    user=config["db_user"],
    password=config["db_password"],
    database=config["db_name"]
)

csv_path = config["csv_path"]
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders( 
    order_id BIGINT PRIMARY KEY, 
    user_id BIGINT, 
    item_id INT, 
    quantity INT, 
    price DECIMAL(10,2), 
    order_ts TIMESTAMP, 
    status VARCHAR(20)
);
""")

# Insert from CSV
try:
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sql = """
            INSERT INTO orders (order_id, user_id, item_id, quantity, price, order_ts, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                int(row['order_id']),
                int(row['user_id']),
                int(row['item_id']),
                int(row['quantity']),
                float(row['price']),
                row['order_ts'].replace("T", " ").replace("Z", ""),
                row['status']
            )
            cursor.execute(sql, values)
    conn.commit()
    print("Data inserted successfully!")

except FileNotFoundError:
    print(f"File not found: {csv_path}")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

cursor.close()
conn.close()
