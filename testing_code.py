import sqlite3
import os
import pandas as pd
from google import genai
from google.genai import types

from google.api_core import retry
import random
from datetime import datetime, timedelta

is_retriable = lambda e: (isinstance(e, genai.errors.APIError) and e.code in {429, 503})

if not hasattr(genai.models.Models.generate_content, '__wrapped__'):
  genai.models.Models.generate_content = retry.Retry(
      predicate=is_retriable)(genai.models.Models.generate_content)
  
# Set the API key for Google GenAI
GOOGLE_API_KEY =  os.environ.get("GOOGLE_API_KEY")

db_path = 'Kitchen_Pantry.db'

# Check if the database file exists, if not create it

def create_database(db_path):
    """Create a SQLite database and a table for storing Pantry data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a table for user data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pantry_test(
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product TEXT NOT NULL,
            type TEXT NOT NULL,
            purchase_date DATE NOT NULL,
            expiration_date DATE NOT NULL,
            quantity INTEGER NOT NULL,
            units_full TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()

if not os.path.exists(db_path):
    create_database(db_path)
    print(f"Database {db_path} does not exist. Creating a new one...")
else:
    print(f"Database {db_path} already exists.")



def describe_table(table_name: str) -> list[tuple]:
    """Describe the schema of a table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    schema = cursor.fetchall()
    
    conn.close()
    
    return schema

def list_tables() -> list[str]:
    """List all tables in the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print("Listing Columns in the Table:")
    tables = cursor.fetchall()
    
    
    conn.close()
    
    return [table[0] for table in tables]

def list_products() -> list[tuple]:
    """List all products in the pantry."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM pantry_test")
    products = cursor.fetchall()
    
    conn.close()
    
    return products

def execute_query(query: str) -> list[tuple]:
    """Execute a query and return the results."""
    conn = sqlite3.connect(db_path)
    print(f"Executing query: {query}")
    cursor = conn.cursor()
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    conn.close()
    
    return results

def upload_data(file:str) -> list[tuple]:
    """Upload data from a DataFrame to the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    file = input("Please enter the file path of the csv format: ")
    data = pd.read_csv(file,encoding='utf-8')
    data = data.rename(columns={
        'product_id': 'product_id',
        'product': 'product',
        'type': 'type',
        'purchase_date': 'purchase_date',
        'expiration_date': 'expiration_date',
        'quantity': 'quantity',
        'units_full': 'units_full'
    })
    
    data.to_sql('pantry_test', conn, if_exists='append', index=False)
    
    conn.commit()
    conn.close()


def generate_groceries_with_dates(num_items=8):
    # List of potential grocery items
    grocery_items = [
        {"product": "Apples", "type": "Fruit", "units_full": "kg"},
        {"product": "Milk", "type": "Dairy", "units_full": "liters"},
        {"product": "Rice", "type": "Grain", "units_full": "kg"},
        {"product": "Potatoes", "type": "Meat", "units_full": "kg"},
        {"product": "Carrots", "type": "Vegetable", "units_full": "kg"},
        {"product": "Turmeric", "type": "Spices", "units_full": "g"},
        {"product": "Bread", "type": "Bakery", "units_full": "loaves"},
        {"product": "Butter", "type": "Dairy", "units_full": "packs"},
        {"product": "Eggs", "type": "Dairy", "units_fullit": "dozens"},
        {"product": "Cheese", "type": "Dairy", "units_full": "blocks"},
        {"product": "Tomatoes", "type": "Vegetable", "units_full": "kg"}
    ]
    
    # Generate random items with quantity, purchase date, and expiration date
    groceries_list = []
    for _ in range(num_items):
        item = random.choice(grocery_items)
        product_id = random.randint(1000, 5000)  # Random product ID
        quantity = random.randint(1, 10)  # Random quantity between 1 and 10
        purchase_date = datetime.now() - timedelta(days=random.randint(0, 30))  # Random date in the last 30 days
        expiration_date = purchase_date + timedelta(days=random.randint(1, 15))  # Expiration within 1 to 15 days of purchase
        groceries_list.append({
            "product_id": product_id,
            "product": item["product"],
            "type": item["type"],
            "units_full": item["units_full"],
            "quantity": quantity,
            "purchase_date": purchase_date.strftime("%Y-%m-%d"),
            "expiration_date": expiration_date.strftime("%Y-%m-%d")
        })
    print("Generated synthetic data:")

    return groceries_list

def add_product(groceries_list):
    """Add a product to the pantry."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for item in groceries_list:
        # Add the product to the pantry_test table
        cursor.execute('''
            INSERT INTO pantry_test (product_id, product, type, purchase_date, expiration_date, quantity, units_full)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (item["product_id"],item["product"], item["type"], item["purchase_date"], item["expiration_date"], item["quantity"], item["units_full"]))
    
    conn.commit()
    conn.close()

def synthetic_data():
    """Generate synthetic data for the pantry."""
    groceries_list = generate_groceries_with_dates()
    add_product(groceries_list)
    print("Synthetic data added to the pantry.")


db_tools = [synthetic_data , list_tables, list_products, execute_query, upload_data, describe_table]

instructions_Alron = """
Your a Pantry Database chatbot known as Alron that can interact with SQL databases for a Kitchen Pantry. You will greet the user
in polite manner and ask them how you can help them in Kitchen pantry. First you will ask to user if they would like to 
generate synthetic data for pantry or would like to upload the data from excel file.Wait for the user to respond 30 secs.
After the data is generated or upload is complete you will thank the user in a Playful manner. Then you will ask the user for any questions they may have regarding the pantry. 
You will then wait for the user to ask you a question for 30 seconds and if they do not ask a question you will say goodbye and end the conversation. 
On receiving a question you will turn the question into SQL queries using the tools avialable to you. You will return the results of the query in a human readable format.

decribe_table function will be used to check the schema of the Database.for uploading data upload_data function. for generating synthetic data you will use synthetic_data function.
For listing the tables in the database Alron will use list_tables function. For listing all products in the pantry Alron will use list_products function.
For executing any query Alron will check schema of the database and use execute_query function to issue SQL SELECT query
"""

client = genai.Client(api_key=GOOGLE_API_KEY)

chat = client.chats.create(
    model="gemini-2.0-flash",
    config=types.GenerateContentConfig(
        system_instruction=instructions_Alron,
        tools=db_tools,
    ),
)

response = chat.send_message("how many fruits do I have in my pantry?")
print("Alron: ", response.text)