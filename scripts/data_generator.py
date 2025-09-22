# scripts/data_generator.py

"""
E-commerce Data Generator for the DE Pipeline.

This script simulates the generation of raw data for the data pipeline.
It can be run in two modes, controlled by the --type argument:
1.  'dimensions': Creates one-time CSV files for products and countries.
2.  'transactions': Creates a stream of TXT log files for sales transactions.

It also includes a special --test-mode 'outlier' for the 'transactions' type,
which guarantees the creation of a high-quantity transaction for a specific
product to reliably test the business logic alert system.
"""

import os
import random
import time
from datetime import datetime, date
import csv
import argparse

# --- Configuration ---
OUTPUT_DIR = "generated_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Master Data ---
PRODUCTS = {
    "22752": "SET OF 3 CAKE TINS PANTRY DESIGN", "21730": "GLASS STAR FROSTED T-LIGHT HOLDER",
    "22633": "HAND WARMER UNION JACK", "84879": "ASSORTED COLOUR BIRD ORNAMENT",
    "22745": "POPPY'S PLAYHOUSE BEDROOM", "22310": "IVORY KNITTED MUG COSY",
}
CUSTOMERS = [17850, 13047, 12583, 14688, 15100]
COUNTRIES = {
    "1101": "United Kingdom", "1102": "France", "1103": "Germany",
    "5501": "USA", "3301": "Japan", "1301": "Turkey"
}
REGIONS = ["1", "2", "3", "4", "5", "6"]

def generate_dimension_files():
    """Generates the daily dimension files (countries and products)."""
    country_file_path = os.path.join(OUTPUT_DIR, "countries.csv")
    with open(country_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["CountryID", "CountryName"])
        for country_id, country_name in COUNTRIES.items():
            writer.writerow([country_id, country_name])
    print(f"Generated country file at {country_file_path}")

    product_file_path = os.path.join(OUTPUT_DIR, "product_info.csv")
    with open(product_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["StockCode", "ProductDescription", "UnitPrice", "Date"])
        today_date = date.today().strftime("%Y-%m-%d")
        for stock_code, description in PRODUCTS.items():
            unit_price = round(random.uniform(10.0, 50.0), 2)
            writer.writerow([stock_code, description, unit_price, today_date])
    print(f"Generated product info file at {product_file_path}")

def generate_transaction_logs(test_mode=None):
    """Generates transaction logs. Can run in a special 'outlier' test mode."""
    invoice_counter = 536365
    print("Starting transaction data generator...")

    # --- NOTE: TESTING LOGIC FOR EP-7 ---
    # The following block was used to reliably test the business alert.
    # It runs only if the script is called with '--test-mode outlier'.
    # It is commented out for the final production version.
    #
    # if test_mode == 'outlier':
    #     print("--- RUNNING IN OUTLIER TEST MODE ---")
    #     invoice_date = datetime.now().strftime("%d/%m/%Y %H:%M")
    #     customer_id = random.choice(CUSTOMERS)
        
    #     outlier_items = [f"{invoice_counter},22752,999,{invoice_date},{customer_id},1101-1"]
    #     for _ in range(5):
    #          outlier_items.append(f"{invoice_counter},22752,{random.randint(1,10)},{invoice_date},{customer_id},1102-2")

    #     file_name = f"invoice_{invoice_counter}_{int(time.time())}.txt"
    #     file_path = os.path.join(OUTPUT_DIR, file_name)
    #     with open(file_path, "w") as f:
    #         f.write("\n".join(outlier_items) + "\n")
    #     print(f"Generated GUARANTEED OUTLIER invoice {invoice_counter}.")
    #     invoice_counter += 1
    # --- END OF TESTING LOGIC ---

    # Main loop for generating normal, random data.
    print("Generating a batch of normal transaction data...")
    for i in range(5): # Generate 5 additional normal invoices.
        invoice_date = datetime.now().strftime("%d/%m/%Y %H:%M")
        customer_id = random.choice(CUSTOMERS)
        invoice_items = []
        num_items = random.randint(1, 5)
        
        for _ in range(num_items):
            stock_code = random.choice(list(PRODUCTS.keys()))
            quantity = random.randint(1, 10)
            country_field = f"{random.choice(list(COUNTRIES.keys()))}-{random.choice(REGIONS)}"
            line = f"{invoice_counter},{stock_code},{quantity},{invoice_date},{customer_id},{country_field}"
            invoice_items.append(line)
        
        file_name = f"invoice_{invoice_counter}_{int(time.time())}.txt"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        
        with open(file_path, "w") as f:
            f.write("\n".join(invoice_items) + "\n")
        
        print(f"Generated normal invoice {invoice_counter} with {num_items} items.")
        invoice_counter += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Data Generator")
    parser.add_argument('--type', required=True, choices=['dimensions', 'transactions'])
    parser.add_argument('--test-mode', choices=['outlier'], help="Run in a special test mode.")
    
    args = parser.parse_args()

    if args.type == 'dimensions':
        generate_dimension_files()
    elif args.type == 'transactions':
        generate_transaction_logs(test_mode=args.test_mode)
