# scripts/data_generator.py

"""
E-commerce Data Generator for the DE Pipeline.

This script simulates the generation of raw data for the data pipeline.
It is designed to create a finite batch of historical data (for the last 5 days)
and then exit, making it suitable for a daily batch processing workflow.
"""

import os
import random
import time
from datetime import datetime, date, timedelta
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
    """
    Generates dimension files with data for the last 5 days.
    """
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
        for i in range(5):
            day = date.today() - timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            for stock_code, description in PRODUCTS.items():
                unit_price = round(random.uniform(10.0, 50.0), 2)
                writer.writerow([stock_code, description, unit_price, day_str])
    print(f"Generated product info file at {product_file_path} for the last 5 days.")

def generate_transaction_logs():
    """
    Generates a finite batch of transaction log files, distributed
    across the last 5 days to create a realistic dataset for BI dashboards.
    """
    invoice_counter = 536365
    print("Generating a batch of historical transaction data...")
    
    for i in range(5):
        current_date = datetime.now() - timedelta(days=i)
        
        for _ in range(random.randint(5, 10)):
            invoice_date = current_date.strftime("%d/%m/%Y %H:%M")
            customer_id = random.choice(CUSTOMERS)
            invoice_items = []
            num_items = random.randint(1, 5)
            
            for _ in range(num_items):
                stock_code = random.choice(list(PRODUCTS.keys()))
                quantity = random.randint(1, 10)

                # --- NOTE FOR MENTOR: TESTING LOGIC FOR EP-7 ---
                # To reliably test the business alert, the following block of code was
                # temporarily used to guarantee the creation of a high-quantity outlier.
                # It has been commented out for the final production version.
                #
                # if stock_code == "22752":
                #     quantity = 9999
                # else:
                #     quantity = random.randint(1, 10)
                # --- END OF TESTING LOGIC ---

                country_field = f"{random.choice(list(COUNTRIES.keys()))}-{random.choice(REGIONS)}"
                line = f"{invoice_counter},{stock_code},{quantity},{invoice_date},{customer_id},{country_field}"
                invoice_items.append(line)
            
            file_name = f"invoice_{invoice_counter}_{int(time.time())}.txt"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            with open(file_path, "w") as f:
                f.write("\n".join(invoice_items) + "\n")
            
            print(f"Generated invoice {invoice_counter} for date {current_date.strftime('%Y-%m-%d')}.")
            invoice_counter += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Data Generator")
    parser.add_argument(
        '--type', 
        required=True, 
        choices=['dimensions', 'transactions'],
        help="Type of data to generate: 'dimensions' or 'transactions'."
    )
    
    args = parser.parse_args()

    if args.type == 'dimensions':
        generate_dimension_files()
    elif args.type == 'transactions':
        generate_transaction_logs()
