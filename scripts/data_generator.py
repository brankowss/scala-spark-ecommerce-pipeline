# scripts/data_generator.py 

import os
import random
import time
from datetime import datetime, date
import csv
import argparse # Library for parsing command-line arguments

# --- Configuration ---
# Directory to store the generated data files.
OUTPUT_DIR = "generated_data"
# Create the output directory if it doesn't already exist.
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- Master Data ---
# This master data simulates the real-world dimension tables.
# In a real system, this information would be read from a database.
PRODUCTS = {
    "22752": "SET OF 3 CAKE TINS PANTRY DESIGN",
    "21730": "GLASS STAR FROSTED T-LIGHT HOLDER",
    "22633": "HAND WARMER UNION JACK",
    "84879": "ASSORTED COLOUR BIRD ORNAMENT",
    "22745": "POPPY'S PLAYHOUSE BEDROOM",
    "22310": "IVORY KNITTED MUG COSY",
}
CUSTOMERS = [17850, 13047, 12583, 14688, 15100]
COUNTRIES = {
    "1101": "United Kingdom",
    "1102": "France",
    "1103": "Germany",
    "5501": "USA",
    "3301": "Japan",
    "1301": "Turkey"
}
REGIONS = ["1", "2", "3", "4", "5", "6"]


def generate_dimension_files():
    """
    Generates the daily dimension files (countries and products).
    This function runs once and simulates the daily data drop.
    """
    # 1. Generate Country File
    country_file_path = os.path.join(OUTPUT_DIR, "countries.csv")
    with open(country_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["CountryID", "CountryName"]) # Write header
        for country_id, country_name in COUNTRIES.items():
            writer.writerow([country_id, country_name])
    print(f"Generated country file at {country_file_path}")

    # 2. Generate Product Info File
    product_file_path = os.path.join(OUTPUT_DIR, "product_info.csv")
    with open(product_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["StockCode", "ProductDescription", "UnitPrice", "Date"]) # Write header
        # For each product, create a price entry for the current date.
        today_date = date.today().strftime("%Y-%m-%d")
        for stock_code, description in PRODUCTS.items():
            unit_price = round(random.uniform(1.0, 50.0), 2)
            writer.writerow([stock_code, description, unit_price, today_date])
    print(f"Generated product info file at {product_file_path}")


def generate_transaction_logs():
    """
    Continuously generates new transaction log files in a loop,
    simulating a live e-commerce application that produces hourly logs.
    """
    invoice_counter = 536365 # Start from an example invoice number
    print("Starting transaction data generator... Press Ctrl+C to stop.")
    
    while True:
        try:
            # Each invoice gets the current timestamp.
            invoice_date = datetime.now().strftime("%d/%m/%Y %H:%M")
            customer_id = random.choice(CUSTOMERS)
            
            # An invoice can contain multiple items (rows in the log file).
            invoice_items = []
            num_items = random.randint(1, 5)
            
            for _ in range(num_items):
                stock_code = random.choice(list(PRODUCTS.keys()))
                quantity = random.randint(1, 10)
                country_id = random.choice(list(COUNTRIES.keys()))
                region_id = random.choice(REGIONS)
                
                # Assemble the composite Country-Region field as specified.
                country_field = f"{country_id}-{region_id}"
                
                # Format the log line as a comma-separated string.
                line = f"{invoice_counter},{stock_code},{quantity},{invoice_date},{customer_id},{country_field}"
                invoice_items.append(line)
            
            # Create a unique filename for each new invoice log.
            file_name = f"invoice_{invoice_counter}_{int(time.time())}.txt"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            with open(file_path, "w") as f:
                for item in invoice_items:
                    f.write(item + "\n")
            
            print(f"Generated invoice {invoice_counter} with {num_items} items.")
            
            invoice_counter += 1
            # Simulate a real-world delay between new transactions.
            time.sleep(random.randint(2, 5))
            
        except KeyboardInterrupt:
            # Allows for a graceful shutdown of the script with Ctrl+C.
            print("\nStopping data generator.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

# --- Script Entry Point ---
# This block runs when the script is executed directly from the command line.
if __name__ == "__main__":
    # Set up the command-line argument parser.
    parser = argparse.ArgumentParser(description="E-commerce Data Generator for the DE Pipeline")
    parser.add_argument(
        '--type', 
        type=str, 
        required=True, 
        choices=['dimensions', 'transactions'],
        help="Type of data to generate: 'dimensions' (for daily files) or 'transactions' (for continuous logs)."
    )
    
    args = parser.parse_args()

    # Execute the appropriate function based on the provided argument.
    if args.type == 'dimensions':
        generate_dimension_files()
    elif args.type == 'transactions':
        generate_transaction_logs()