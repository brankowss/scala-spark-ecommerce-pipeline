# scripts/data_generator.py

import os
import random
import time
from datetime import datetime

# --- Configuration ---
OUTPUT_DIR = "generated_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Master Data (Our "database" of products and customers) ---
# In a real-world scenario, this would come from actual databases.
PRODUCTS = {
    "22752": "SET OF 3 CAKE TINS PANTRY DESIGN",
    "21730": "GLASS STAR FROSTED T-LIGHT HOLDER",
    "22633": "HAND WARMER UNION JACK",
    "84879": "ASSORTED COLOUR BIRD ORNAMENT",
    "22745": "POPPY'S PLAYHOUSE BEDROOM",
    "22310": "IVORY KNITTED MUG COSY",
}
CUSTOMERS = [17850, 13047, 12583, 14688, 15100]
COUNTRIES = ["1101-1", "1101-2", "1102-1", "1103-3", "2201-2", "3301-4"]

# --- Main Function ---

def generate_data():
    """
    Main function that continuously generates new transaction logs.
    """
    invoice_counter = 536365 # Start from an example invoice number
    
    print("Starting data generator... Press Ctrl+C to stop.")
    
    while True:
        try:
            # Transaction timestamp
            invoice_date = datetime.now().strftime("%d/%m/%Y %H:%M")
            
            # Pick a random customer
            customer_id = random.choice(CUSTOMERS)
            
            # Create an invoice with 1 to 5 items
            invoice_items = []
            num_items = random.randint(1, 5)
            
            for _ in range(num_items):
                stock_code = random.choice(list(PRODUCTS.keys()))
                quantity = random.randint(1, 10)
                country = random.choice(COUNTRIES)
                
                # Format the line as specified in the task
                line = f"{invoice_counter},{stock_code},{quantity},{invoice_date},{customer_id},{country}"
                invoice_items.append(line)
            
            # Save each invoice to a new, unique file
            file_name = f"invoice_{invoice_counter}_{int(time.time())}.txt"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            with open(file_path, "w") as f:
                for item in invoice_items:
                    f.write(item + "\n")
            
            print(f"Generated invoice {invoice_counter} with {num_items} items in file {file_name}")
            
            invoice_counter += 1
            
            # Wait a few seconds before generating the next invoice
            time.sleep(random.randint(2, 5))
            
        except KeyboardInterrupt:
            print("\nStopping data generator.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

# --- Script Entry Point ---
if __name__ == "__main__":
    generate_data()