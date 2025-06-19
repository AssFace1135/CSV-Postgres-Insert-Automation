import psycopg2
import csv
import os
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional
import shutil

# Configuration for each table:
#   - csv_file: Path to the original CSV.
#   - db_table_name: Name of the table in the database.
#   - id_column_in_db: Primary key column in the database table.
#   - natural_key_in_csv: A column in the CSV that can uniquely identify a row
#                         and is also present in the DB to fetch its actual ID.
#                         (e.g., 'email' for Customer, 'supplier_name' for Auction_Supplier).
#                         If None, implies row index might be used or ID is fetched differently.
#   - db_column_for_natural_key: The corresponding DB column for 'natural_key_in_csv'.
#   - fk_configs: List of foreign key configurations for this table.
#     - csv_fk_column: The FK column in the current table's CSV.
#     - parent_config_key: The key in TABLE_CONFIGS for the parent table.
#     - placeholder_is_1_based_index: True if the placeholder FK value in the CSV
#                                     (e.g., '1') refers to the 1st row of the parent's CSV.

TABLE_CONFIGS = {
    "Car_Condition_Rating": {
        "csv_file": "data/car_condition_rating.csv", "db_table_name": "Car_Condition_Rating",
        "id_column_in_db": "condition_id", "natural_key_in_csv": "rating_code",
        "db_column_for_natural_key": "rating_code", "fk_configs": []
    },
    "Engine_Type_Lookup": {
        "csv_file": "data/engine_type_lookup.csv", "db_table_name": "Engine_Type_Lookup",
        "id_column_in_db": "engine_type_id", "natural_key_in_csv": "engine_code",
        "db_column_for_natural_key": "engine_code", "fk_configs": []
    },
    "Transmission_Type_Lookup": {
        "csv_file": "data/transmission_type_lookup.csv", "db_table_name": "Transmission_Type_Lookup",
        "id_column_in_db": "transmission_type_id", "natural_key_in_csv": "type_name",
        "db_column_for_natural_key": "type_name", "fk_configs": []
    },
    "Drivetrain_Type_Lookup": {
        "csv_file": "data/drivetrain_type_lookup.csv", "db_table_name": "Drivetrain_Type_Lookup",
        "id_column_in_db": "drivetrain_type_id", "natural_key_in_csv": "type_name",
        "db_column_for_natural_key": "type_name", "fk_configs": []
    },
    "Auction_Supplier": {
        "csv_file": "data/auction_supplier.csv", "db_table_name": "Auction_Supplier",
        "id_column_in_db": "supplier_id", "natural_key_in_csv": "supplier_name",
        "db_column_for_natural_key": "supplier_name", "fk_configs": []
    },
    "Employee": {
        "csv_file": "data/employee.csv", "db_table_name": "Employee",
        "id_column_in_db": "employee_id", "natural_key_in_csv": "email",
        "db_column_for_natural_key": "email", "fk_configs": []
    },
    "Customer": {
        "csv_file": "data/customer.csv", "db_table_name": "Customer",
        "id_column_in_db": "customer_id", "natural_key_in_csv": "email",
        "db_column_for_natural_key": "email", "fk_configs": []
    },
    "Car": {
        "csv_file": "data/car.csv", "db_table_name": "Car",
        "id_column_in_db": "car_id", "natural_key_in_csv": "vin",
        "db_column_for_natural_key": "vin",
        "fk_configs": [
            {"csv_fk_column": "engine_type_id", "parent_config_key": "Engine_Type_Lookup", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "transmission_type_id", "parent_config_key": "Transmission_Type_Lookup", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "drivetrain_type_id", "parent_config_key": "Drivetrain_Type_Lookup", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "condition_id", "parent_config_key": "Car_Condition_Rating", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "supplier_id", "parent_config_key": "Auction_Supplier", "placeholder_is_1_based_index": True},
        ]
    },
    "Salary": {
        "csv_file": "data/salary.csv", "db_table_name": "Salary",
        "id_column_in_db": "salary_id", "natural_key_in_csv": None, # Identified by combination, or assume order
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "employee_id", "parent_config_key": "Employee", "placeholder_is_1_based_index": True},
        ]
    },
    "Saved_Address": {
        "csv_file": "data/saved_address.csv", "db_table_name": "Saved_Address",
        "id_column_in_db": "saved_address_id", "natural_key_in_csv": None, # FK is the main identifier
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Customer_Preference": {
        "csv_file": "data/customer_preference.csv", "db_table_name": "Customer_Preference",
        "id_column_in_db": "preference_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Shopping_Cart": {
        "csv_file": "data/shopping_cart.csv", "db_table_name": "Shopping_Cart",
        "id_column_in_db": "cart_id", "natural_key_in_csv": None, # FK is the main identifier
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Wishlist": {
        "csv_file": "data/wishlist.csv", "db_table_name": "Wishlist",
        "id_column_in_db": "wishlist_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Search_History": {
        "csv_file": "data/search_history.csv", "db_table_name": "Search_History",
        "id_column_in_db": "search_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Customer_Activity_Log": {
        "csv_file": "data/customer_activity_log.csv", "db_table_name": "Customer_Activity_Log",
        "id_column_in_db": "activity_id", "natural_key_in_csv": None, # Needs a composite key from CSV or rely on order
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
        ]
    },
    "Order": {
        "csv_file": "data/order.csv", "db_table_name": "Order",
        "id_column_in_db": "order_id", "natural_key_in_csv": None, # Needs composite key from CSV
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "managed_by_employee_id", "parent_config_key": "Employee", "placeholder_is_1_based_index": True},
        ]
    },
    "Cart_Item": {
        "csv_file": "data/cart_item.csv", "db_table_name": "Cart_Item",
        "id_column_in_db": "cart_item_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "cart_id", "parent_config_key": "Shopping_Cart", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Wishlist_Item": {
        "csv_file": "data/wishlist_item.csv", "db_table_name": "Wishlist_Item",
        "id_column_in_db": "wishlist_item_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "wishlist_id", "parent_config_key": "Wishlist", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Product_View_History": {
        "csv_file": "data/product_view_history.csv", "db_table_name": "Product_View_History",
        "id_column_in_db": "product_view_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Review": {
        "csv_file": "data/review.csv", "db_table_name": "Review",
        "id_column_in_db": "review_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Order_Item": {
        "csv_file": "data/order_item.csv", "db_table_name": "Order_Item",
        "id_column_in_db": "order_item_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "order_id", "parent_config_key": "Order", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Payment_Transaction": {
        "csv_file": "data/payment_transaction.csv", "db_table_name": "Payment_Transaction",
        "id_column_in_db": "transaction_id", "natural_key_in_csv": None,
        "db_column_for_natural_key": None,
        "fk_configs": [
            {"csv_fk_column": "order_id", "parent_config_key": "Order", "placeholder_is_1_based_index": True},
        ]
    },
    "Shipping_Logistics": {
        "csv_file": "data/shipping_logistics.csv", "db_table_name": "Shipping_Logistics",
        "id_column_in_db": "shipping_id", "natural_key_in_csv": "tracking_number",
        "db_column_for_natural_key": "tracking_number",
        "fk_configs": [
            {"csv_fk_column": "order_id", "parent_config_key": "Order", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
        ]
    },
    "Activity_Data": {
        "csv_file": "data/activity_data.csv", "db_table_name": "Activity_Data",
        "id_column_in_db": "activity_data_id", "natural_key_in_csv": "activity_data_id", # PK is not auto-generated
        "db_column_for_natural_key": "activity_data_id",
        "fk_configs": [
            {"csv_fk_column": "activity_id", "parent_config_key": "Customer_Activity_Log", "placeholder_is_1_based_index": True},
            {"csv_fk_column": "wishlist_item_id", "parent_config_key": "Wishlist_Item", "placeholder_is_1_based_index": True}, # Assuming placeholder refers to index in wishlist_item.csv
            {"csv_fk_column": "cart_item_id", "parent_config_key": "Cart_Item", "placeholder_is_1_based_index": True}, # Assuming placeholder refers to index in cart_item.csv
        ]
    },
}

# This order should match the insertion order in main.py
PROCESSING_ORDER = [
    "Car_Condition_Rating", "Engine_Type_Lookup", "Transmission_Type_Lookup", "Drivetrain_Type_Lookup",
    "Auction_Supplier", "Employee", "Customer", "Car", "Salary", "Saved_Address",
    "Customer_Preference", "Shopping_Cart", "Wishlist", "Search_History", "Customer_Activity_Log", "Order",
    "Cart_Item", "Wishlist_Item", "Product_View_History", "Review", "Order_Item",
    "Payment_Transaction", "Shipping_Logistics", "Activity_Data"
]

def connect_to_db():
    load_dotenv()
    credentials = {
        "host": os.getenv("DB_HOST"), "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT")
    }
    try:
        conn = psycopg2.connect(**credentials)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        return None

def read_csv_to_dicts(file_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(file_path):
        print(f"Warning: CSV file not found: {file_path}. Skipping.")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_dicts_to_csv(file_path: str, data: List[Dict[str, Any]], fieldnames: List[str]):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    conn = connect_to_db()
    if not conn:
        return

    # Stores { 'ConfigKey': { 'natural_key_value_from_csv': actual_db_id } }
    # OR { 'ConfigKey': { csv_row_index_0_based: actual_db_id } } if natural_key_in_csv is None
    actual_id_cache: Dict[str, Dict[Any, int]] = {}

    # --- Pass 1: Populate actual_id_cache for all tables based on current DB state ---
    print("--- Pass 1: Fetching actual IDs from database ---")
    with conn.cursor() as cursor:
        for config_key in PROCESSING_ORDER:
            config = TABLE_CONFIGS[config_key]
            print(f"Fetching IDs for {config_key}...")
            actual_id_cache[config_key] = {}
            original_csv_data = read_csv_to_dicts(config["csv_file"])

            for idx, csv_row in enumerate(original_csv_data):
                actual_db_id: Optional[int] = None
                if config["natural_key_in_csv"] and config["db_column_for_natural_key"]:
                    natural_key_val = csv_row.get(config["natural_key_in_csv"])
                    if natural_key_val is not None and natural_key_val != '':
                        sql = f'SELECT "{config["id_column_in_db"]}" FROM "{config["db_table_name"]}" WHERE "{config["db_column_for_natural_key"]}" = %s'
                        cursor.execute(sql, (natural_key_val,))
                        result = cursor.fetchone()
                        if result:
                            actual_db_id = result[0]
                            actual_id_cache[config_key][natural_key_val] = actual_db_id
                
                # Fallback or primary way to cache by index if ID was found
                # This is crucial for FKs that use placeholder_is_1_based_index
                if actual_db_id is not None: # If we found an ID (either by natural key or if we were to fetch it by other means)
                     actual_id_cache[config_key][idx] = actual_db_id # Store by 0-based index
                elif not config["natural_key_in_csv"]: # If no natural key, and we need to map by index, but couldn't fetch ID (e.g. row wasn't inserted)
                    print(f"  Warning: Could not determine DB ID for row {idx} of {config_key} as no natural key is defined and row might be missing in DB.")


    # --- Pass 2: Rewrite CSVs with correct foreign key IDs ---
    print("\n--- Pass 2: Rewriting CSVs with corrected foreign keys ---")
    output_dir = "data_corrected"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir) # Clear previous corrected data
    os.makedirs(output_dir, exist_ok=True)

    all_original_csv_data_store: Dict[str, List[Dict[str,str]]] = {}

    for config_key in PROCESSING_ORDER:
        config = TABLE_CONFIGS[config_key]
        print(f"Processing CSV for {config_key}...")
        original_csv_data = read_csv_to_dicts(config["csv_file"])
        if not original_csv_data:
            print(f"  Skipping {config_key} as its original CSV is empty or not found.")
            continue
        
        all_original_csv_data_store[config_key] = original_csv_data # Store for parent lookups
        
        corrected_rows = []
        fieldnames = list(original_csv_data[0].keys()) if original_csv_data else []

        for original_row in original_csv_data:
            new_row = original_row.copy()
            for fk_conf in config["fk_configs"]:
                placeholder_fk_val_str = original_row.get(fk_conf["csv_fk_column"])
                if placeholder_fk_val_str is None or placeholder_fk_val_str == '':
                    continue # Skip if FK is blank

                actual_fk_id: Optional[int] = None
                parent_config_key = fk_conf["parent_config_key"]
                parent_config = TABLE_CONFIGS[parent_config_key]

                if fk_conf["placeholder_is_1_based_index"]:
                    try:
                        parent_csv_row_index_0_based = int(placeholder_fk_val_str) - 1
                        if parent_csv_row_index_0_based >= 0:
                            # Get the actual ID of the parent row using its 0-based index from its original CSV
                            # This relies on Pass 1 having cached parent IDs by their 0-based index
                            actual_fk_id = actual_id_cache.get(parent_config_key, {}).get(parent_csv_row_index_0_based)
                    except ValueError:
                        print(f"  Warning: Invalid placeholder index '{placeholder_fk_val_str}' for {fk_conf['csv_fk_column']} in {config_key}.")
                else: # Placeholder is expected to be a natural key value of the parent
                    actual_fk_id = actual_id_cache.get(parent_config_key, {}).get(placeholder_fk_val_str)

                if actual_fk_id is not None:
                    new_row[fk_conf["csv_fk_column"]] = actual_fk_id
                else:
                    print(f"  Warning: Could not resolve FK for {config_key}.{fk_conf['csv_fk_column']} (value: '{placeholder_fk_val_str}') to an actual ID in {parent_config_key}.")
            corrected_rows.append(new_row)

        if corrected_rows:
            output_csv_path = os.path.join(output_dir, os.path.basename(config["csv_file"]))
            write_dicts_to_csv(output_csv_path, corrected_rows, fieldnames)
            print(f"  Created corrected CSV: {output_csv_path}")

    if conn:
        conn.close()
    print("\nDebug script finished. Corrected CSVs are in 'data_corrected/' directory.")
    print("Review the warnings and corrected CSVs. You may need to manually adjust TABLE_CONFIGS if assumptions about natural keys or index-based FKs are incorrect for some tables.")
    print("After verification, you can try running main.py again, possibly by pointing it to the 'data_corrected' files.")

if __name__ == "__main__":
    main()