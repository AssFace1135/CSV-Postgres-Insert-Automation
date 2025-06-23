#
# #####################################################################
# Python script to populate a complete PostgreSQL database schema.
#
# This script uses 'psycopg2' and inserts sample data into every table
# from the provided schema, respecting all foreign key constraints.
#
# PRE-REQUISITES:
# 1. PostgreSQL server must be running.
# 2. The database schema (tables, enums, etc.) must be created.
# 3. Install the psycopg2 library:
#    pip install psycopg2-binary
#
# HOW TO USE:
# 1. Update the 'db_credentials' with your actual PostgreSQL details.
# 2. Run the script: python your_script_name.py
#
# The script will perform all insertions within a single transaction.
# If any step fails, the entire transaction is rolled back.
# #####################################################################
#

import psycopg2
import csv
from dotenv import load_dotenv
import os
from typing import Dict, List, Any, Tuple
import sys

def safe_print(message: Any):
    """
    Prints a message to the console, replacing any characters that cannot be
    encoded in the console's default encoding.
    """
    # Convert the message to a string first.
    message_str = str(message)
    # Get the console's encoding, falling back to utf-8 if it's not set.
    encoding = sys.stdout.encoding or 'utf-8'
    # Encode the string, replacing errors, then decode it back.
    safe_message = message_str.encode(encoding, errors='replace').decode(encoding)
    print(safe_message)

def connect_to_db(credentials):
    """Establishes a connection to the PostgreSQL database."""
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(**credentials)
        print("Connection successful!")
        return conn
    except psycopg2.OperationalError as e:
        safe_print(f"Could not connect to the database: {e}")
        return None

def read_csv_data(file_path: str, table_name_for_logging: str = "") -> List[Dict[str, Any]]:
    """Reads data from a CSV file and returns a list of dictionaries."""
    if not os.path.exists(file_path):
        # This specific FileNotFoundError will be caught and handled in main's loop
        raise FileNotFoundError(f"CSV file for table '{table_name_for_logging}' not found: {file_path}")
    
    data = []
    # Check if file is empty before trying to read
    if os.path.getsize(file_path) == 0:
        safe_print(f"Info: CSV file '{file_path}' for table '{table_name_for_logging}' is empty.")
        return data
        
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def insert_processed_data(cursor, data_to_insert: List[Dict[str, Any]], db_table_sql_identifier: str, csv_to_db_map: Dict[str, str], db_id_col_name: str, natural_key_columns: List[str] = None) -> Tuple[List[int], int]:
    """
    Inserts data from a CSV file into a specified table.
    Skips rows that cause IntegrityErrors (e.g., unique constraint violations)
    and continues with other rows. If a unique violation occurs and natural
    keys are provided, it attempts to fetch the existing row's ID.
    
    Args:
        cursor: Database cursor
        data_to_insert: A list of dictionaries, where each dictionary is a row to be inserted.
        db_table_sql_identifier: The SQL identifier for the database table (e.g., "my_table" or '"order"').
        csv_to_db_map: Dictionary mapping CSV column headers to database column names.
        db_id_col_name: Name of the primary key column in the database for the RETURNING clause.
        natural_key_columns: Optional list of database column names that form a unique key.
                             Used to retrieve existing IDs on unique constraint violation.

    Returns:
        A tuple containing:
            - List of successfully inserted or retrieved row IDs.
            - Total number of rows attempted from the CSV.
    """
    total_rows_to_insert = len(data_to_insert)

    if not data_to_insert:
        return [], 0 # No data to process

    inserted_ids = []
    
    db_columns_to_insert = list(csv_to_db_map.values())
    placeholders = ', '.join(['%s'] * len(db_columns_to_insert))
    sql = f"""\
        INSERT INTO {db_table_sql_identifier} ({', '.join(f'"{col}"' for col in db_columns_to_insert)})
        VALUES ({placeholders})
        RETURNING "{db_id_col_name}";
    """

    for i, row_dict in enumerate(data_to_insert):
        values_tuple = []
        try:
            for csv_col_header in csv_to_db_map.keys():
                value = row_dict.get(csv_col_header)
                if value == '':
                    value = None
                values_tuple.append(value)
        except Exception as e:
            safe_print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to error preparing data. CSV row (approx): {row_dict}. Error: {e}")
            continue

        safe_table_name_for_sp = "".join(c if c.isalnum() else "_" for c in db_table_sql_identifier)
        savepoint_name = f"sp_csv_{safe_table_name_for_sp}_row_{i}"
        try:
            cursor.execute(f"SAVEPOINT {savepoint_name};")
            cursor.execute(sql, values_tuple)
            fetched_id = cursor.fetchone()
            if fetched_id:
                inserted_ids.append(fetched_id[0])
        except psycopg2.IntegrityError as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            if hasattr(e, 'diag') and e.diag.sqlstate == '23505':
                safe_print(f"Warning: Unique constraint violation for row in '{db_table_sql_identifier}'. Attempting to retrieve existing ID. CSV row (approx): {row_dict}.")
                if natural_key_columns:
                    where_clauses = []
                    select_values = []
                    for db_col_name in natural_key_columns:
                        csv_col_header = next((k for k, v in csv_to_db_map.items() if v == db_col_name), None)
                        if csv_col_header is None:
                            safe_print(f"Error: Natural key column '{db_col_name}' not found in csv_to_db_map for '{db_table_sql_identifier}'. Cannot retrieve existing ID.")
                            break
                        value = row_dict.get(csv_col_header)
                        if value == '' or value is None:
                            where_clauses.append(f'"{db_col_name}" IS NULL')
                        else:
                            where_clauses.append(f'"{db_col_name}" = %s')
                            select_values.append(value)

                    if where_clauses and len(where_clauses) == len(natural_key_columns):
                        select_sql = f"""
                            SELECT "{db_id_col_name}" FROM {db_table_sql_identifier}
                            WHERE {' AND '.join(where_clauses)};
                        """
                        try:
                            cursor.execute(select_sql, select_values)
                            existing_id = cursor.fetchone()
                            if existing_id:
                                inserted_ids.append(existing_id[0])
                                safe_print(f"Info: Retrieved existing ID {existing_id[0]} for row in '{db_table_sql_identifier}'.")
                            else:
                                safe_print(f"Warning: Unique violation occurred but existing row not found via natural keys for '{db_table_sql_identifier}'. This might indicate a data inconsistency or incorrect natural_key_columns. Row: {row_dict}")
                        except psycopg2.Error as select_err:
                            safe_print(f"Error: Failed to retrieve existing ID for '{db_table_sql_identifier}' after unique violation. Error: {select_err}. Row: {row_dict}")
                    else:
                        safe_print(f"Warning: Cannot retrieve existing ID for '{db_table_sql_identifier}' because natural_key_columns could not be fully mapped from CSV data. Skipping row. Row: {row_dict}")
                else:
                    safe_print(f"Warning: Unique violation occurred for '{db_table_sql_identifier}' but no natural_key_columns provided to retrieve existing ID. Skipping row. Row: {row_dict}")
            else:
                safe_print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to other integrity violation. CSV row (approx): {row_dict}. Details: {e}")
        except (psycopg2.Error, Exception) as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            safe_print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to database/unexpected error. CSV row (approx): {row_dict}. Error: {e}")
    return inserted_ids, total_rows_to_insert

def main():
    """Main function to run the database population process."""
    # Load environment variables from .env file
    load_dotenv()

    # Retrieve database credentials from environment variables
    db_credentials = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT")
    }

    conn = connect_to_db(db_credentials)
    if not conn:
        return

    # Use a single transaction for all insertions
    try:
        with conn.cursor() as cursor:
            # This dictionary will store the mapping of a CSV's 0-based row index
            # to the actual database ID generated upon insertion.
            # Format: { 'ConfigKey': { 0: db_id_1, 1: db_id_2, ... } }
            csv_row_to_db_id_maps: Dict[str, Dict[int, int]] = {}
            print("\nStarting database population process...")
            
            # Define CSV file paths and column mappings for each table
            # CSV keys (e.g., "Car_Condition_Rating") are for script logic and finding CSV files.
            # "db_identifier_for_sql" is the actual SQL table name (quoted if necessary).
            # "csv_to_db_column_map" maps CSV headers to DB column names (all DB names are snake_case).
            # "db_id_column_name" is the DB primary key name (snake_case).
            # "fk_configs" defines the foreign key relationships for robust insertion.
            csv_mappings: Dict[str, Dict[str, Any]] = {
                # Level 0: No FK dependencies to other tables
                "Car_Condition_Rating": {
                    "csv_file_path": "data/car_condition_rating.csv",
                    "db_identifier_for_sql": "car_condition_rating",
                    "csv_to_db_column_map": {
                        "rating_code": "rating_code",
                        "description": "description"
                    },
                    "db_id_column_name": "condition_id", # Primary key column
                    "natural_key_columns": ["rating_code"], # Columns that form a unique key
                    "fk_configs": []
                },
                "Engine_Type_Lookup": {
                    "csv_file_path": "data/engine_type_lookup.csv",
                    "db_identifier_for_sql": "engine_type_lookup",
                    "csv_to_db_column_map": {
                        "engine_code": "engine_code",
                        "description": "description"
                    },
                    "db_id_column_name": "engine_type_id", # Primary key column
                    "natural_key_columns": ["engine_code"], # Columns that form a unique key
                    "fk_configs": []
                },
                "Transmission_Type_Lookup": {
                    "csv_file_path": "data/transmission_type_lookup.csv",
                    "db_identifier_for_sql": "transmission_type_lookup",
                    "csv_to_db_column_map": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "db_id_column_name": "transmission_type_id", # Primary key column
                    "natural_key_columns": ["type_name"], # Columns that form a unique key
                    "fk_configs": []
                },
                "Drivetrain_Type_Lookup": {
                    "csv_file_path": "data/drivetrain_type_lookup.csv",
                    "db_identifier_for_sql": "drivetrain_type_lookup",
                    "csv_to_db_column_map": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "db_id_column_name": "drivetrain_type_id", # Primary key column
                    "natural_key_columns": ["type_name"], # Columns that form a unique key
                    "fk_configs": []
                },
                "Auction_Supplier": {
                    "csv_file_path": "data/auction_supplier.csv",
                    "db_identifier_for_sql": "auction_supplier",
                    "csv_to_db_column_map": {
                        "supplier_name": "supplier_name",
                        "contact_person": "contact_person",
                        "phone_number": "phone_number",
                        "email": "email",
                        "website": "website",
                        "location": "location"
                    },
                    "db_id_column_name": "supplier_id", # Primary key column
                    "natural_key_columns": ["supplier_name"], # Assuming supplier_name is unique
                    "fk_configs": []
                },
                "Employee": {
                    "csv_file_path": "data/employee.csv",
                    "db_identifier_for_sql": "employee",
                    "csv_to_db_column_map": {
                        "first_name": "first_name",
                        "last_name": "last_name",
                        "email": "email",
                        "password_hash": "password_hash",
                        "phone_number": "phone_number",
                        "job_title": "job_title",
                        "hire_date": "hire_date", # NOT NULL in schema
                        "is_active": "is_active"
                    },
                    "db_id_column_name": "employee_id", # Primary key column
                    "natural_key_columns": ["email"], # Assuming email is unique
                    "fk_configs": []
                },
                "Customer": {
                    "csv_file_path": "data/customer.csv",
                    "db_identifier_for_sql": "customer",
                    "csv_to_db_column_map": {
                        "first_name": "first_name",
                        "last_name": "last_name",
                        "email": "email",
                        "password_hash": "password_hash",
                        "phone_number": "phone_number",
                        "address_line1": "address_line1",
                        "address_line2": "address_line2", # Added from schema
                        "city": "city",
                        "state_province": "state_province",
                        "postal_code": "postal_code",
                        "country": "country",
                        # registration_date has DEFAULT in schema, not mapped if CSV doesn't provide
                        "last_login_date": "last_login_date",
                        "total_orders_count": "total_orders_count",
                        "total_spent_jpy": "total_spent_jpy",
                        "last_activity_date": "last_activity_date",
                        "loyalty_score": "loyalty_score",
                        "preferred_contact_method": "preferred_contact_method"
                    },
                    "db_id_column_name": "customer_id", # Primary key column
                    "natural_key_columns": ["email"], # Assuming email is unique
                    "fk_configs": []
                },
                # Level 1: Depends only on Level 0 tables
                "Car": {
                    "csv_file_path": "data/car.csv",
                    "db_identifier_for_sql": "car",
                    "csv_to_db_column_map": {
                        "vin": "vin",
                        "chassis_code": "chassis_code",
                        "make": "make",
                        "model": "model",
                        "year": "year",
                        "color": "color",
                        "engine_type_id": "engine_type_id",
                        "transmission_type_id": "transmission_type_id",
                        "drivetrain_type_id": "drivetrain_type_id",
                        "steering_side": "steering_side", # Added from schema
                        "mileage_km": "mileage_km",
                        "condition_id": "condition_id",
                        "current_listing_price_jpy": "current_listing_price_jpy",
                        "photos_url": "photos_url",
                        "description": "description",
                        "status": "status", # Added from schema
                        # date_added_to_inventory has DEFAULT
                        "auction_lot_number": "auction_lot_number",
                        "supplier_id": "supplier_id",
                        "view_count": "view_count",
                        "add_to_cart_count": "add_to_cart_count",
                        "add_to_wishlist": "add_to_wishlist_count" # Renamed in schema
                    },
                    "db_id_column_name": "car_id",
                    "natural_key_columns": ["vin"],
                    "fk_configs": [
                        {"csv_fk_column": "engine_type_id", "parent_config_key": "Engine_Type_Lookup", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "transmission_type_id", "parent_config_key": "Transmission_Type_Lookup", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "drivetrain_type_id", "parent_config_key": "Drivetrain_Type_Lookup", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "condition_id", "parent_config_key": "Car_Condition_Rating", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "supplier_id", "parent_config_key": "Auction_Supplier", "placeholder_is_1_based_index": True},
                    ]
                },
                "Salary": {
                    "csv_file_path": "data/salary.csv",
                    "db_identifier_for_sql": "salary",
                    "csv_to_db_column_map": {
                        "employee_id": "employee_id", # NOT NULL in schema
                        "salary_amount": "salary_amount",
                        "from_date": "from_date", # TIMESTAMPTZ
                        "to_date": "to_date"
                    },
                    "db_id_column_name": "salary_id",
                    "fk_configs": [
                        {"csv_fk_column": "employee_id", "parent_config_key": "Employee", "placeholder_is_1_based_index": True},
                    ]
                },
                "Saved_Address": {
                    "csv_file_path": "data/saved_address.csv",
                    "db_identifier_for_sql": "saved_address",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "address_label": "address_label", # Added from schema, NOT NULL
                        "shipping_address_line1": "shipping_address_line1",
                        "shipping_address_line2": "shipping_address_line2",
                        "shipping_city": "shipping_city",
                        "shipping_state_province": "shipping_state_province",
                        "shipping_postal_code": "shipping_postal_code",
                        "shipping_country": "shipping_country"
                    },
                    "db_id_column_name": "saved_address_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Customer_Preference": {
                    "csv_file_path": "data/customer_preference.csv",
                    "db_identifier_for_sql": "customer_preference",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "preferred_make": "preferred_make",
                        "preferred_model": "preferred_model",
                        "min_year": "min_year",
                        "max_year": "max_year",
                        "max_budget_jpy": "max_budget_jpy",
                        "preferred_specs": "preferred_specs",
                        "preferred_condition": "preferred_condition"
                    },
                    "db_id_column_name": "preference_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Shopping_Cart": {
                    "csv_file_path": "data/shopping_cart.csv",
                    "db_identifier_for_sql": "shopping_cart",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id"
                        # created_date, last_updated_date have defaults
                    },
                    "natural_key_columns": ["customer_id"], # A customer has only one cart
                    "db_id_column_name": "cart_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Wishlist": {
                    "csv_file_path": "data/wishlist.csv",
                    "db_identifier_for_sql": "wishlist",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id"
                        # created_date, last_updated_date have defaults
                    },
                    "natural_key_columns": ["customer_id"], # A customer has only one wishlist
                    "db_id_column_name": "wishlist_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Search_History": {
                    "csv_file_path": "data/search_history.csv",
                    "db_identifier_for_sql": "search_history",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "search_query": "search_query",
                        "search_timestamp": "search_timestamp",
                        "result_count": "result_count",
                        "filter_applied": "filter_applied"
                        # search_timestamp has DEFAULT
                    },
                    "db_id_column_name": "search_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Customer_Activity_Log": {
                    "csv_file_path": "data/customer_activity_log.csv",
                    "db_identifier_for_sql": "customer_activity_log",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "activity_type": "activity_type",
                        "activity_timestamp": "activity_timestamp",
                        "details": "details",
                        "ip_address": "ip_address",
                        "user_agent": "user_agent"
                        # activity_timestamp has DEFAULT
                    },
                    "db_id_column_name": "activity_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                    ]
                },
                "Order": {
                    "csv_file_path": "data/order.csv",
                    "db_identifier_for_sql": '"order"', # Quoted because 'order' is a keyword
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "total_amount_jpy": "total_amount_jpy",
                        "order_status": "order_status", # Enum, has default
                        "payment_status": "payment_status", # Enum, has default
                        "currency": "currency", # Has default
                        "exchange_rate_at_order_time": "exchange_rate_at_order_time", # NOT NULL in schema
                        "managed_by_employee_id": "managed_by_employee_id"
                        # order_date has DEFAULT
                    },
                    "db_id_column_name": "order_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "managed_by_employee_id", "parent_config_key": "Employee", "placeholder_is_1_based_index": True},
                    ]
                },
                # Level 2: Depends on Level 1 and/or Level 0 tables
                "Cart_Item": {
                    "csv_file_path": "data/cart_item.csv",
                    "db_identifier_for_sql": "cart_item",
                    "csv_to_db_column_map": {
                        "cart_id": "cart_id",
                        "car_id": "car_id",
                        "quantity": "quantity" # Default is 1, CSV can override
                        # added_date has DEFAULT
                    },
                    "natural_key_columns": ["cart_id", "car_id"],
                    "db_id_column_name": "cart_item_id",
                    "fk_configs": [
                        {"csv_fk_column": "cart_id", "parent_config_key": "Shopping_Cart", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
                    ]
                },
                "Wishlist_Item": {
                    "csv_file_path": "data/wishlist_item.csv",
                    "db_identifier_for_sql": "wishlist_item",
                    "csv_to_db_column_map": {
                        "wishlist_id": "wishlist_id",
                        "car_id": "car_id",
                        "notification_preference": "notification_preference"
                        # added_date has DEFAULT
                    },
                    "natural_key_columns": ["wishlist_id", "car_id"],
                    "db_id_column_name": "wishlist_item_id",
                    "fk_configs": [
                        {"csv_fk_column": "wishlist_id", "parent_config_key": "Wishlist", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
                    ]
                },
                "Product_View_History": {
                    "csv_file_path": "data/product_view_history.csv",
                    "db_identifier_for_sql": "product_view_history",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "car_id": "car_id",
                        "view_timestamp": "view_timestamp",
                        "time_on_page_seconds": "time_on_page_seconds",
                        "source": "source"
                        # view_timestamp has DEFAULT
                    },
                    "db_id_column_name": "product_view_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
                    ]
                },
                "Review": {
                    "csv_file_path": "data/review.csv",
                    "db_identifier_for_sql": "review",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id",
                        "car_id": "car_id",
                        "rating": "rating", # NOT NULL in schema
                        "comment": "comment",
                        "is_approved": "is_approved" # Has default
                        # review_date has DEFAULT
                    },
                    "natural_key_columns": ["customer_id", "car_id"], # A customer can review a car only once
                    "db_id_column_name": "review_id",
                    "fk_configs": [
                        {"csv_fk_column": "customer_id", "parent_config_key": "Customer", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
                    ]
                },
                "Order_Item": {
                    "csv_file_path": "data/order_item.csv",
                    "db_identifier_for_sql": "order_item",
                    "csv_to_db_column_map": {
                        "order_id": "order_id",
                        "car_id": "car_id",
                        "unit_price_jpy": "unit_price_jpy", # NOT NULL in schema
                        "quantity": "quantity", # Has default
                        "item_status": "item_status", # Enum, has default
                        "customs_documentation_status": "customs_documentation_status" # Enum, has default
                    },
                    "db_id_column_name": "order_item_id",
                    "fk_configs": [
                        {"csv_fk_column": "order_id", "parent_config_key": "Order", "placeholder_is_1_based_index": True},
                        {"csv_fk_column": "car_id", "parent_config_key": "Car", "placeholder_is_1_based_index": True},
                    ]
                },
                "Payment_Transaction": {
                    "csv_file_path": "data/payment_transaction.csv",
                    "db_identifier_for_sql": "payment_transaction",
                    "csv_to_db_column_map": {
                        "order_id": "order_id",
                        "amount_jpy": "amount_jpy", # NOT NULL in schema
                        "payment_method": "payment_method", # NOT NULL in schema
                        "transaction_status": "transaction_status", # Enum, has default
                        "payment_gateway_ref_id": "payment_gateway_ref_id"
                        # payment_date has DEFAULT
                    },
                    "db_id_column_name": "transaction_id",
                    "fk_configs": [
                        {"csv_fk_column": "order_id", "parent_config_key": "Order", "placeholder_is_1_based_index": True},
                    ]
                },
                "Shipping_Logistics": {
                    "csv_file_path": "data/shipping_logistics.csv",
                    "db_identifier_for_sql": "shipping_logistics",
                    "csv_to_db_column_map": {
                        # Schema changed: FK is to order_item_id. CSV must provide this.
                        # IMPORTANT: Your shipping_logistics.csv must now have an 'order_item_id' column.
                        "order_item_id": "order_item_id", 
                        "shipping_company_name": "shipping_company_name", # NOT NULL in schema
                        "tracking_number": "tracking_number",
                        "ship_date": "ship_date",
                        "estimated_arrival_date": "estimated_arrival_date",
                        "actual_arrival_date": "actual_arrival_date",
                        "current_location": "current_location",
                        "shipping_cost_jpy": "shipping_cost_jpy",
                        "delivery_status": "delivery_status", # Enum, has default
                        "container_id": "container_id"
                    },
                    "natural_key_columns": ["tracking_number"],
                    "db_id_column_name": "shipping_id",
                    "fk_configs": [
                        # Corrected based on schema: FK is to order_item, not order.
                        {"csv_fk_column": "order_item_id", "parent_config_key": "Order_Item", "placeholder_is_1_based_index": True},
                    ]
                },
            }
            
            # Insert data from each CSV file in the correct order
            for config_key, config_values in csv_mappings.items():
                try:
                    safe_print(f"\nInserting data for {config_key} into DB table {config_values['db_identifier_for_sql']}...")
                    
                    # 1. Read the raw data from the CSV file
                    raw_csv_data = read_csv_data(config_values["csv_file_path"], config_key)
                    if not raw_csv_data:
                        safe_print(f"Finished processing {config_key}. No data to insert.")
                        continue

                    # 2. Process rows to resolve foreign keys
                    processed_rows_to_insert = []
                    for raw_row in raw_csv_data:
                        processed_row = raw_row.copy()
                        for fk_conf in config_values.get("fk_configs", []):
                            fk_col = fk_conf["csv_fk_column"]
                            parent_key = fk_conf["parent_config_key"]
                            placeholder_val_str = processed_row.get(fk_col)

                            if placeholder_val_str and placeholder_val_str.isdigit():
                                # Assumes placeholder is a 1-based index into the parent's CSV
                                parent_row_index = int(placeholder_val_str) - 1
                                actual_db_id = csv_row_to_db_id_maps.get(parent_key, {}).get(parent_row_index)
                                if actual_db_id:
                                    processed_row[fk_col] = actual_db_id
                                else:
                                    safe_print(f"Warning: Could not resolve FK for {config_key}.{fk_col} (value: {placeholder_val_str}). Parent ID not found in map. Setting to NULL.")
                                    processed_row[fk_col] = None # Set to None to avoid FK violation if column is nullable
                        processed_rows_to_insert.append(processed_row)

                    # 3. Insert the processed data
                    inserted_ids, total_attempted = insert_processed_data(
                        cursor,
                        processed_rows_to_insert,
                        config_values["db_identifier_for_sql"],
                        config_values["csv_to_db_column_map"],
                        config_values["db_id_column_name"],
                        config_values.get("natural_key_columns") # Pass the new argument
                    )
                    # 4. Store the mapping of CSV row index to the new DB ID for future lookups
                    if inserted_ids:
                        csv_row_to_db_id_maps[config_key] = {i: db_id for i, db_id in enumerate(inserted_ids)}

                    safe_print(f"Finished processing {config_key}. Inserted {len(inserted_ids)} new rows out of {total_attempted} attempted.")
                except FileNotFoundError as e:
                    safe_print(f"Warning: {e} Skipping this table.") # The error message from read_csv_data is already informative
                    continue # Continue to the next table
                except Exception as e: # Catches more critical errors during a table's processing setup
                    safe_print(f"Critical error during processing of {config_key} (DB table: {config_values['db_identifier_for_sql']}): {e}. Further processing for this table halted.")
                    raise # This will cause the main transaction to rollback

            # If all insertions are successful, commit the transaction
            conn.commit()
            safe_print("\nDatabase population successful. All changes have been committed.")

    except psycopg2.Error as e:
        safe_print(f"\nAn error occurred: {e}")
        safe_print("Transaction is being rolled back.")
        if conn:
            conn.rollback()
    except Exception as e:
        safe_print(f"\nAn unexpected error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        # Always close the connection
        if conn:
            conn.close()
            safe_print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
