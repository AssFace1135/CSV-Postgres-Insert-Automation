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

def connect_to_db(credentials):
    """Establishes a connection to the PostgreSQL database."""
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(**credentials)
        print("Connection successful!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        return None

def read_csv_data(file_path: str, table_name_for_logging: str = "") -> List[Dict[str, Any]]:
    """Reads data from a CSV file and returns a list of dictionaries."""
    if not os.path.exists(file_path):
        # This specific FileNotFoundError will be caught and handled in main's loop
        raise FileNotFoundError(f"CSV file for table '{table_name_for_logging}' not found: {file_path}")
    
    data = []
    # Check if file is empty before trying to read
    if os.path.getsize(file_path) == 0:
        print(f"Info: CSV file '{file_path}' for table '{table_name_for_logging}' is empty.")
        return data
        
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def insert_data_from_csv(cursor, csv_file_path: str, db_table_sql_identifier: str, csv_to_db_map: Dict[str, str], db_id_col_name: str) -> Tuple[List[int], int]:
    """
    Inserts data from a CSV file into a specified table.
    Skips rows that cause IntegrityErrors (e.g., unique constraint violations)
    and continues with other rows.
    
    Args:
        cursor: Database cursor
        csv_file_path: Path to the CSV file.
        db_table_sql_identifier: The SQL identifier for the database table (e.g., "my_table" or "order").
        csv_to_db_map: Dictionary mapping CSV column headers to database column names.
        db_id_col_name: Name of the primary key column in the database for the RETURNING clause.

    Returns:
        A tuple containing:
            - List of successfully inserted row IDs.
            - Total number of rows attempted from the CSV.
    """
    # read_csv_data can raise FileNotFoundError, which will be caught in the calling function (main)
    data_from_csv = read_csv_data(csv_file_path, db_table_sql_identifier) # Use db_table_sql_identifier for logging context
    total_rows_in_csv = len(data_from_csv)

    if not data_from_csv:
        return [], 0 # No data to process

    inserted_ids = []
    
    db_columns_to_insert = list(csv_to_db_map.values())
    placeholders = ', '.join(['%s'] * len(db_columns_to_insert))
    # db_table_sql_identifier is already correctly formatted (e.g., "my_table" or """order""")
    # db_columns_to_insert contains simple snake_case names; these should be quoted.
    # db_id_col_name is a simple snake_case name; this should be quoted.
    sql = f"""\
        INSERT INTO {db_table_sql_identifier} ({', '.join(f'"{col}"' for col in db_columns_to_insert)})
        VALUES ({placeholders})
        RETURNING "{db_id_col_name}";
    """

    for i, row_dict in enumerate(data_from_csv):
        values_tuple = []
        try:
            # Prepare values for insertion
            for csv_col_header in csv_to_db_map.keys(): # Iterate over CSV headers defined in map
                value = row_dict.get(csv_col_header)
                # Convert empty strings to None for potentially nullable fields
                # This is especially important for date/timestamp fields
                if value == '':
                    value = None
                values_tuple.append(value)
        except Exception as e:
            print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to error preparing data. CSV row (approx): {row_dict}. Error: {e}")
            continue # Skip to next row

        # Sanitize table_name for savepoint if it contains special characters (though current ones are fine)
        safe_table_name_for_sp = "".join(c if c.isalnum() else "_" for c in db_table_sql_identifier)
        savepoint_name = f"sp_csv_{safe_table_name_for_sp}_row_{i}"
        try:
            cursor.execute(f"SAVEPOINT {savepoint_name};")
            cursor.execute(sql, values_tuple)
            fetched_id = cursor.fetchone()
            if fetched_id:
                inserted_ids.append(fetched_id[0])
            # cursor.execute(f"RELEASE SAVEPOINT {savepoint_name};") # Optional: good practice
        except psycopg2.IntegrityError as e: # Catches UniqueViolation, ForeignKeyViolation, NotNullViolation etc.
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            # Check if it's specifically a unique constraint violation (common for "same values added")
            if hasattr(e, 'diag') and hasattr(e.diag, 'sqlstate') and e.diag.sqlstate == '23505': # '23505' is the SQLSTATE for unique_violation
                print(f"Warning: There is nothing new to add for this row in '{db_table_sql_identifier}'. CSV row (approx): {row_dict}.")
            else:
                # For other integrity errors (e.g., foreign key, not null)
                print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to other integrity violation. CSV row (approx): {row_dict}. Details: {e}")
        except (psycopg2.Error, Exception) as e: # Catch other psycopg2 specific errors or general Python errors for this row
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            print(f"Warning: Skipping row in table '{db_table_sql_identifier}' due to database/unexpected error. CSV row (approx): {row_dict}. Error: {e}")
    return inserted_ids, total_rows_in_csv

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
            print("\nStarting database population process...")
            
            # Define CSV file paths and column mappings for each table
            # CSV keys (e.g., "Car_Condition_Rating") are for script logic and finding CSV files.
            # "db_identifier_for_sql" is the actual SQL table name (quoted if necessary).
            # "csv_to_db_column_map" maps CSV headers to DB column names (all DB names are snake_case).
            # "db_id_column_name" is the DB primary key name (snake_case).
            csv_mappings: Dict[str, Dict[str, Any]] = {
                # Level 0: No FK dependencies to other tables
                "Car_Condition_Rating": {
                    "csv_file_path": "data/car_condition_rating.csv",
                    "db_identifier_for_sql": "car_condition_rating",
                    "csv_to_db_column_map": {
                        "rating_code": "rating_code",
                        "description": "description"
                    },
                    "db_id_column_name": "condition_id"
                },
                "Engine_Type_Lookup": {
                    "csv_file_path": "data/engine_type_lookup.csv",
                    "db_identifier_for_sql": "engine_type_lookup",
                    "csv_to_db_column_map": {
                        "engine_code": "engine_code",
                        "description": "description"
                    },
                    "db_id_column_name": "engine_type_id"
                },
                "Transmission_Type_Lookup": {
                    "csv_file_path": "data/transmission_type_lookup.csv",
                    "db_identifier_for_sql": "transmission_type_lookup",
                    "csv_to_db_column_map": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "db_id_column_name": "transmission_type_id"
                },
                "Drivetrain_Type_Lookup": {
                    "csv_file_path": "data/drivetrain_type_lookup.csv",
                    "db_identifier_for_sql": "drivetrain_type_lookup",
                    "csv_to_db_column_map": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "db_id_column_name": "drivetrain_type_id"
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
                    "db_id_column_name": "supplier_id"
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
                    "db_id_column_name": "employee_id"
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
                    "db_id_column_name": "customer_id"
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
                    "db_id_column_name": "car_id"
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
                    "db_id_column_name": "salary_id"
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
                    "db_id_column_name": "saved_address_id"
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
                    "db_id_column_name": "preference_id"
                },
                "Shopping_Cart": {
                    "csv_file_path": "data/shopping_cart.csv",
                    "db_identifier_for_sql": "shopping_cart",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id"
                        # created_date, last_updated_date have defaults
                    },
                    "db_id_column_name": "cart_id"
                },
                "Wishlist": {
                    "csv_file_path": "data/wishlist.csv",
                    "db_identifier_for_sql": "wishlist",
                    "csv_to_db_column_map": {
                        "customer_id": "customer_id"
                        # created_date, last_updated_date have defaults
                    },
                    "db_id_column_name": "wishlist_id"
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
                    "db_id_column_name": "search_id"
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
                    "db_id_column_name": "activity_id"
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
                    "db_id_column_name": "order_id"
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
                    "db_id_column_name": "cart_item_id"
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
                    "db_id_column_name": "wishlist_item_id"
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
                    "db_id_column_name": "product_view_id"
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
                    "db_id_column_name": "review_id"
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
                    "db_id_column_name": "order_item_id"
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
                    "db_id_column_name": "transaction_id"
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
                    "db_id_column_name": "shipping_id"
                },
            }
            
            # Insert data from each CSV file in the correct order
            # The order in this dictionary is important for handling foreign key dependencies.
            # Ensure parent tables are populated before child tables.
            for config_key, config_values in csv_mappings.items():
                try:
                    print(f"\nInserting data for {config_key} into DB table {config_values['db_identifier_for_sql']}...")
                    inserted_ids, total_attempted = insert_data_from_csv(
                        cursor,
                        config_values["csv_file_path"],
                        config_values["db_identifier_for_sql"],
                        config_values["csv_to_db_column_map"],
                        config_values["db_id_column_name"]
                    )
                    print(f"Finished processing {config_key}. Inserted {len(inserted_ids)} new rows out of {total_attempted} attempted.")
                except FileNotFoundError as e: # Catches FNF from read_csv_data (via insert_data_from_csv)
                    print(f"Warning: {e} Skipping this table.") # The error message from read_csv_data is already informative
                    continue # Continue to the next table
                except Exception as e: # Catches more critical errors during a table's processing setup
                    print(f"Critical error during processing of {config_key} (DB table: {config_values['db_identifier_for_sql']}): {e}. Further processing for this table halted.")
                    raise # This will cause the main transaction to rollback

            # If all insertions are successful, commit the transaction
            conn.commit()
            print("\nDatabase population successful. All changes have been committed.")

    except psycopg2.Error as e:
        print(f"\nAn error occurred: {e}")
        print("Transaction is being rolled back.")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        # Always close the connection
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
