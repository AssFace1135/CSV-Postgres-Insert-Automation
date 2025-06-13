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
import psycopg2.extras
from datetime import datetime, date
import csv
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

# #####################################################################
# DATA INSERTION FUNCTIONS
# Each function inserts data into one table and returns the new row's ID.
# #####################################################################

def insert_auction_supplier(cursor, data):
    """Inserts a new Auction_Supplier and returns its ID."""
    sql = """
        INSERT INTO "Auction_Supplier" (supplier_name, contact_person, phone_number, email, website, location)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING supplier_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_employee(cursor, data):
    """Inserts a new Employee and returns their ID."""
    sql = """
        INSERT INTO "Employee" (first_name, last_name, email, password_hash, phone_number, job_title, hire_date, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING employee_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_customer(cursor, data):
    """Inserts a new Customer and returns their ID."""
    sql = """
        INSERT INTO "Customer" (first_name, last_name, email, password_hash, phone_number, address_line1, city, state_province, postal_code, country, last_login_date, total_orders_count, total_spent_jpy, last_activity_date, loyalty_score, preferred_contact_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_car(cursor, data):
    """Inserts a new Car and returns its ID."""
    sql = """
        INSERT INTO "Car" (vin, chassis_code, make, model, year, color, engine_type_id, transmission_type_id, drivetrain_type_id, steering_side, mileage_km, condition_id, current_listing_price_jpy, photos_url, description, status, auction_lot_number, supplier_id, view_count, add_to_cart_count, add_to_wishlist)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING car_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_order(cursor, data):
    """Inserts a new Order and returns its ID."""
    sql = """
        INSERT INTO "Order" (customer_id, total_amount_jpy, order_status, payment_status, currency, exchange_rate_at_order_time, managed_by_employee_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING order_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_order_item(cursor, data):
    """Inserts a new Order_Item and returns its ID."""
    sql = """
        INSERT INTO "Order_Item" (order_id, car_id, unit_price_jpy, quantity, item_status, customs_documentation_status)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_item_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_payment_transaction(cursor, data):
    """Inserts a new Payment_Transaction and returns its ID."""
    sql = """
        INSERT INTO "Payment_Transaction" (order_id, amount_jpy, payment_method, transaction_status, payment_gateway_ref_id)
        VALUES (%s, %s, %s, %s, %s) RETURNING transaction_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_shipping_logistics(cursor, data):
    """Inserts a new Shipping_Logistics record and returns its ID."""
    sql = """
        INSERT INTO "Shipping_Logistics" (order_id, car_id, shipping_company_name, tracking_number, ship_date, estimated_arrival_date, shipping_cost_jpy, delivery_status, container_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING shipping_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]

def insert_review(cursor, data):
    """Inserts a new Review and returns its ID."""
    sql = """
        INSERT INTO "Review" (customer_id, car_id, rating, comment, is_approved)
        VALUES (%s, %s, %s, %s, %s) RETURNING review_id;
    """
    cursor.execute(sql, data)
    return cursor.fetchone()[0]
    
# ... you can add functions for other tables like Wishlist, Cart, etc. following the same pattern.

def read_csv_data(file_path: str) -> List[Dict[str, Any]]:
    """Reads data from a CSV file and returns a list of dictionaries."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    data = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def insert_data_from_csv(cursor, csv_file: str, table_name: str, column_mapping: Dict[str, str], id_column_name: str) -> List[int]:
    """
    Inserts data from a CSV file into a specified table.
    
    Args:
        cursor: Database cursor
        csv_file: Path to the CSV file
        table_name: Name of the table to insert into
        column_mapping: Dictionary mapping CSV column names to table column names
        id_column_name: Name of the primary key column to be returned
    
    Returns:
        List of inserted row IDs
    """
    data = read_csv_data(csv_file)
    inserted_ids = []
    
    # Prepare the SQL statement
    columns = list(column_mapping.values())
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"""
        INSERT INTO "{table_name}" ({', '.join(f'"{col}"' for col in columns)})
        VALUES ({placeholders})
        RETURNING "{id_column_name}";
    """
    
    # Insert each row
    for row in data:
        # Map CSV data to table columns
        values = [row[csv_col] for csv_col in column_mapping.keys()]
        cursor.execute(sql, values)
        inserted_ids.append(cursor.fetchone()[0])
    
    return inserted_ids

def main():
    """Main function to run the database population process."""
    db_credentials = {
        "host": "localhost",
        "dbname": "Car_Bussiness",
        "user": "postgres",
        "password": "12345678",
        "port": "5432"
    }
    
    conn = connect_to_db(db_credentials)
    if not conn:
        return

    # Use a single transaction for all insertions
    try:
        with conn.cursor() as cursor:
            print("\nStarting database population process...")
            
            # Define CSV file paths and column mappings for each table
            csv_mappings = {
                "Car_Condition_Rating": {
                    "file": "data/car_condition_rating.csv",
                    "mapping": {
                        "rating_code": "rating_code",
                        "description": "description"
                    },
                    "id_column": "condition_id"
                },
                "Engine_Type_Lookup": {
                    "file": "data/engine_type_lookup.csv",
                    "mapping": {
                        "engine_code": "engine_code",
                        "description": "description"
                    },
                    "id_column": "engine_type_id"
                },
                "Transmission_Type_Lookup": {
                    "file": "data/transmission_type_lookup.csv",
                    "mapping": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "id_column": "transmission_type_id"
                },
                "Drivetrain_Type_Lookup": {
                    "file": "data/drivetrain_type_lookup.csv",
                    "mapping": {
                        "type_name": "type_name",
                        "description": "description"
                    },
                    "id_column": "drivetrain_type_id"
                },
                "Auction_Supplier": {
                    "file": "data/auction_supplier.csv",
                    "mapping": {
                        "supplier_name": "supplier_name",
                        "contact_person": "contact_person",
                        "phone_number": "phone_number",
                        "email": "email",
                        "website": "website",
                        "location": "location"
                    },
                    "id_column": "supplier_id"
                },
                "Employee": {
                    "file": "data/employee.csv",
                    "mapping": {
                        "first_name": "first_name",
                        "last_name": "last_name",
                        "email": "email",
                        "password_hash": "password_hash",
                        "phone_number": "phone_number",
                        "job_title": "job_title",
                        "hire_date": "hire_date",
                        "is_active": "is_active"
                    },
                    "id_column": "employee_id"
                },
                "Customer": {
                    "file": "data/customer.csv",
                    "mapping": {
                        "first_name": "first_name",
                        "last_name": "last_name",
                        "email": "email",
                        "password_hash": "password_hash",
                        "phone_number": "phone_number",
                        "address_line1": "address_line1",
                        "city": "city",
                        "state_province": "state_province",
                        "postal_code": "postal_code",
                        "country": "country",
                        "last_login_date": "last_login_date",
                        "total_orders_count": "total_orders_count",
                        "total_spent_jpy": "total_spent_jpy",
                        "last_activity_date": "last_activity_date",
                        "loyalty_score": "loyalty_score",
                        "preferred_contact_method": "preferred_contact_method"
                    },
                    "id_column": "customer_id"
                },
                "Car": {
                    "file": "data/car.csv",
                    "mapping": {
                        "vin": "vin",
                        "chassis_code": "chassis_code",
                        "make": "make",
                        "model": "model",
                        "year": "year",
                        "color": "color",
                        "engine_type_id": "engine_type_id",
                        "transmission_type_id": "transmission_type_id",
                        "drivetrain_type_id": "drivetrain_type_id",
                        "steering_side": "steering_side",
                        "mileage_km": "mileage_km",
                        "condition_id": "condition_id",
                        "current_listing_price_jpy": "current_listing_price_jpy",
                        "photos_url": "photos_url",
                        "description": "description",
                        "status": "status",
                        "auction_lot_number": "auction_lot_number",
                        "supplier_id": "supplier_id",
                        "view_count": "view_count",
                        "add_to_cart_count": "add_to_cart_count",
                        "add_to_wishlist": "add_to_wishlist"
                    },
                    "id_column": "car_id"
                }
            }
            
            # Insert data from each CSV file in the correct order
            for table_name, config in csv_mappings.items():
                try:
                    print(f"\nInserting data into {table_name}...")
                    inserted_ids = insert_data_from_csv(
                        cursor,
                        config["file"],
                        table_name,
                        config["mapping"],
                        config["id_column"]
                    )
                    print(f"Successfully inserted {len(inserted_ids)} rows into {table_name}")
                except FileNotFoundError as e:
                    print(f"Warning: {e}")
                    continue
                except Exception as e:
                    print(f"Error inserting data into {table_name}: {e}")
                    raise

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
