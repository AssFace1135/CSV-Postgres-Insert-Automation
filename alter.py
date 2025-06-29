#
# #####################################################################
# Python script to alter data in a PostgreSQL database.
#
# This script specifically targets the "order" table and updates the
# 'order_date' column with a randomized timestamp for each existing row.
# This is useful for creating more realistic-looking sample data.
#
# PRE-REQUISITES:
# 1. PostgreSQL server must be running.
# 2. The database and tables must exist.
# 3. Install the required libraries:
#    pip install psycopg2-binary python-dotenv
#
# HOW TO USE:
# 1. Ensure your .env file has the correct database credentials.
# 2. Run the script: python alter.py
#
# The script performs all updates within a single transaction.
# If any step fails, the entire transaction is rolled back.
# #####################################################################
#

import psycopg2
import os
from dotenv import load_dotenv
import random
from datetime import date, timedelta, datetime

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

def randomize_order_dates(conn):
    """
    Fetches all orders and updates their order_date to a random date
    and time within a specified range.
    """
    # Define the date range for randomization: +/- 1 year from today.
    today = date.today()
    start_date = today - timedelta(days=730) # Go back 2 years
    end_date = today - timedelta(days=1) # Ensure all dates are at least 1 day in the past
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days

    try:
        with conn.cursor() as cursor:
            print("Fetching order IDs from the 'order' table...")
            # Use a quoted identifier for the "order" table as it's a SQL keyword.
            cursor.execute('SELECT order_id FROM "order";')
            order_ids = [row[0] for row in cursor.fetchall()]
            
            if not order_ids:
                print("No orders found in the 'order' table. Nothing to update.")
                return

            print(f"Found {len(order_ids)} orders. Starting update process...")
            
            for i, order_id in enumerate(order_ids):
                # Generate a random date and time.
                random_number_of_days = random.randrange(days_between_dates)
                random_date = start_date + timedelta(days=random_number_of_days)
                random_time = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
                random_datetime = datetime.combine(random_date, datetime.min.time()) + random_time

                # Update the order_date for the current order_id.
                update_query = 'UPDATE "order" SET order_date = %s WHERE order_id = %s;'
                cursor.execute(update_query, (random_datetime, order_id))

            # Commit the transaction once all updates are prepared.
            conn.commit()
            print(f"\nSuccessfully updated the 'order_date' for {len(order_ids)} records.")

    except psycopg2.Error as e:
        print(f"\nA database error occurred: {e}")
        print("Transaction is being rolled back.")
        if conn:
            conn.rollback()

def main():
    """Main function to run the database alteration process."""
    load_dotenv()
    db_credentials = {"host": os.getenv("DB_HOST"), "dbname": os.getenv("DB_NAME"), "user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD"), "port": os.getenv("DB_PORT")}
    conn = connect_to_db(db_credentials)
    if not conn:
        return
    try:
        randomize_order_dates(conn)
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()