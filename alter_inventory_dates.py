import psycopg2
import os
from dotenv import load_dotenv
import random
from datetime import date, timedelta, datetime, timezone

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

def randomize_inventory_dates(conn):
    """
    Fetches all cars and updates their date_added_to_inventory to a random date
    within +/- 6 months of their current date_added_to_inventory.
    """
    # Define the range for randomization: +/- 6 months (approx 180 days)
    random_days_range = 180 # +/- 180 days

    try:
        with conn.cursor() as cursor:
            print("Fetching car IDs and current inventory dates from the 'car' table...")
            cursor.execute('SELECT car_id, date_added_to_inventory FROM car;')
            cars_data = cursor.fetchall()

            if not cars_data:
                print("No cars found in the 'car' table. Nothing to update.")
                return

            print(f"Found {len(cars_data)} cars. Starting update process...")

            for i, (car_id, original_date_added) in enumerate(cars_data):
                # Ensure original_date_added is a datetime object
                if isinstance(original_date_added, date) and not isinstance(original_date_added, datetime):
                    original_date_added = datetime.combine(original_date_added, datetime.min.time())
                elif not isinstance(original_date_added, datetime):
                    print(f"Warning: date_added_to_inventory for car_id {car_id} is not a valid date/datetime. Skipping.")
                    continue

                # Generate a random number of days between -random_days_range and +random_days_range
                random_offset_days = random.randint(-random_days_range, random_days_range)
                
                # Calculate the new random date
                new_date_added = original_date_added + timedelta(days=random_offset_days)

                # Ensure the new date is not in the future
                # Cap it at the current UTC time to prevent negative inventory ages
                now_utc = datetime.now(timezone.utc)
                if new_date_added > now_utc:
                    new_date_added = now_utc
                # Update the date_added_to_inventory for the current car_id.
                update_query = 'UPDATE car SET date_added_to_inventory = %s WHERE car_id = %s;'
                cursor.execute(update_query, (new_date_added, car_id))

            # Commit the transaction once all updates are prepared.
            conn.commit()
            print(f"\nSuccessfully updated the 'date_added_to_inventory' for {len(cars_data)} records.")

    except psycopg2.Error as e:
        print(f"\nA database error occurred: {e}")
        print("Transaction is being rolled back.")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        if conn:
            conn.rollback()

def main():
    """Main function to run the database alteration process."""
    load_dotenv()
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
    try:
        randomize_inventory_dates(conn)
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()