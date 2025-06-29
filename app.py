import streamlit as st
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import functools
import pycountry
from psycopg2 import sql
from typing import Dict, List, Any, Tuple
import csv
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta # Added for RFM analysis
import re # Added for regex parsing of location strings
from geopy.geocoders import Nominatim
from diskcache import Cache # Import diskcache for persistent caching

# --- Configuration and Setup ---
# Load environment variables from .env file
load_dotenv()

# Database credentials
db_credentials = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

# Initialize a geolocator for converting location names to coordinates
geolocator = Nominatim(user_agent="car_dealership_dashboard/1.0")

# Initialize diskcache for persistent geocoding results
# This cache will store results in a directory named 'geocoding_cache'
# The cache will persist across Streamlit runs and application restarts.
# Set a reasonable size limit (e.g., 100 MB) or None for unlimited.
GEOCACHE_DIR = "geocoding_cache"
os.makedirs(GEOCACHE_DIR, exist_ok=True) # Ensure the cache directory exists
geocache = Cache(GEOCACHE_DIR, size_limit=100 * 1024 * 1024) # 100 MB cache limit

def get_lat_lon(location_str: str):
    """
    Geocodes a location string to (latitude, longitude).
    Uses caching to avoid repeated API calls.
    Returns None if location not found or on error.
    """
    if not location_str or not isinstance(location_str, str):
        return None

    # Store the original string for warning messages
    original_location_str = location_str
    
    # Pre-process common problematic strings
    processed_location_str = location_str.strip()

    # Handle "In transit to X"
    match_in_transit = re.match(r"In transit to (.+)", processed_location_str, re.IGNORECASE)
    if match_in_transit:
        city_or_region = match_in_transit.group(1).strip()
        processed_location_str = city_or_region
    
    # Handle "Port of X" - apply only if not already handled by "In transit to"
    elif re.match(r"Port of (.+)", processed_location_str, re.IGNORECASE):
        port_name = re.match(r"Port of (.+)", processed_location_str, re.IGNORECASE).group(1).strip()
        # Heuristic: if it's a known Japanese port from the examples, add Japan context
        if "Osaka" in port_name or "Kyoto" in port_name:
            processed_location_str = f"{port_name}, Japan"
        else:
            # For other ports, try just the name, Nominatim is often smart.
            processed_location_str = port_name
    
    # Check diskcache first
    cached_result = geocache.get(processed_location_str)
    if cached_result is not None:
        return cached_result # Return cached (lat, lon) or None if previously failed

    try:
        location = geolocator.geocode(processed_location_str, timeout=5)
        if location:
            result = (location.latitude, location.longitude)
            geocache.set(processed_location_str, result) # Cache successful result
            return result
        else:
            st.warning(f"Geocoding failed for location: '{original_location_str}' (processed as '{processed_location_str}'). Pin might be inaccurate or missing.")
            geocache.set(processed_location_str, None) # Cache failure to avoid re-attempting
            return None
    except Exception:
        # Catch any other errors during geocoding (e.g., network issues, Nominatim service down)
        st.warning(f"Error geocoding '{original_location_str}' (processed as '{processed_location_str}'). Pin might be inaccurate or missing.")
    return None
# This configuration is copied from your main.py to make the Streamlit app self-contained.
# It maps a logical name to the CSV file and database table information.
csv_mappings: Dict[str, Dict[str, Any]] = {
    "Car_Condition_Rating": {"csv_file_path": "data/car_condition_rating.csv", "db_identifier_for_sql": "car_condition_rating", "csv_to_db_column_map": {"rating_code": "rating_code", "description": "description"}, "db_id_column_name": "condition_id"},
    "Engine_Type_Lookup": {"csv_file_path": "data/engine_type_lookup.csv", "db_identifier_for_sql": "engine_type_lookup", "csv_to_db_column_map": {"engine_code": "engine_code", "description": "description"}, "db_id_column_name": "engine_type_id"},
    "Transmission_Type_Lookup": {"csv_file_path": "data/transmission_type_lookup.csv", "db_identifier_for_sql": "transmission_type_lookup", "csv_to_db_column_map": {"type_name": "type_name", "description": "description"}, "db_id_column_name": "transmission_type_id"},
    "Drivetrain_Type_Lookup": {"csv_file_path": "data/drivetrain_type_lookup.csv", "db_identifier_for_sql": "drivetrain_type_lookup", "csv_to_db_column_map": {"type_name": "type_name", "description": "description"}, "db_id_column_name": "drivetrain_type_id"},
    "Auction_Supplier": {"csv_file_path": "data/auction_supplier.csv", "db_identifier_for_sql": "auction_supplier", "csv_to_db_column_map": {"supplier_name": "supplier_name", "contact_person": "contact_person", "phone_number": "phone_number", "email": "email", "website": "website", "location": "location"}, "db_id_column_name": "supplier_id"},
    "Employee": {"csv_file_path": "data/employee.csv", "db_identifier_for_sql": "employee", "csv_to_db_column_map": {"first_name": "first_name", "last_name": "last_name", "email": "email", "password_hash": "password_hash", "phone_number": "phone_number", "job_title": "job_title", "hire_date": "hire_date", "is_active": "is_active"}, "db_id_column_name": "employee_id"},
    "Customer": {"csv_file_path": "data/customer.csv", "db_identifier_for_sql": "customer", "csv_to_db_column_map": {"first_name": "first_name", "last_name": "last_name", "email": "email", "password_hash": "password_hash", "phone_number": "phone_number", "address_line1": "address_line1", "address_line2": "address_line2", "city": "city", "state_province": "state_province", "postal_code": "postal_code", "country": "country", "last_login_date": "last_login_date", "total_orders_count": "total_orders_count", "total_spent_jpy": "total_spent_jpy", "last_activity_date": "last_activity_date", "loyalty_score": "loyalty_score", "preferred_contact_method": "preferred_contact_method"}, "db_id_column_name": "customer_id"},
    "Car": {"csv_file_path": "data/car.csv", "db_identifier_for_sql": "car", "csv_to_db_column_map": {"vin": "vin", "chassis_code": "chassis_code", "make": "make", "model": "model", "year": "year", "color": "color", "engine_type_id": "engine_type_id", "transmission_type_id": "transmission_type_id", "drivetrain_type_id": "drivetrain_type_id", "steering_side": "steering_side", "mileage_km": "mileage_km", "condition_id": "condition_id", "current_listing_price_jpy": "current_listing_price_jpy", "photos_url": "photos_url", "description": "description", "status": "status", "auction_lot_number": "auction_lot_number", "supplier_id": "supplier_id", "view_count": "view_count", "add_to_cart_count": "add_to_cart_count", "add_to_wishlist": "add_to_wishlist_count"}, "db_id_column_name": "car_id"},
    "Salary": {"csv_file_path": "data/salary.csv", "db_identifier_for_sql": "salary", "csv_to_db_column_map": {"employee_id": "employee_id", "salary_amount": "salary_amount", "from_date": "from_date", "to_date": "to_date"}, "db_id_column_name": "salary_id"},
    "Saved_Address": {"csv_file_path": "data/saved_address.csv", "db_identifier_for_sql": "saved_address", "csv_to_db_column_map": {"customer_id": "customer_id", "address_label": "address_label", "shipping_address_line1": "shipping_address_line1", "shipping_address_line2": "shipping_address_line2", "shipping_city": "shipping_city", "shipping_state_province": "shipping_state_province", "shipping_postal_code": "shipping_postal_code", "shipping_country": "shipping_country"}, "db_id_column_name": "saved_address_id"},
    "Customer_Preference": {"csv_file_path": "data/customer_preference.csv", "db_identifier_for_sql": "customer_preference", "csv_to_db_column_map": {"customer_id": "customer_id", "preferred_make": "preferred_make", "preferred_model": "preferred_model", "min_year": "min_year", "max_year": "max_year", "max_budget_jpy": "max_budget_jpy", "preferred_specs": "preferred_specs", "preferred_condition": "preferred_condition"}, "db_id_column_name": "preference_id"},
    "Shopping_Cart": {"csv_file_path": "data/shopping_cart.csv", "db_identifier_for_sql": "shopping_cart", "csv_to_db_column_map": {"customer_id": "customer_id"}, "db_id_column_name": "cart_id"},
    "Wishlist": {"csv_file_path": "data/wishlist.csv", "db_identifier_for_sql": "wishlist", "csv_to_db_column_map": {"customer_id": "customer_id"}, "db_id_column_name": "wishlist_id"},
    "Search_History": {"csv_file_path": "data/search_history.csv", "db_identifier_for_sql": "search_history", "csv_to_db_column_map": {"customer_id": "customer_id", "search_query": "search_query", "search_timestamp": "search_timestamp", "result_count": "result_count", "filter_applied": "filter_applied"}, "db_id_column_name": "search_id"},
    "Customer_Activity_Log": {"csv_file_path": "data/customer_activity_log.csv", "db_identifier_for_sql": "customer_activity_log", "csv_to_db_column_map": {"customer_id": "customer_id", "activity_type": "activity_type", "activity_timestamp": "activity_timestamp", "details": "details", "ip_address": "ip_address", "user_agent": "user_agent"}, "db_id_column_name": "activity_id"},
    "Order": {"csv_file_path": "data/order.csv", "db_identifier_for_sql": '"order"', "csv_to_db_column_map": {"customer_id": "customer_id", "total_amount_jpy": "total_amount_jpy", "order_status": "order_status", "payment_status": "payment_status", "currency": "currency", "exchange_rate_at_order_time": "exchange_rate_at_order_time", "managed_by_employee_id": "managed_by_employee_id"}, "db_id_column_name": "order_id"},
    "Cart_Item": {"csv_file_path": "data/cart_item.csv", "db_identifier_for_sql": "cart_item", "csv_to_db_column_map": {"cart_id": "cart_id", "car_id": "car_id", "quantity": "quantity"}, "db_id_column_name": "cart_item_id"},
    "Wishlist_Item": {"csv_file_path": "data/wishlist_item.csv", "db_identifier_for_sql": "wishlist_item", "csv_to_db_column_map": {"wishlist_id": "wishlist_id", "car_id": "car_id", "notification_preference": "notification_preference"}, "db_id_column_name": "wishlist_item_id"},
    "Product_View_History": {"csv_file_path": "data/product_view_history.csv", "db_identifier_for_sql": "product_view_history", "csv_to_db_column_map": {"customer_id": "customer_id", "car_id": "car_id", "view_timestamp": "view_timestamp", "time_on_page_seconds": "time_on_page_seconds", "source": "source"}, "db_id_column_name": "product_view_id"},
    "Review": {"csv_file_path": "data/review.csv", "db_identifier_for_sql": "review", "csv_to_db_column_map": {"customer_id": "customer_id", "car_id": "car_id", "rating": "rating", "comment": "comment", "is_approved": "is_approved"}, "db_id_column_name": "review_id"},
    "Order_Item": {"csv_file_path": "data/order_item.csv", "db_identifier_for_sql": "order_item", "csv_to_db_column_map": {"order_id": "order_id", "car_id": "car_id", "unit_price_jpy": "unit_price_jpy", "quantity": "quantity", "item_status": "item_status", "customs_documentation_status": "customs_documentation_status"}, "db_id_column_name": "order_item_id"},
    "Payment_Transaction": {"csv_file_path": "data/payment_transaction.csv", "db_identifier_for_sql": "payment_transaction", "csv_to_db_column_map": {"order_id": "order_id", "amount_jpy": "amount_jpy", "payment_method": "payment_method", "transaction_status": "transaction_status", "payment_gateway_ref_id": "payment_gateway_ref_id"}, "db_id_column_name": "transaction_id"},
    "Shipping_Logistics": {"csv_file_path": "data/shipping_logistics.csv", "db_identifier_for_sql": "shipping_logistics", "csv_to_db_column_map": {"order_item_id": "order_item_id", "shipping_company_name": "shipping_company_name", "tracking_number": "tracking_number", "ship_date": "ship_date", "estimated_arrival_date": "estimated_arrival_date", "actual_arrival_date": "actual_arrival_date", "current_location": "current_location", "shipping_cost_jpy": "shipping_cost_jpy", "delivery_status": "delivery_status", "container_id": "container_id"}, "db_id_column_name": "shipping_id"},
}

# --- Database Functions ---

@st.cache_resource
def get_db_connection():
    """Establishes a connection to the PostgreSQL database, cached for performance."""
    try:
        return psycopg2.connect(**db_credentials)
    except psycopg2.OperationalError as e:
        st.error(f"Could not connect to the database: {e}")
        return None

@st.cache_data(ttl=600) # Cache for 10 minutes
def get_table_names(_conn):
    """Fetches all user-defined table names from the public schema."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cursor:
            # This query gets all base tables from the 'public' schema
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            # The result is a list of tuples, so we extract the first element of each
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error fetching table list: {e}")
        return []
    # No need to close the connection here as it's managed by @st.cache_resource

@st.cache_data(ttl=600)
def get_table_columns(_conn, table_name: str) -> List[str]:
    """Fetches column names for a specific table, excluding the primary key."""
    conn = get_db_connection()
    if not conn or not table_name:
        return []
    try:
        with conn.cursor() as cursor:
            # This query gets column names, excluding the one that is the primary key
            query = sql.SQL("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                AND column_name NOT IN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                )
                ORDER BY ordinal_position;
            """)
            cursor.execute(query, (table_name, table_name))
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error fetching columns for table `{table_name}`: {e}")
        return []

# --- BI Dashboard Functions ---

@st.cache_data(ttl=600)
def get_sales_performance_data(_conn, period: str = 'Monthly'):
    """Fetches sales revenue and volume for a given period (Daily, Weekly, Monthly)."""
    period_map = {
        'Daily': 'day',
        'Weekly': 'week',
        'Monthly': 'month'
    }
    trunc_period = period_map.get(period, 'month')

    query = sql.SQL("""
        SELECT
            DATE_TRUNC({trunc_period}, o.order_date)::date AS order_period,
            SUM(o.total_amount_jpy) AS total_revenue,
            COUNT(oi.order_item_id) AS cars_sold
        FROM "order" o
        JOIN order_item oi ON o.order_id = oi.order_id
        WHERE o.order_status NOT IN ('cancelled')
        GROUP BY order_period
        ORDER BY order_period;
    """).format(trunc_period=sql.Literal(trunc_period))
    df = pd.read_sql_query(query.as_string(_conn), _conn)
    return df

@st.cache_data(ttl=600)
def get_sales_by_make_model_data(_conn):
    """Fetches sales data grouped by car make and model."""
    query = """
        SELECT
            c.make,
            c.model,
            COUNT(oi.order_item_id) AS units_sold,
            SUM(oi.unit_price_jpy) AS total_revenue_jpy
        FROM order_item oi
        JOIN car c ON oi.car_id = c.car_id
        JOIN "order" o ON oi.order_id = o.order_id
        WHERE o.order_status NOT IN ('cancelled')
        GROUP BY c.make, c.model;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_inventory_hotness_data(_conn):
    """Fetches car engagement metrics and aging for available inventory."""
    query = """
        SELECT
            make, model, year, view_count, add_to_cart_count,
            add_to_wishlist_count, current_listing_price_jpy,
            date_added_to_inventory,
            (add_to_cart_count + add_to_wishlist_count) as engagement_score
        FROM car
        WHERE status = 'available';
    """
    df = pd.read_sql_query(query, _conn)
    # Calculate inventory age in days
    if not df.empty and 'date_added_to_inventory' in df.columns:
        # Ensure the datetime column is timezone-aware (UTC) for accurate calculations
        df['date_added_to_inventory'] = pd.to_datetime(df['date_added_to_inventory'], utc=True)
        df['inventory_age_days'] = (pd.Timestamp.now(tz='UTC') - df['date_added_to_inventory']).dt.days
    return df

@st.cache_data(ttl=600)
def get_customer_demographics_data(_conn):
    """Fetches customer counts by country."""
    query = """
        SELECT country, COUNT(customer_id) AS number_of_customers
        FROM customer
        WHERE country IS NOT NULL AND country <> ''
        GROUP BY country ORDER BY number_of_customers DESC;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_sales_funnel_data(_conn):
    """ 
    Fetches data for a more accurate, user-centric sales funnel.
    This funnel tracks the number of unique customers at each stage.
    """
    # Stage 1: Unique customers who viewed any product
    query_viewers = "SELECT COUNT(DISTINCT customer_id) FROM product_view_history;"

    # Stage 2: Unique customers who added any item to their cart
    query_cart_adders = """
        SELECT COUNT(DISTINCT sc.customer_id)
        FROM cart_item ci
        JOIN shopping_cart sc ON ci.cart_id = sc.cart_id;
    """

    # Stage 3: Unique customers who placed a valid order
    query_purchasers = """
        SELECT COUNT(DISTINCT o.customer_id)
        FROM "order" o
        WHERE o.order_status NOT IN ('cancelled', 'pending_confirmation');
    """

    total_viewers, total_cart_adders, total_purchasers = 0, 0, 0
    try:
        with _conn.cursor() as cursor:
            cursor.execute(query_viewers)
            total_viewers = cursor.fetchone()[0]

            cursor.execute(query_cart_adders)
            total_cart_adders = cursor.fetchone()[0]

            cursor.execute(query_purchasers)
            total_purchasers = cursor.fetchone()[0]

    except (psycopg2.Error, TypeError, IndexError) as e:
        # Catch DB errors or issues if fetchone() is None or returns an empty row
        st.warning(f"Could not calculate all funnel stages, some data might be missing. Error: {e}")
        # We can still return a partial funnel if some queries succeeded
        pass

    # Combine into a single DataFrame for the funnel chart
    funnel_data = pd.DataFrame({
        'Stage': ['Unique Product Viewers', 'Added to Cart', 'Placed Order'],
        'Value': [
            total_viewers or 0,
            total_cart_adders or 0,
            total_purchasers or 0
        ]
    })
    return funnel_data

@st.cache_data(ttl=600)
def get_top_abandoned_cars_data(_conn):
    """
    Fetches the top 10 cars that are most frequently added to carts but have never been sold.
    This helps identify cars that might have issues with pricing, description, or availability.
    """
    query = """
        WITH CartAdditions AS (
            -- Count how many times each car has been added to any cart
            SELECT
                car_id,
                COUNT(cart_item_id) AS times_added_to_cart
            FROM cart_item
            GROUP BY car_id
        ),
        SoldCars AS (
            -- Get a unique list of cars that have been part of a successful order
            SELECT DISTINCT car_id
            FROM order_item oi
            JOIN "order" o ON oi.order_id = o.order_id
            WHERE o.order_status NOT IN ('cancelled', 'pending_confirmation')
        )
        -- Select cars that have been added to a cart but never sold
        SELECT
            c.make, c.model, c.year, c.current_listing_price_jpy, ca.times_added_to_cart
        FROM CartAdditions ca
        JOIN car c ON ca.car_id = c.car_id
        LEFT JOIN SoldCars sc ON ca.car_id = sc.car_id
        WHERE sc.car_id IS NULL -- The crucial filter: car has never been sold
        ORDER BY ca.times_added_to_cart DESC
        LIMIT 10;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_shipping_status_data(_conn):
    """Fetches counts of shipments by delivery status."""
    query = """
        SELECT delivery_status, COUNT(shipping_id) AS count
        FROM shipping_logistics
        GROUP BY delivery_status
        ORDER BY count DESC;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_shipping_carrier_performance_data(_conn):
    """
    Fetches average shipping cost and average delivery time by shipping company.
    Only considers completed deliveries.
    """
    query = """
        SELECT
            shipping_company_name,
            AVG(shipping_cost_jpy) AS average_cost_jpy,
            AVG(EXTRACT(EPOCH FROM (actual_arrival_date - ship_date))) / (60*60*24) AS average_delivery_days -- Convert seconds to days
        FROM shipping_logistics
        WHERE delivery_status = 'delivered' AND ship_date IS NOT NULL AND actual_arrival_date IS NOT NULL
        GROUP BY shipping_company_name
        ORDER BY average_cost_jpy ASC;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_in_transit_shipments(_conn):
    """Fetches details of shipments currently in transit, including origin and destination."""
    query = """
        SELECT
            sl.tracking_number,
            sl.shipping_company_name,
            sl.ship_date,
            sl.estimated_arrival_date,
            sl.current_location,
            sl.shipping_cost_jpy,
            c.make,
            c.model,
            sup.location AS origin_location,
            CONCAT(cust.city, ', ', cust.country) AS destination_location
        FROM shipping_logistics sl
        JOIN order_item oi ON sl.order_item_id = oi.order_item_id
        JOIN car c ON oi.car_id = c.car_id
        JOIN "order" o ON oi.order_id = o.order_id
        JOIN customer cust ON o.customer_id = cust.customer_id
        JOIN auction_supplier sup ON c.supplier_id = sup.supplier_id
        WHERE sl.delivery_status = 'in_transit'
        ORDER BY sl.estimated_arrival_date ASC;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_rfm_data(_conn):
    """Fetches Recency, Frequency, and Monetary values for each customer."""
    query = """
        WITH CustomerOrders AS (
            SELECT
                c.customer_id,
                c.first_name,
                c.last_name,
                o.order_date, -- Can be NULL for customers with no orders
                o.total_amount_jpy -- Can be NULL for customers with no orders
            FROM customer c
            LEFT JOIN "order" o ON c.customer_id = o.customer_id
            WHERE o.order_status NOT IN ('cancelled') OR o.order_id IS NULL -- Include customers with no orders
        ),
        RFM_Calculations AS (
            SELECT
                customer_id,
                first_name,
                last_name,
                MAX(order_date) AS last_order_date, -- NULL if no orders
                COUNT(order_date) AS frequency, -- 0 if no orders
                COALESCE(SUM(total_amount_jpy), 0) AS monetary_value, -- 0 if no orders
                CASE
                    WHEN MAX(order_date) IS NOT NULL THEN GREATEST(0, (CURRENT_DATE - MAX(order_date)::date))
                    ELSE NULL -- Recency is NULL for customers with no orders
                END AS recency_days
            FROM CustomerOrders
            GROUP BY customer_id, first_name, last_name
        )
        SELECT
            customer_id,
            first_name,
            last_name,
            recency_days,
            frequency,
            monetary_value
        FROM RFM_Calculations
        ORDER BY recency_days ASC, frequency DESC, monetary_value DESC;
    """
    df = pd.read_sql_query(query, _conn)
    return df

def assign_rfm_segment(recency, frequency, monetary):
    """
    Assigns an RFM segment based on more granular and distinct rules.
    The order of checks is crucial, moving from most valuable/recent to least.
    Handles customers with no purchases (frequency = 0) as a separate segment.
    """
    # Handle customers with no purchases (frequency == 0)
    if frequency == 0:
        return "Prospects (No Purchase)"

    # For customers with purchases (frequency >= 1)
    # 1. True Champions: Very recent, very frequent, very high spend
    if recency <= 15 and frequency >= 4 and monetary >= 20000000:
        return "True Champions"

    # 2. Loyal Customers: Recent, frequent, high spend
    elif recency <= 30 and frequency >= 3 and monetary >= 5000000:
        return "Loyal Customers"

    # 3. Recent Promising: Very recent, but maybe less frequent or lower spend than Loyal/Champions
    elif recency <= 45 and monetary >= 1000000:
        return "Recent Promising"

    # 4. Potential Loyalists: Recent, some frequency, decent spend
    elif recency <= 60 and frequency >= 2 and monetary >= 500000:
        return "Potential Loyalists"

    # 5. New Customers: Recent, single purchase
    elif recency <= 60 and frequency == 1:
        return "New Customers"

    # 6. Active but Infrequent: Still somewhat recent, but not very frequent
    elif recency <= 90 and frequency >= 1:
        return "Active but Infrequent"

    # 7. Needs Attention: Getting older, but still within a few months
    elif recency <= 120 and frequency >= 1:
        return "Needs Attention"

    # 8. At Risk: Approaching 6 months of inactivity
    elif recency <= 180 and frequency >= 1:
        return "At Risk"

    # 9. Hibernating: Inactive for a longer period, but not completely lost
    elif recency <= 365 and frequency >= 1:
        return "Hibernating"

    # 10. Lost: No purchase in over a year (but had purchases in the past)
    elif recency > 365 and frequency >= 1:
        return "Lost"

    # Fallback for any edge cases (should ideally not be hit if logic is comprehensive)
    return "Undefined Segment"

@st.cache_data(ttl=600) # Cache the entire geocoded result for 10 mins
def geocode_shipments_with_progress(_conn, in_transit_df: pd.DataFrame):
    """
    Takes the in-transit dataframe, geocodes all locations with a progress bar,
    and returns the geocoded data. The results are cached by Streamlit based on the dataframe's content.
    The _conn parameter is not used but helps Streamlit manage the cache context.
    """
    if in_transit_df.empty:
        return []

    # The progress bar will only show when this function is actually executed (i.e., on a cache miss)
    st.markdown("##### Geocoding shipment locations...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_rows = len(in_transit_df)
    
    geocoded_data = []

    for i, row in enumerate(in_transit_df.itertuples()):
        status_text.text(f"Processing shipment {i+1}/{total_rows}: {row.make} {row.model} (Tracking: {row.tracking_number})")
        
        # get_lat_lon is still cached with lru_cache for individual location lookups
        origin_coords = get_lat_lon(row.origin_location)
        dest_coords = get_lat_lon(row.destination_location)
        current_coords = get_lat_lon(row.current_location)
        
        geocoded_data.append({
            'origin': origin_coords, 'destination': dest_coords, 'current': current_coords,
            'origin_location': row.origin_location, 'destination_location': row.destination_location,
            'current_location': row.current_location, 'make': row.make, 'model': row.model,
            'tracking_number': row.tracking_number
        })
        
        progress_bar.progress((i + 1) / total_rows)

    # Clear progress indicators once done
    status_text.empty()
    progress_bar.empty()
    
    return geocoded_data

@functools.lru_cache(maxsize=None)
def standardize_country_name(name: str) -> str | None:
    """
    Finds the standard country name using pycountry.
    Returns the standard name, or the original name if not found.
    Handles common abbreviations and edge cases.
    """
    if not isinstance(name, str) or not name.strip():
        return None

    # Manual mapping for common cases that fuzzy search might miss or get wrong
    manual_map = {
        'USA': 'United States',
        'UK': 'United Kingdom',
        'England': 'United Kingdom',
        'UAE': 'United Arab Emirates',
        'Russia': 'Russian Federation',
        'Congo, Dem. Rep.': 'Congo, The Democratic Republic of the',
        'Congo, Rep.': 'Congo',
        'Korea, Rep.': 'Korea, Republic of',
        'Korea, Dem. Rep.': "Korea, Democratic People's Republic of",
    }
    if name in manual_map:
        name = manual_map[name]

    try:
        # pycountry's fuzzy search is good for slight misspellings or variations
        results = pycountry.countries.search_fuzzy(name)
        if results:
            return results[0].name
    except LookupError:
        # If nothing is found, return the original name to see if Plotly can handle it
        return name
    # Fallback to original name
    return name

# --- Data Display and Insertion Functions ---

def display_data(table_name: str):
    """Fetches and displays the first 100 rows of a given table."""
    conn = get_db_connection()
    if conn and table_name:
        try:
            # Use psycopg2.sql to safely format the query and prevent SQL injection
            query = sql.SQL("SELECT * FROM {table} LIMIT 100;").format(
                table=sql.Identifier(table_name)
            )
            df = pd.read_sql_query(query.as_string(conn), conn)
            # st.write(f"Displaying first 100 rows from `{table_name}`:") # Title is now handled in the calling tab
            if df.empty:
                st.warning("The table is empty.")
            else:
                st.dataframe(df)
        except Exception as e:
            st.error(f"Error fetching data from `{table_name}`: {e}")

def read_csv_data(file_path: str, table_name_for_logging: str = "") -> List[Dict[str, Any]]:
    """Reads data from a CSV file and returns a list of dictionaries."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file for table '{table_name_for_logging}' not found: {file_path}")
    data = []
    if os.path.getsize(file_path) == 0:
        st.info(f"Info: CSV file '{file_path}' for table '{table_name_for_logging}' is empty.")
        return data
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def insert_data_from_csv(cursor, csv_file_path: str, db_table_sql_identifier: str, csv_to_db_map: Dict[str, str], db_id_col_name: str) -> Tuple[List[int], int]:
    """Inserts data from CSV, skipping duplicates and handling errors row-by-row."""
    data_from_csv = read_csv_data(csv_file_path, db_table_sql_identifier)
    total_rows_in_csv = len(data_from_csv)
    if not data_from_csv:
        return [], 0

    inserted_ids = []
    db_columns_to_insert = list(csv_to_db_map.values())
    placeholders = ', '.join(['%s'] * len(db_columns_to_insert))
    
    # Note: db_identifier_for_sql for 'order' table is already quoted (""order"")
    sql_query = f"""
        INSERT INTO {db_table_sql_identifier} ({', '.join(f'"{col}"' for col in db_columns_to_insert)})
        VALUES ({placeholders}) RETURNING "{db_id_col_name}";
    """
    
    for i, row_dict in enumerate(data_from_csv):
        # Convert empty strings to None
        values_tuple = [row_dict.get(csv_col) if row_dict.get(csv_col) != '' else None for csv_col in csv_to_db_map.keys()]
        savepoint_name = f"sp_row_{i}"
        try:
            cursor.execute(f"SAVEPOINT {savepoint_name};")
            cursor.execute(sql_query, values_tuple)
            fetched_id = cursor.fetchone()
            if fetched_id:
                inserted_ids.append(fetched_id[0])
        except psycopg2.IntegrityError as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            # Specifically check for unique constraint violation
            if hasattr(e, 'diag') and e.diag.sqlstate == '23505':
                st.warning(f"Skipping duplicate row in '{db_table_sql_identifier}': {row_dict}")
            else:
                st.warning(f"Skipping row in '{db_table_sql_identifier}' due to integrity error: {e}")
        except (psycopg2.Error, Exception) as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            st.warning(f"Skipping row in '{db_table_sql_identifier}' due to an unexpected error: {e}")
            
    return inserted_ids, total_rows_in_csv

def run_insertion_for_table(config_key: str):
    """Orchestrates the data insertion for a specific table configuration."""
    conn = get_db_connection()
    if not conn:
        return
        
    config = csv_mappings.get(config_key)
    if not config:
        st.error(f"No configuration found for '{config_key}'")
        return

    st.info(f"Starting data insertion for {config_key}...")
    try:
        with conn.cursor() as cursor:
            inserted_ids, total_attempted = insert_data_from_csv(
                cursor,
                config["csv_file_path"],
                config["db_identifier_for_sql"],
                config["csv_to_db_column_map"],
                config["db_id_column_name"]
            )
            conn.commit()
            st.success(f"Finished for {config_key}. Inserted {len(inserted_ids)} new rows out of {total_attempted} attempted.")
    except FileNotFoundError as e:
        st.error(f"Could not find data file: {e}")
    except Exception as e:
        conn.rollback()
        st.error(f"A critical error occurred during insertion for {config_key}: {e}")

# --- Streamlit UI ---

st.set_page_config(page_title="Postgres DB Interface", layout="wide")
st.title("PostgreSQL Database Interface")

conn = get_db_connection()
table_names = get_table_names(conn) if conn else []

tab1, tab2, tab3, tab4 = st.tabs(["📊 View Data", "⬆️ Insert from File", "✍️ Add New Record", "📈 Business Intelligence"])

with tab1:
    st.header("Database Table Viewer")
    selected_table_to_view = st.selectbox(
        "Select a table to view",
        options=sorted(table_names),
        index=None,
        placeholder="Choose a table..."
    )
    if selected_table_to_view:
        display_data(selected_table_to_view)

with tab2:
    st.header("Insert Data from Local CSV")
    st.markdown("This uses the predefined mappings in the script to load data from the `data/` directory.")
    insertion_options = sorted(list(csv_mappings.keys()))
    selected_dataset_to_insert = st.selectbox(
        "Select a predefined dataset to insert",
        options=sorted(insertion_options),
        index=None,
        placeholder="Choose a dataset..."
    )
    if st.button("Run Local Insertion", disabled=not selected_dataset_to_insert):
        with st.spinner(f"Inserting data for {selected_dataset_to_insert}..."):
            run_insertion_for_table(selected_dataset_to_insert)

with tab3:
    st.header("Add New Records to a Table")
    selected_table_for_insert = st.selectbox(
        "Select a table to insert into",
        options=sorted(table_names),
        index=None,
        placeholder="Choose a table...",
        key="editor_table_select"
    )

    if selected_table_for_insert:
        # Display existing data for context
        st.subheader(f"Existing Data in `{selected_table_for_insert}`")
        display_data(selected_table_for_insert)

        columns_to_insert = get_table_columns(conn, selected_table_for_insert)
        if columns_to_insert:
            st.subheader("Add new rows below")

            # Use a form to batch the submission
            with st.form(key=f"data_editor_form_{selected_table_for_insert}"):
                # The data editor allows adding/removing rows dynamically
                edited_df = st.data_editor(
                    pd.DataFrame(columns=columns_to_insert), # Start with an empty but structured DF
                    num_rows="dynamic",
                    use_container_width=True,
                    key=f"data_editor_{selected_table_for_insert}"
                )

                submitted = st.form_submit_button("Save New Records")
                if submitted:
                    # Filter out rows that are completely empty
                    valid_rows = edited_df.dropna(how='all').loc[~(edited_df == '').all(axis=1)]

                    if not valid_rows.empty:
                        records_to_insert = valid_rows.to_dict('records')

                        # Build the SQL query for all insertable columns
                        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                            table=sql.Identifier(selected_table_for_insert),
                            fields=sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
                            values=sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
                        )

                        success_count = 0
                        error_count = 0

                        try:
                            with conn.cursor() as cursor:
                                for i, record in enumerate(records_to_insert):
                                    # Prepare values, converting pandas NA to None
                                    vals = [None if pd.isna(record.get(col)) else record.get(col) for col in columns_to_insert]
                                    savepoint_name = f"sp_editor_row_{i}"
                                    try:
                                        cursor.execute(f"SAVEPOINT {savepoint_name};")
                                        cursor.execute(query, vals)
                                        success_count += 1
                                    except Exception as row_error:
                                        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                                        st.warning(f"Could not insert row: `{record}`. Error: {row_error}")
                                        error_count += 1
                            conn.commit() # Commit all successful inserts

                            if success_count > 0:
                                st.success(f"Successfully added {success_count} new record(s) to `{selected_table_for_insert}`!")
                                st.cache_data.clear()
                            if error_count > 0:
                                st.error(f"Failed to add {error_count} record(s). See warnings above.")

                        except Exception as e:
                            conn.rollback()
                            st.error(f"A critical error occurred during transaction: {e}")
                    else:
                        st.warning("No data entered. Please add and fill at least one row.")
        else:
            st.warning(f"Could not retrieve columns for table `{selected_table_for_insert}`.")

with tab4:
    st.header("Business Intelligence Dashboard")
    st.markdown("""
    This dashboard provides live visualizations based on the data in the database.
    *(Note: You may need to run `pip install plotly` to view these charts.)*
    """)

    if not conn:
        st.error("Cannot display dashboards without a database connection.")
    else:
        bi_tab1, bi_tab2, bi_tab3, bi_tab4, bi_tab5, bi_tab6, bi_tab7 = st.tabs(["Sales Performance", "Top Brands", "Inventory Insights", "Customer Demographics", "Sales Funnel", "Shipping Operations", "Customer RFM"])

        with bi_tab1:
            st.subheader("Revenue & Sales Volume")
            period = st.radio(
                "Select Period",
                ('Monthly', 'Weekly', 'Daily'),
                horizontal=True,
                key='sales_period'
            )
            with st.spinner(f"Loading {period.lower()} sales data..."):
                sales_df = get_sales_performance_data(conn, period)
                if not sales_df.empty:
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(x=sales_df['order_period'], y=sales_df['cars_sold'], name='Cars Sold'), secondary_y=False)
                    fig.add_trace(go.Scatter(x=sales_df['order_period'], y=sales_df['total_revenue'], name='Revenue (JPY)', mode='lines+markers'), secondary_y=True)
                    fig.update_layout(
                        title_text=f"{period} Revenue and Sales Volume",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    fig.update_xaxes(title_text=f"Period ({period})")
                    fig.update_yaxes(title_text="<b>Cars Sold</b> (Units)", secondary_y=False)
                    fig.update_yaxes(title_text="<b>Revenue</b> (JPY)", secondary_y=True)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No sales data available for the {period.lower()} view.")

        with bi_tab2:
            st.subheader("Sales by Car Make and Model")
            st.markdown("This treemap shows sales grouped by car make. **Click on any make** to drill down and see the sales distribution by model.")
            with st.spinner("Loading brand data..."):
                make_model_df = get_sales_by_make_model_data(conn)
                if make_model_df.empty:
                    st.warning("No sales data available to display.")
                else:
                    # Treemap with drill-down capability
                    fig_treemap = px.treemap(
                        make_model_df,
                        path=[px.Constant("All Makes"), 'make', 'model'],
                        values='units_sold',
                        color='total_revenue_jpy',
                        color_continuous_scale='YlGnBu',
                        title='Sales by Car Make and Model (Click a make to expand)'
                    )

                    # The hovertemplate uses the aggregated color value for revenue, which is correct for parent nodes.
                    fig_treemap.update_traces(
                        hovertemplate="<b>%{label}</b><br>Units Sold: %{value}<br>Total Revenue: %{color:,.0f} JPY<extra></extra>",
                        textinfo='label+value',
                        textfont_size=14
                    )
                    
                    st.plotly_chart(fig_treemap, use_container_width=True)

        with bi_tab3:
            st.subheader("Inventory Insights")
            st.markdown("Analyze the current state of available car inventory, from value and age to customer engagement.")
            with st.spinner("Loading inventory data..."):
                inventory_df = get_inventory_hotness_data(conn)
                if inventory_df.empty:
                    st.warning("No available inventory data to display.")
                else:
                    # --- Key Metrics ---
                    total_inventory_count = len(inventory_df)
                    total_inventory_value = inventory_df['current_listing_price_jpy'].sum()
                    average_age = inventory_df['inventory_age_days'].mean() if 'inventory_age_days' in inventory_df.columns else 0

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Cars in Inventory", f"{total_inventory_count:,}")
                    col2.metric("Total Inventory Value (JPY)", f"¥{total_inventory_value:,.0f}")
                    col3.metric("Average Inventory Age (Days)", f"{average_age:.1f}")

                    st.divider()

                    # --- Inventory Aging ---
                    if 'inventory_age_days' in inventory_df.columns:
                        st.subheader("Inventory Aging Distribution")
                        fig_age = px.histogram(
                            inventory_df,
                            x='inventory_age_days',
                            nbins=20,
                            title='Distribution of Inventory Age',
                            labels={'inventory_age_days': 'Age in Days', 'count': 'Number of Cars'}
                        )
                        st.plotly_chart(fig_age, use_container_width=True)
                        st.divider()

                    # --- Hotness Map (existing chart) ---
                    st.subheader("Inventory 'Hotness' Map")
                    st.markdown("Which available cars get high views but low engagement, and which are your hidden gems? (Bubble size represents price)")
                    
                    # Filter for cars with at least one view for a more meaningful scatter plot
                    hotness_df = inventory_df[inventory_df['view_count'] > 0].copy()
                    if not hotness_df.empty:
                        hover_cols = ['year', 'current_listing_price_jpy']
                        if 'inventory_age_days' in hotness_df.columns:
                            hover_cols.append('inventory_age_days')
                        
                        fig_hotness = px.scatter(
                            hotness_df, x='view_count', y='engagement_score',
                            size='current_listing_price_jpy', color='make',
                            hover_name='model', hover_data=hover_cols,
                            title='Inventory "Hotness" Map (Available Cars with Views)',
                            labels={
                                'view_count': 'Product Page Views',
                                'engagement_score': 'Engagement (Adds to Cart/Wishlist)',
                                'current_listing_price_jpy': 'Price (JPY)',
                                'make': 'Car Make',
                                'inventory_age_days': 'Age (Days)'
                            },
                            log_x=True
                        )
                        st.plotly_chart(fig_hotness, use_container_width=True)

                        st.divider()

                        # --- Opportunity Tables ---
                        st.subheader("Actionable Inventory Segments")
                        col_opportunity1, col_opportunity2 = st.columns(2)

                        # Define thresholds for segmentation
                        view_threshold = hotness_df['view_count'].quantile(0.75) # Top 25% views
                        engagement_threshold = 1 # Simple threshold: at least one engagement action
                        
                        with col_opportunity1:
                            st.markdown("##### 🧐 High Views, Low Engagement")
                            st.markdown("_Consider reviewing price, photos, or description._")
                            high_views_low_engagement = hotness_df[(hotness_df['view_count'] >= view_threshold) & (hotness_df['engagement_score'] < engagement_threshold)].sort_values('view_count', ascending=False)
                            display_cols = ['make', 'model', 'year', 'view_count', 'engagement_score']
                            if 'inventory_age_days' in high_views_low_engagement.columns: display_cols.append('inventory_age_days')
                            st.dataframe(high_views_low_engagement[display_cols], use_container_width=True, height=300)

                        with col_opportunity2:
                            st.markdown("##### ✨ Hidden Gems (High Engagement, Low Views)")
                            st.markdown("_Consider promoting these cars to increase visibility._")
                            low_views_high_engagement = hotness_df[(hotness_df['view_count'] < view_threshold) & (hotness_df['engagement_score'] >= engagement_threshold)].sort_values('engagement_score', ascending=False)
                            display_cols = ['make', 'model', 'year', 'view_count', 'engagement_score']
                            if 'inventory_age_days' in low_views_high_engagement.columns: display_cols.append('inventory_age_days')
                            st.dataframe(low_views_high_engagement[display_cols], use_container_width=True, height=300)
                    else:
                        st.info("No cars with view data to display in the 'Hotness' map or actionable segments.")

        with bi_tab4:
            st.subheader("Customer Demographics by Country")
            st.markdown("""
            This choropleth map shows the distribution of customers across the globe. 
            The color intensity of each country corresponds to its number of customers. 
            This visualization is powered by Plotly's built-in mapping features and does not require any external API keys.
            """)
            with st.spinner("Loading and preparing map data..."):
                
                # Insert CSS for vertical centering, targeting only the map in bi_tab4
                st.markdown(
                    """
                    <style>
                        div[data-testid="stVerticalBlock"] > div:nth-child(2) > div:has(div.plotly-chart) {
                            justify-content: center;
                            align-items: center;
                            display: flex;
                            min-height: 80vh;
                        }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                customer_df = get_customer_demographics_data(conn)

                # --- Country Name Standardization ---
                # Standardize country names from the database to match what Plotly expects.
                # This uses the `pycountry` library for robust fuzzy matching.
                if not customer_df.empty:
                    customer_df['country'] = customer_df['country'].apply(standardize_country_name)
                    # After replacing, we group and sum again in case multiple names mapped to one
                    # (e.g., 'USA' and 'United States' both become 'United States').
                    customer_df = customer_df.groupby('country', as_index=False)['number_of_customers'].sum()

                # --- Get a Complete List of Countries ---
                # We generate a complete list of countries from `pycountry` to ensure the entire
                # world map is drawn, not just countries present in our data.
                try:
                    all_countries_list = [{'country': country.name} for country in pycountry.countries]
                    all_countries_df = pd.DataFrame(all_countries_list)
                except Exception as e:
                    st.error(f"Could not load country list from pycountry: {e}")
                    st.info("Please ensure the `pycountry` library is installed (`pip install pycountry`).")
                    # As a fallback, use an empty dataframe so the app doesn't crash.
                    all_countries_df = pd.DataFrame(columns=['country'])

                # Merge customer data with the comprehensive list of countries.
                # This ensures all countries are present in the dataframe used for mapping.
                # Countries without customer data will have NaN in 'number_of_customers'.
                merged_df = pd.merge(
                    all_countries_df,
                    customer_df,
                    on='country',
                    how='left'
                )

                # Fill NaN values (countries with no customers) with 0.
                merged_df['number_of_customers'] = merged_df['number_of_customers'].fillna(0)

                if customer_df.empty:
                    st.warning("No customer country data available. Displaying a world map with no customer-specific coloring.")

                # Create the choropleth map using the merged_df.
                # Countries with 0 customers will be colored with the lowest value in the colorscale.
                fig_map = go.Figure(data=go.Choropleth(
                    locations=merged_df['country'], # Use the standardized, complete list of country names
                    locationmode='country names',  # Use country names to match locations
                    z=merged_df['number_of_customers'],  # Data to be color-coded (0 for no customers)
                    text=merged_df.apply(lambda row: f"{row['country']}: {int(row['number_of_customers'])} customers", axis=1), # Custom hover text
                    colorscale='YlGnBu',
                    autocolorscale=False,
                    reversescale=False,
                    marker_line_color='darkgray',
                    marker_line_width=0.5,
                    colorbar_title='Number of<br>Customers',
                    hoverinfo='text' # Use custom text for hover
                ))

                fig_map.update_layout(
                    title_text='Global Customer Distribution',
                    geo=dict(
                        showframe=False,
                        showcoastlines=False,
                        projection_type='natural earth',
                    ),
                    width=1000,  # Set the desired width here
                    height=600   # You can also adjust the height if needed
                )

                col1, col2, col3 = st.columns([1, 3, 1])  # Create columns with a 3:1:1 ratio

                with col2:  # Place the map in the center column
                     st.plotly_chart(fig_map, use_container_width=False)  # Disable automatic width adjustment

        with bi_tab5: # Sales Funnel
            st.subheader("Customer Conversion Funnel")
            st.markdown("""
            This funnel tracks the journey of unique customers from initial product interest to a final purchase.
            It helps identify at which stage customers are dropping off.
            - **Unique Product Viewers**: The number of distinct customers who have viewed at least one car.
            - **Added to Cart**: The number of distinct customers who have added at least one car to their shopping cart.
            - **Placed Order**: The number of distinct customers who have successfully placed a valid order.
            """)
            with st.spinner("Loading funnel data..."):
                funnel_df = get_sales_funnel_data(conn)
                if not funnel_df.empty and funnel_df['Value'].sum() > 0:
                    fig_funnel = px.funnel(
                        funnel_df, x='Value', y='Stage',
                        title='Customer Conversion Funnel',
                        labels={'Value': 'Number of Unique Customers', 'Stage': 'Conversion Stage'}
                    )
                    # This update adds the conversion rate from the previous stage directly onto the funnel chart.
                    fig_funnel.update_traces(textposition='inside', textinfo='value+percent previous')
                    st.plotly_chart(fig_funnel, use_container_width=True)

                    st.divider()

                    # --- Abandoned Cart Analysis ---
                    st.subheader("Top Abandoned Cars in Carts")
                    st.markdown("These are the top 10 cars most frequently added to shopping carts but **never purchased**. This could indicate issues with price, shipping costs, or the checkout process for these specific items.")
                    with st.spinner("Analyzing abandoned carts..."):
                        abandoned_cars_df = get_top_abandoned_cars_data(conn)
                        if not abandoned_cars_df.empty:
                            st.dataframe(abandoned_cars_df.rename(columns={
                                'make': 'Make', 'model': 'Model', 'year': 'Year',
                                'current_listing_price_jpy': 'Price (JPY)',
                                'times_added_to_cart': 'Times Added to Cart'
                            }), use_container_width=True)
                        else:
                            st.info("No data available for abandoned cart analysis.")
                else:
                    st.warning("No data available for sales funnel analysis. Ensure tables like `product_view_history`, `cart_item`, and `order` are populated.")

        with bi_tab6:
            st.subheader("Shipping & Logistics Operations")
            st.markdown("Monitor the status and performance of your car shipments.")

            col_status, col_carrier = st.columns(2)

            with col_status:
                st.subheader("Delivery Status Breakdown")
                with st.spinner("Loading shipping status data..."):
                    status_df = get_shipping_status_data(conn)
                    if not status_df.empty:
                        fig_status = px.pie(
                            status_df,
                            values='count',
                            names='delivery_status',
                            title='Shipment Delivery Status',
                            hole=0.3
                        )
                        st.plotly_chart(fig_status, use_container_width=True)
                    else:
                        st.warning("No shipping status data available.")

            with col_carrier:
                st.subheader("Shipping Carrier Performance")
                with st.spinner("Loading carrier performance data..."):
                    carrier_df = get_shipping_carrier_performance_data(conn)
                    if not carrier_df.empty:
                        fig_carrier_cost = px.bar(
                            carrier_df,
                            x='shipping_company_name',
                            y='average_cost_jpy',
                            title='Average Shipping Cost by Carrier (JPY)',
                            labels={'shipping_company_name': 'Carrier', 'average_cost_jpy': 'Average Cost (JPY)'}
                        )
                        st.plotly_chart(fig_carrier_cost, use_container_width=True)

                        fig_carrier_time = px.bar(
                            carrier_df,
                            x='shipping_company_name',
                            y='average_delivery_days',
                            title='Average Delivery Time by Carrier (Days)',
                            labels={'shipping_company_name': 'Carrier', 'average_delivery_days': 'Average Days'}
                        )
                        st.plotly_chart(fig_carrier_time, use_container_width=True)
                    else:
                        st.warning("No carrier performance data available.")

            st.subheader("Live Shipment Progress Map")
            in_transit_df = get_in_transit_shipments(conn)

            if not in_transit_df.empty:
                # --- Geocoding with Progress Bar (now cached) ---
                # This function will only run if the in_transit_df has changed.
                # Otherwise, it will return the cached result instantly.
                geocoded_data = geocode_shipments_with_progress(conn, in_transit_df)
                
                # --- Plotting the Map ---
                fig_map = go.Figure()

                for item in geocoded_data:
                    origin, dest, current = item['origin'], item['destination'], item['current']

                    # Add lines connecting the points
                    if origin and dest:
                        fig_map.add_trace(go.Scattergeo(lon=[origin[1], dest[1]], lat=[origin[0], dest[0]], mode='lines', line=dict(width=1, color='gray', dash='dash'), hoverinfo='none', showlegend=False))
                    if origin and current:
                        fig_map.add_trace(go.Scattergeo(lon=[origin[1], current[1]], lat=[origin[0], current[0]], mode='lines', line=dict(width=2, color='blue'), hoverinfo='none', showlegend=False))

                    # Add markers for points
                    lons, lats, texts, marker_colors, marker_sizes = [], [], [], [], []
                    if origin:
                        lons.append(origin[1]); lats.append(origin[0]); texts.append(f"Origin: {item['origin_location']}"); marker_colors.append('green'); marker_sizes.append(8)
                    if dest:
                        lons.append(dest[1]); lats.append(dest[0]); texts.append(f"Destination: {item['destination_location']}"); marker_colors.append('red'); marker_sizes.append(8)
                    if current:
                        lons.append(current[1]); lats.append(current[0]); texts.append(f"Current: {item['current_location']}<br>Car: {item['make']} {item['model']}<br>Tracking: {item['tracking_number']}"); marker_colors.append('blue'); marker_sizes.append(12)

                    if lons:
                        fig_map.add_trace(go.Scattergeo(lon=lons, lat=lats, hoverinfo='text', text=texts, mode='markers', marker=dict(color=marker_colors, size=marker_sizes, line=dict(width=1, color='black')), showlegend=False))

                fig_map.update_layout(
                    title_text='Live Shipment Progress',
                    showlegend=False,
                    geo=dict(
                        scope='world', projection_type='natural earth', showland=True,
                        landcolor='rgb(243, 243, 243)', countrycolor='rgb(204, 204, 204)',
                        # center=dict(lat=36, lon=138) # Reverted: Map will now use default centering
                    ),
                    margin={"r":0,"t":40,"l":0,"b":0}
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("No shipments currently in transit to display on map.")

            st.subheader("In-Transit Shipments Details")
            # Reuse the dataframe fetched for the map
            if not in_transit_df.empty:
                st.dataframe(in_transit_df, use_container_width=True)
            else:
                st.info("No shipments currently in transit.")

        with bi_tab7:
            st.subheader("Customer RFM Analysis")
            st.markdown("Segment your customers based on Recency, Frequency, and Monetary value.")
            with st.spinner("Loading customer RFM data..."):
                rfm_df = get_rfm_data(conn)
                if not rfm_df.empty:
                    # Assign RFM segments
                    rfm_df['rfm_segment'] = rfm_df.apply(
                        lambda row: assign_rfm_segment(row['recency_days'], row['frequency'], row['monetary_value']),
                        axis=1
                    )

                    col_rfm_scatter, col_rfm_segments = st.columns(2)

                    with col_rfm_scatter:
                        fig_rfm_scatter = px.scatter(
                            rfm_df,
                            x='recency_days',
                            y='frequency',
                            size='monetary_value',
                            color='rfm_segment',
                            hover_name='first_name',
                            title='Customer RFM Scatter Plot',
                            labels={'recency_days': 'Recency (Days Since Last Order)', 'frequency': 'Frequency (Total Orders)', 'monetary_value': 'Monetary Value (JPY)'}
                        )
                        st.plotly_chart(fig_rfm_scatter, use_container_width=True)

                    with col_rfm_segments:
                        segment_counts = rfm_df['rfm_segment'].value_counts().reset_index()
                        segment_counts.columns = ['rfm_segment', 'count']
                        fig_rfm_segments = px.bar(
                            segment_counts,
                            x='rfm_segment',
                            y='count',
                            title='Customer Segments Distribution',
                            labels={'rfm_segment': 'RFM Segment', 'count': 'Number of Customers'}
                        )
                        st.plotly_chart(fig_rfm_segments, use_container_width=True)

                    st.subheader("RFM Data Table")
                    st.dataframe(rfm_df[['first_name', 'last_name', 'recency_days', 'frequency', 'monetary_value', 'rfm_segment']].rename(columns={
                        'first_name': 'First Name', 'last_name': 'Last Name', 'recency_days': 'Recency (Days)', 'frequency': 'Frequency (Orders)', 'monetary_value': 'Monetary (JPY)', 'rfm_segment': 'Segment'
                    }), use_container_width=True)

                else:
                    st.warning("No customer data available for RFM analysis. Ensure 'order' and 'customer' tables have data.")
