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
def get_sales_performance_data(_conn):
    """Fetches monthly sales revenue and volume."""
    query = """
        SELECT
            TO_CHAR(o.order_date, 'YYYY-MM') AS order_month,
            SUM(o.total_amount_jpy) AS total_revenue,
            COUNT(oi.order_item_id) AS cars_sold
        FROM "order" o
        JOIN order_item oi ON o.order_id = oi.order_id
        WHERE o.order_status NOT IN ('cancelled')
        GROUP BY order_month
        ORDER BY order_month;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_sales_by_make_data(_conn):
    """Fetches sales data grouped by car make, limited to the top 15."""
    query = """
        SELECT
            c.make,
            COUNT(oi.order_item_id) AS units_sold,
            SUM(oi.unit_price_jpy) AS total_revenue_jpy
        FROM order_item oi
        JOIN car c ON oi.car_id = c.car_id
        GROUP BY c.make
        ORDER BY units_sold DESC
        LIMIT 15;
    """
    df = pd.read_sql_query(query, _conn)
    return df

@st.cache_data(ttl=600)
def get_inventory_hotness_data(_conn):
    """Fetches car engagement metrics for available inventory."""
    query = """
        SELECT
            make, model, year, view_count, add_to_cart_count,
            add_to_wishlist_count, current_listing_price_jpy,
            (add_to_cart_count + add_to_wishlist_count) as engagement_score
        FROM car
        WHERE status = 'available' AND view_count > 0;
    """
    df = pd.read_sql_query(query, _conn)
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
        bi_tab1, bi_tab2, bi_tab3, bi_tab4 = st.tabs(["Sales Performance", "Top Brands", "Inventory Insights", "Customer Demographics"])

        with bi_tab1:
            st.subheader("Monthly Revenue & Sales Volume")
            with st.spinner("Loading sales data..."):
                sales_df = get_sales_performance_data(conn)
                if not sales_df.empty:
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(x=sales_df['order_month'], y=sales_df['cars_sold'], name='Cars Sold'), secondary_y=False)
                    fig.add_trace(go.Scatter(x=sales_df['order_month'], y=sales_df['total_revenue'], name='Revenue (JPY)', mode='lines+markers'), secondary_y=True)
                    fig.update_layout(title_text="Monthly Revenue and Sales Volume", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    fig.update_xaxes(title_text="Month")
                    fig.update_yaxes(title_text="<b>Cars Sold</b> (Units)", secondary_y=False)
                    fig.update_yaxes(title_text="<b>Revenue</b> (JPY)", secondary_y=True)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No sales data available to display.")

        with bi_tab2:
            st.subheader("Sales by Car Make")
            with st.spinner("Loading brand data..."):
                make_df = get_sales_by_make_data(conn)
                if not make_df.empty:
                    fig = px.bar(
                        make_df, x='units_sold', y='make', orientation='h',
                        title='Top 15 Car Makes by Units Sold',
                        labels={'units_sold': 'Number of Units Sold', 'make': 'Car Make'},
                        text='units_sold', hover_data=['total_revenue_jpy']
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No sales data available to display.")

        with bi_tab3:
            st.subheader("Inventory 'Hotness' Map")
            st.markdown("Which available cars get high views but low engagement, and which are your hidden gems?")
            with st.spinner("Loading inventory data..."):
                hotness_df = get_inventory_hotness_data(conn)
                if not hotness_df.empty:
                    fig = px.scatter(
                        hotness_df, x='view_count', y='engagement_score',
                        size='current_listing_price_jpy', color='make',
                        hover_name='model', hover_data=['year', 'current_listing_price_jpy'],
                        title='Inventory "Hotness" Map (Available Cars)',
                        labels={
                            'view_count': 'Product Page Views',
                            'engagement_score': 'Engagement (Adds to Cart/Wishlist)',
                            'current_listing_price_jpy': 'Price (JPY)',
                            'make': 'Car Make'
                        },
                        log_x=True
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No available inventory data to display.")

        with bi_tab4:
            st.subheader("Customer Demographics by Country")
            st.markdown("""
            This choropleth map shows the distribution of customers across the globe. 
            The color intensity of each country corresponds to its number of customers. 
            This visualization is powered by Plotly's built-in mapping features and does not require any external API keys.
            """)
            with st.spinner("Loading and preparing map data..."):
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
                        projection_type='natural earth'
                    )
                )

                st.plotly_chart(fig_map, use_container_width=True)
