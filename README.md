# PostgreSQL Data Management and BI Dashboard

This project provides a Streamlit-based web application to interact with a PostgreSQL database. It allows for inserting sample data from CSV files, viewing tables, and visualizing business intelligence insights through an interactive dashboard.

## Features

- **One-Click Data Seeding**: Populate your PostgreSQL database from the provided CSV files with a single button click.
- **Dynamic Table Viewer**: Browse and search data across all tables directly within the application.
- **Interactive BI Dashboard**:
    - **Sales Analysis**: Analyze sales trends over custom time periods with interactive Plotly charts.
    - **Customer Demographics**: Visualize customer locations on a geographic map, with optimized performance via geocoding caching.
    - **RFM Customer Segmentation**: Perform Recency, Frequency, and Monetary (RFM) analysis to identify and segment key customer groups.
- **Transactional Integrity**: Ensures all data is inserted in a single, atomic transaction to maintain data consistency.
- **Optimized Geocoding**: Caches geocoding results to a local SQLite database (`geocoding_cache/cache.db`) to speed up map visualizations and reduce redundant API calls.

## Project Structure
```
Posgres_data_Insert_automation/
├── .env.example        # Example environment variables file
├── app.py              # The main Streamlit application script
├── data/               # Contains sample data in CSV format
│   ├── car.csv
│   ├── customer.csv
│   ├── order.csv
│   ├── order_item.csv
│   └── shipping_logistics.csv
└── README.md           # This file
```

## Pre-requisites

1.  **PostgreSQL**: A running PostgreSQL server instance.
2.  **Database Schema**: Before running the application, you must create the database and its schema. You can execute the `schema.sql` script using a tool like `psql` or your favorite SQL client to set up the required tables and types.
3.  **Python 3.8+**: Make sure you have a recent version of Python installed.
4.  **Python Libraries**: Install the necessary libraries using pip. These include `streamlit` for the web app, `psycopg2-binary` for PostgreSQL connection, `pandas` for data manipulation, `python-dotenv` for configuration, `plotly` for interactive charts, and `geopy` for geocoding addresses.

```
   pip install streamlit psycopg2-binary pandas python-dotenv plotly geopy
```



## Configuration

1.  Clone this repository to your local machine.
2.  Create a `.env` file in the root of the project directory by copying the example file. In your terminal, you can run:
    ```bash
    cp .env.example .env
    ```
3.  Open the `.env` file and update the variables with your actual PostgreSQL database credentials:
    ```ini
    DB_HOST=your_db_host
    DB_NAME=your_db_name
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    DB_PORT=your_db_port
    ```

## How to Use

1.  Ensure all pre-requisites and configuration steps are completed.
2.  Navigate to the project's root directory in your terminal.
3.  Run the Streamlit application:
    ```bash
    streamlit run app.py
    ```
4.  The application will open in a new tab in your default web browser, ready to use.
