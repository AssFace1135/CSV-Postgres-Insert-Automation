# PostgreSQL Data Management and BI Dashboard

This project provides a Streamlit-based web application to interact with a PostgreSQL database. It allows for inserting sample data from CSV files, viewing tables, and visualizing business intelligence insights through an interactive dashboard.

## Features

- **GUI for Data Insertion**: Easily populate your PostgreSQL database from provided CSV files with a single click.
- **Database Table Viewer**: Browse the data in your database tables directly within the application.
- **Business Intelligence Dashboard**:
    - Analyze sales data over custom time periods.
    - Visualize customer demographics on a map.
    - Explore other key business metrics.
- **Transactional Integrity**: Data insertion is performed within a single transaction to ensure data consistency. If any part of the insertion fails, the entire operation is rolled back.

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
2.  **Database Schema**: The target database and schema (tables, enums, etc.) must be created before running the application. You can use the schema provided in schema.sql file to make the database.
3.  **Python 3.x**: Make sure you have a recent version of Python installed.
4.  **Python Libraries**: Install the necessary libraries. Based on the project, you'll need `streamlit`, `psycopg2-binary`, `pandas`, and `python-dotenv`.

    ```bash
    pip install streamlit psycopg2-binary pandas python-dotenv
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
