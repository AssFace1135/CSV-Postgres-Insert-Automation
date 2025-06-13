###Python script to populate a complete PostgreSQL database schema.

This script uses 'psycopg2' and inserts sample data into every table
from the provided schema, respecting all foreign key constraints.

## PRE-REQUISITES:
1. PostgreSQL server must be running.
2. The database schema (tables, enums, etc.) must be created.
3. Install the psycopg2 library:
```
pip install psycopg2-binary
```

## HOW TO USE:
1. Update the 'db_credentials' with your actual PostgreSQL details.
2. Run the script: python your_script_name.py

The script will perform all insertions within a single transaction.
If any step fails, the entire transaction is rolled back.
