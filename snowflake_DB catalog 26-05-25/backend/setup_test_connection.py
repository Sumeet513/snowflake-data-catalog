"""
This script creates a test connection entry in the SNOWFLAKE_CATALOG.PUBLIC.CONNECTIONS table
to enable testing of the search functionality.

Usage:
python setup_test_connection.py
"""

import os
import uuid
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
import sys
import uuid

# Import from local modules
from db_connection.snowflake_metadata_helper import initialize_snowflake_catalog
from db_connection.fix_syntax_error import fix_syntax_error

# Fix any syntax errors in metadata module
fix_syntax_error()

# Load environment variables from .env file
load_dotenv()

def setup_test_connection():
    """
    Set up a test connection in SNOWFLAKE_CATALOG.PUBLIC.CONNECTIONS table
    """
    # Get connection parameters from environment variables
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    role = os.environ.get('SNOWFLAKE_ROLE')
    
    # Validate required parameters
    if not account or not user or not password or not warehouse:
        print("Error: Missing required environment variables")
        print(f"SNOWFLAKE_ACCOUNT: {'Set' if account else 'Missing'}")
        print(f"SNOWFLAKE_USER: {'Set' if user else 'Missing'}")
        print(f"SNOWFLAKE_PASSWORD: {'Set' if password else 'Missing'}")
        print(f"SNOWFLAKE_WAREHOUSE: {'Set' if warehouse else 'Missing'}")
        return False
    
    try:
        # Connect to Snowflake
        print(f"Connecting to Snowflake account: {account}")
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role
        )
        print("Connected to Snowflake successfully")
        
        # Create database if it doesn't exist
        print("Creating SNOWFLAKE_CATALOG database (if not exists)")
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
        
        # Create schema if it doesn't exist
        print("Creating PUBLIC schema (if not exists)")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_CATALOG.PUBLIC")
        
        # Initialize all catalog tables
        print("Creating all catalog tables...")
        initialize_snowflake_catalog(conn)
        
        # Create sample data in the tables
        insert_sample_data(conn, cursor)
        
        # Verify the connection was saved
        print("Verifying the connection was saved correctly:")
        cursor.execute("""
            SELECT CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, DATABASE_NAME, SCHEMA_NAME, ROLE, STATUS
            FROM SNOWFLAKE_CATALOG.PUBLIC.CATALOG_CONNECTIONS
            WHERE ACCOUNT = %s AND USERNAME = %s
        """, (account, user))
        
        result = cursor.fetchone()
        if result:
            print("Connection saved successfully:")
            print(f"  ID: {result[0]}")
            print(f"  Account: {result[1]}")
            print(f"  Username: {result[2]}")
            print(f"  Warehouse: {result[3]}")
            print(f"  Database: {result[4]}")
            print(f"  Schema: {result[5]}")
            print(f"  Role: {result[6]}")
            print(f"  Status: {result[7]}")
        else:
            print("Warning: Connection was not found after saving!")
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        print("Setup completed successfully")
        return True
        
    except Exception as e:
        print(f"Error setting up test connection: {str(e)}")
        return False

def insert_sample_data(conn, cursor):
    """Insert sample data into all catalog tables"""
    try:
        print("Inserting sample data into catalog tables...")
        
        # Add a sample database record
        database_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO CATALOG_DATABASES (DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, COMMENT) "
            "VALUES (%s, %s, %s, %s)",
            (database_id, "SAMPLE_DB", "ACCOUNTADMIN", "A sample database with customer data")
        )
        print("Added sample database record")
        
        # Add a sample schema record
        schema_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO CATALOG_SCHEMAS (SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, SCHEMA_OWNER, COMMENT) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (schema_id, "PUBLIC", database_id, "SAMPLE_DB", "ACCOUNTADMIN", "Default public schema")
        )
        print("Added sample schema record")
        
        # Add a sample table record
        table_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO CATALOG_TABLES (TABLE_ID, TABLE_NAME, SCHEMA_ID, SCHEMA_NAME, "
            "DATABASE_ID, DATABASE_NAME, TABLE_TYPE, TABLE_OWNER, COMMENT, ROW_COUNT) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (table_id, "CUSTOMERS", schema_id, "PUBLIC", database_id, "SAMPLE_DB", 
             "TABLE", "ACCOUNTADMIN", "Customer information table", 1000)
        )
        print("Added sample table record")
        
        # Add sample column records
        column_fields = [
            ("CUSTOMER_ID", "NUMBER", "Primary key for customer records", True, False),
            ("FIRST_NAME", "VARCHAR", "Customer's first name", False, False),
            ("LAST_NAME", "VARCHAR", "Customer's last name", False, False),
            ("EMAIL", "VARCHAR", "Customer's email address", False, False),
            ("PHONE", "VARCHAR", "Customer's phone number", False, False),
            ("CREATED_AT", "TIMESTAMP_NTZ", "When the customer record was created", False, False)
        ]
        
        for idx, (col_name, data_type, comment, is_primary, is_foreign) in enumerate(column_fields):
            column_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO CATALOG_COLUMNS (COLUMN_ID, COLUMN_NAME, TABLE_ID, TABLE_NAME, "
                "SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, ORDINAL_POSITION, "
                "DATA_TYPE, IS_NULLABLE, COMMENT, IS_PRIMARY_KEY, IS_FOREIGN_KEY) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (column_id, col_name, table_id, "CUSTOMERS", schema_id, "PUBLIC", 
                 database_id, "SAMPLE_DB", idx + 1, data_type, not is_primary, 
                 comment, is_primary, is_foreign)
            )
        print("Added sample column records")
        
        # Generate a unique connection ID for the test connection
        connection_id = str(uuid.uuid4())
        
        # Define the database and schema for metadata
        database_name = "SNOWFLAKE_CATALOG"
        schema_name = "PUBLIC"
        
        # Insert connection into CATALOG_CONNECTIONS table
        print(f"Inserting test connection with ID: {connection_id}")
        cursor.execute("""
            INSERT INTO CATALOG_CONNECTIONS (
                CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, ROLE, DATABASE_NAME, SCHEMA_NAME,
                CREATED_AT, LAST_USED, STATUS
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, 
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'ACTIVE'
            )
        """, (
            connection_id, 
            os.environ.get('SNOWFLAKE_ACCOUNT'),
            os.environ.get('SNOWFLAKE_USER'),
            os.environ.get('SNOWFLAKE_WAREHOUSE'),
            os.environ.get('SNOWFLAKE_ROLE'),
            database_name,
            schema_name
        ))
        
        # Commit the transaction
        conn.commit()
        print("Sample data inserted successfully")
        
    except Exception as e:
        print(f"Error inserting sample data: {str(e)}")
        raise

if __name__ == "__main__":
    print("Setting up test connection in SNOWFLAKE_CATALOG.PUBLIC.CONNECTIONS table")
    success = setup_test_connection()
    if success:
        print("\nTest connection has been successfully set up.")
        print("You can now use the search functionality with stored credentials.")
    else:
        print("\nFailed to set up test connection. See errors above.") 