"""
Setup all Snowflake catalog tables with sample data.

This script creates all the necessary catalog tables and populates them with sample data.
It should be run after a successful connection to Snowflake has been established.
"""
import os
import sys
import uuid
import snowflake.connector
from dotenv import load_dotenv

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.db_connection.snowflake_metadata_helper import initialize_snowflake_catalog
from backend.db_connection.fix_syntax_error import fix_syntax_error

# Fix any syntax errors in metadata module
fix_syntax_error()

# Load environment variables
load_dotenv()

def insert_sample_data(connection):
    """
    Insert sample data into catalog tables.
    
    Args:
        connection: Snowflake connection object
    """
    try:
        cursor = connection.cursor()
        
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
        
        # Commit all changes
        connection.commit()
        print("Sample data initialization completed successfully")
        
    except Exception as e:
        print(f"Error initializing sample data: {str(e)}")
        
def setup_all_tables():
    """
    Set up all Snowflake catalog tables and initialize with sample data.
    
    Returns:
        bool: Success flag
    """
    # Get connection parameters from environment variables
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    role = os.environ.get('SNOWFLAKE_ROLE')
    
    # Validate required parameters
    if not account or not user or not password:
        print("Error: Missing required environment variables")
        print(f"SNOWFLAKE_ACCOUNT: {'Set' if account else 'Missing'}")
        print(f"SNOWFLAKE_USER: {'Set' if user else 'Missing'}")
        print(f"SNOWFLAKE_PASSWORD: {'Set' if password else 'Missing'}")
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
        
        # Initialize catalog tables
        print("Initializing Snowflake catalog tables...")
        initialize_snowflake_catalog(conn)
        
        # Insert sample data
        insert_sample_data(conn)
        
        # Close connection
        conn.close()
        print("Connection closed")
        
        return True
        
    except Exception as e:
        print(f"Error setting up tables: {str(e)}")
        return False

if __name__ == "__main__":
    print("Setting up all Snowflake catalog tables...")
    success = setup_all_tables()
    if success:
        print("\nAll tables have been successfully set up with sample data.")
        print("You can now use the metadata collection functionality.")
    else:
        print("\nFailed to set up tables. See errors above.") 