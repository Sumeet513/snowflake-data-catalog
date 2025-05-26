"""
Helper functions for Snowflake metadata collection with better error handling.
"""
import snowflake.connector
from typing import Dict, Any, Tuple, Optional
from django.core.cache import cache

def connect_to_snowflake(connection_params: Dict[str, Any]) -> Tuple[bool, Optional[snowflake.connector.SnowflakeConnection], str]:
    """
    Connect to Snowflake with better error handling.
    
    Args:
        connection_params: Dictionary with connection parameters
        
    Returns:
        Tuple containing:
        - success: Boolean indicating if connection was successful
        - connection: SnowflakeConnection object or None if failed
        - message: Success or error message
    """
    try:
        print(f"Attempting to connect to Snowflake with account: {connection_params['account']}")
        # Log connection parameters (with masked password)
        connection_params_log = {
            'account': connection_params['account'],
            'user': connection_params['username'],
            'password': '********',  # Masked for security
            'warehouse': connection_params.get('warehouse', ''),
            'role': connection_params.get('role', '')
        }
        print(f"Connection parameters: {connection_params_log}")
        
        # Create a direct connection with the provided credentials
        connection = snowflake.connector.connect(
            account=connection_params['account'],
            user=connection_params['username'],
            password=connection_params['password'],
            warehouse=connection_params.get('warehouse', ''),
            role=connection_params.get('role', ''),
            login_timeout=60,  # Increase login timeout
            network_timeout=60  # Increase network timeout
        )
        
        # Test the connection with a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        if result:
            version = result[0]
            print(f"Successfully connected to Snowflake! Version: {version}")
        else:
            print("Connected to Snowflake but received no version info.")
        cursor.close()
        
        return True, connection, "Connection successful"
        
    except snowflake.connector.errors.ProgrammingError as e:
        # Handle specific Snowflake errors
        error_code = getattr(e, 'errno', None)
        error_message = str(e)
        
        print(f"Snowflake connection error (code {error_code}): {error_message}")
        
        # Add troubleshooting tips for common errors
        tip = ""
        if "Could not connect to Snowflake backend" in error_message:
            tip = ("Check your network connection, firewall settings, or VPN. Ensure you can reach the Snowflake service.\n"
                  "Verify that the hostnames and port numbers in SYSTEM$ALLOWLIST are added to your firewall's allowed list.\n"
                  "If you're behind a corporate firewall or using a VPN, consult your network administrator.\n"
                  "See https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/overview for more details.")
        elif "Authentication failed" in error_message:
            tip = "Check your username, password, and account identifier. Ensure your account is active and not locked."
        elif "Organization" in error_message and "does not exist" in error_message:
            tip = "Check your account identifier - it appears to be incorrect. Snowflake account identifiers typically follow the format 'orgname-accountname'."
        elif "Account must be specified" in error_message:
            tip = "You must provide a valid Snowflake account identifier. This is required for connection."
        elif "Access denied" in error_message:
            tip = "Your credentials are valid but you don't have permission to access this resource. Contact your Snowflake administrator."
        
        # Format error message with troubleshooting tip
        detailed_message = f"Snowflake connection error: {error_message}" + (f"\n\nTip: {tip}" if tip else "")
        return False, None, detailed_message
        
    except Exception as e:
        # Handle any other errors
        error_type = type(e).__name__
        error_message = str(e)
        print(f"Connection error: {error_type} - {error_message}")
        
        return False, None, f"Failed to connect to Snowflake: {error_type} - {error_message}"

def update_process_status(process_id: str, status_data: Dict[str, Any], timeout: int = 3600) -> None:
    """
    Update the status of a process in the cache.
    
    Args:
        process_id: ID of the process
        status_data: Dictionary with status data
        timeout: Cache timeout in seconds
    """
    key = f"process_status_{process_id}"
    cache.set(key, status_data, timeout)
    print(f"Updated status for process {process_id}: {status_data}")

def initialize_snowflake_catalog(connection: snowflake.connector.SnowflakeConnection) -> None:
    """
    Initialize the catalog tables in Snowflake.
    
    Args:
        connection: Active Snowflake connection
    """
    try:
        print("=== DEBUGGING: Initializing SNOWFLAKE_CATALOG in Snowflake... ===")
        
        # Check current role and privileges
        cursor = connection.cursor()
        print("Checking current role and privileges...")
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_USER()")
        result = cursor.fetchone()
        if result:
            print(f"Current role: {result[0]}, Current user: {result[1]}")
        
        # Get list of databases the user can see
        print("Checking accessible databases...")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"User can see {len(databases)} databases")
        
        # Create database and schemas if not exists
        print("Creating SNOWFLAKE_CATALOG database...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
        print("Switching to SNOWFLAKE_CATALOG database...")
        cursor.execute("USE DATABASE SNOWFLAKE_CATALOG")
        
        # Use only PUBLIC schema
        print("Creating PUBLIC schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
        
        # Create all tables directly in PUBLIC schema
        print("Switching to PUBLIC schema...")
        cursor.execute("USE SCHEMA PUBLIC")
        
        # Create CATALOG_DATABASES table
        print("Creating CATALOG_DATABASES table in PUBLIC schema...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CATALOG_DATABASES (
                DATABASE_ID VARCHAR(255) PRIMARY KEY,
                DATABASE_NAME VARCHAR(255) NOT NULL,
                DATABASE_OWNER VARCHAR(255),
                COMMENT TEXT,
                CREATED_AT TIMESTAMP_NTZ,
                LAST_ALTERED TIMESTAMP_NTZ,
                COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Verify table creation
        cursor.execute("SHOW TABLES LIKE 'CATALOG_DATABASES'")
        tables = cursor.fetchall()
        if tables:
            print(f"✓ CATALOG_DATABASES table exists in PUBLIC schema")
        else:
            print(f"✗ Failed to create CATALOG_DATABASES table in PUBLIC schema")
            raise Exception("Failed to create tables in PUBLIC schema - check permissions")
            
        # Create other catalog tables
        print("Creating CATALOG_SCHEMAS table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CATALOG_SCHEMAS (
                SCHEMA_ID VARCHAR(255) PRIMARY KEY,
                SCHEMA_NAME VARCHAR(255) NOT NULL,
                DATABASE_ID VARCHAR(255) NOT NULL,
                DATABASE_NAME VARCHAR(255) NOT NULL,
                SCHEMA_OWNER VARCHAR(255),
                COMMENT TEXT,
                CREATED_AT TIMESTAMP_NTZ,
                LAST_ALTERED TIMESTAMP_NTZ,
                COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (DATABASE_ID) REFERENCES CATALOG_DATABASES(DATABASE_ID)
            )
        """)
        
        print("Creating CATALOG_TABLES table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CATALOG_TABLES (
                TABLE_ID VARCHAR(255) PRIMARY KEY,
                TABLE_NAME VARCHAR(255) NOT NULL,
                SCHEMA_ID VARCHAR(255) NOT NULL,
                SCHEMA_NAME VARCHAR(255) NOT NULL,
                DATABASE_ID VARCHAR(255) NOT NULL,
                DATABASE_NAME VARCHAR(255) NOT NULL,
                TABLE_TYPE VARCHAR(50),
                TABLE_OWNER VARCHAR(255),
                COMMENT TEXT,
                ROW_COUNT NUMBER,
                BYTES NUMBER,
                CREATED_AT TIMESTAMP_NTZ,
                LAST_ALTERED TIMESTAMP_NTZ,
                COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (SCHEMA_ID) REFERENCES CATALOG_SCHEMAS(SCHEMA_ID)
            )
        """)
        
        print("Creating CATALOG_COLUMNS table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CATALOG_COLUMNS (
                COLUMN_ID VARCHAR(255) PRIMARY KEY,
                COLUMN_NAME VARCHAR(255) NOT NULL,
                TABLE_ID VARCHAR(255) NOT NULL,
                TABLE_NAME VARCHAR(255) NOT NULL,
                SCHEMA_ID VARCHAR(255) NOT NULL,
                SCHEMA_NAME VARCHAR(255) NOT NULL,
                DATABASE_ID VARCHAR(255) NOT NULL,
                DATABASE_NAME VARCHAR(255) NOT NULL,
                ORDINAL_POSITION NUMBER,
                DATA_TYPE VARCHAR(255),
                CHARACTER_MAXIMUM_LENGTH NUMBER,
                NUMERIC_PRECISION NUMBER,
                NUMERIC_SCALE NUMBER,
                IS_NULLABLE BOOLEAN,
                COLUMN_DEFAULT TEXT,
                COMMENT TEXT,
                IS_PRIMARY_KEY BOOLEAN,
                IS_FOREIGN_KEY BOOLEAN,
                COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (TABLE_ID) REFERENCES CATALOG_TABLES(TABLE_ID)
            )
        """)
        
        print("Creating CATALOG_CONNECTIONS table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CATALOG_CONNECTIONS (
                CONNECTION_ID VARCHAR(255) PRIMARY KEY,
                ACCOUNT VARCHAR(255) NOT NULL,
                USERNAME VARCHAR(255) NOT NULL,
                WAREHOUSE VARCHAR(255),
                ROLE VARCHAR(255),
                DATABASE_NAME VARCHAR(255),
                SCHEMA_NAME VARCHAR(255),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                LAST_USED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                STATUS VARCHAR(50) DEFAULT 'ACTIVE'
            )
        """)
        
        # Commit changes
        print("Committing changes...")
        connection.commit()
        
        # Final verification
        print("Verifying table creation...")
        cursor.execute("SHOW TABLES")
        final_tables = cursor.fetchall()
        print(f"Tables in PUBLIC schema: {[t[1] for t in final_tables if t]}")
        
        print("SNOWFLAKE_CATALOG initialization complete")
        
    except Exception as e:
        print(f"ERROR initializing SNOWFLAKE_CATALOG: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def force_create_catalog_tables(connection_params: Dict[str, Any]) -> bool:
    """
    Force create catalog tables using direct SQL in case the normal initialization fails.
    
    Args:
        connection_params: Dictionary with Snowflake connection parameters
        
    Returns:
        bool: True if successful, False otherwise
    """
    print("=== ATTEMPTING TO FORCE CREATE CATALOG TABLES ===")
    try:
        # Create a fresh connection specifically for this operation
        conn = snowflake.connector.connect(
            account=connection_params['account'],
            user=connection_params['username'],
            password=connection_params['password'],
            warehouse=connection_params.get('warehouse', ''),
            role=connection_params.get('role', ''),
            login_timeout=60,
            network_timeout=60
        )
        
        cursor = conn.cursor()
        
        # Try to use ACCOUNTADMIN role if possible
        try:
            print("Attempting to use ACCOUNTADMIN role...")
            cursor.execute("USE ROLE ACCOUNTADMIN")
            print("Successfully switched to ACCOUNTADMIN role")
        except Exception as e:
            print(f"Could not switch to ACCOUNTADMIN role: {str(e)}")
            print("Continuing with current role...")
        
        # Create database and schema with force flag
        print("Creating database with force flag...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
        cursor.execute("USE DATABASE SNOWFLAKE_CATALOG")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
        cursor.execute("USE SCHEMA PUBLIC")
        
        # Create tables with explicit grants
        print("Creating CATALOG_DATABASES table with explicit grants...")
        cursor.execute("""
        CREATE OR REPLACE TABLE CATALOG_DATABASES (
            DATABASE_ID VARCHAR(255) PRIMARY KEY,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            DATABASE_OWNER VARCHAR(255),
            COMMENT TEXT,
            CREATED_AT TIMESTAMP_NTZ,
            LAST_ALTERED TIMESTAMP_NTZ,
            COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        # Grant access to the table
        cursor.execute("GRANT ALL ON TABLE CATALOG_DATABASES TO ROLE PUBLIC")
        
        # Directly create other tables without foreign keys first
        print("Creating other tables without foreign keys...")
        cursor.execute("""
        CREATE OR REPLACE TABLE CATALOG_SCHEMAS (
            SCHEMA_ID VARCHAR(255) PRIMARY KEY,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            DATABASE_ID VARCHAR(255) NOT NULL,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_OWNER VARCHAR(255),
            COMMENT TEXT,
            CREATED_AT TIMESTAMP_NTZ,
            LAST_ALTERED TIMESTAMP_NTZ,
            COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        cursor.execute("GRANT ALL ON TABLE CATALOG_SCHEMAS TO ROLE PUBLIC")
        
        cursor.execute("""
        CREATE OR REPLACE TABLE CATALOG_TABLES (
            TABLE_ID VARCHAR(255) PRIMARY KEY,
            TABLE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_ID VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            DATABASE_ID VARCHAR(255) NOT NULL,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            TABLE_TYPE VARCHAR(50),
            TABLE_OWNER VARCHAR(255),
            COMMENT TEXT,
            ROW_COUNT NUMBER,
            BYTES NUMBER,
            CREATED_AT TIMESTAMP_NTZ,
            LAST_ALTERED TIMESTAMP_NTZ,
            COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        cursor.execute("GRANT ALL ON TABLE CATALOG_TABLES TO ROLE PUBLIC")
        
        cursor.execute("""
        CREATE OR REPLACE TABLE CATALOG_COLUMNS (
            COLUMN_ID VARCHAR(255) PRIMARY KEY,
            COLUMN_NAME VARCHAR(255) NOT NULL,
            TABLE_ID VARCHAR(255) NOT NULL,
            TABLE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_ID VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            DATABASE_ID VARCHAR(255) NOT NULL,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            ORDINAL_POSITION NUMBER,
            DATA_TYPE VARCHAR(255),
            CHARACTER_MAXIMUM_LENGTH NUMBER,
            NUMERIC_PRECISION NUMBER,
            NUMERIC_SCALE NUMBER,
            IS_NULLABLE BOOLEAN,
            COLUMN_DEFAULT TEXT,
            COMMENT TEXT,
            IS_PRIMARY_KEY BOOLEAN,
            IS_FOREIGN_KEY BOOLEAN,
            COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        cursor.execute("GRANT ALL ON TABLE CATALOG_COLUMNS TO ROLE PUBLIC")
        
        cursor.execute("""
        CREATE OR REPLACE TABLE CATALOG_CONNECTIONS (
            CONNECTION_ID VARCHAR(255) PRIMARY KEY,
            ACCOUNT VARCHAR(255) NOT NULL,
            USERNAME VARCHAR(255) NOT NULL,
            WAREHOUSE VARCHAR(255),
            ROLE VARCHAR(255),
            DATABASE_NAME VARCHAR(255),
            SCHEMA_NAME VARCHAR(255),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            LAST_USED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            STATUS VARCHAR(50) DEFAULT 'ACTIVE'
        )
        """)
        
        cursor.execute("GRANT ALL ON TABLE CATALOG_CONNECTIONS TO ROLE PUBLIC")
        
        # Commit all changes
        conn.commit()
        
        # Verify tables were created
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"Tables created: {[t[1] for t in tables if t]}")
        
        # Close connection
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"ERROR in force_create_catalog_tables: {str(e)}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        return False 