"""
Direct script to set up the Snowflake catalog tables.

This script creates the necessary tables in Snowflake for storing metadata.
It uses direct SQL commands and minimal error handling for maximum compatibility.
"""
import snowflake.connector
import os
import sys
import time

def setup_snowflake_catalog(account, username, password, warehouse=None, role='ACCOUNTADMIN'):
    """
    Direct function to set up the Snowflake catalog tables.

    Args:
        account: Snowflake account name
        username: Snowflake username
        password: Snowflake password
        warehouse: Snowflake warehouse (optional)
        role: Snowflake role (optional)
    """
    print(f"Connecting to Snowflake account: {account} as {username}")
    
    # Create connection
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role
    )
    
    # Create cursor
    cursor = conn.cursor()
    
    print("Connected successfully!")
    cursor.execute("SELECT CURRENT_ROLE()")
    role_result = cursor.fetchone()
    if role_result:
        print("Current role:", role_result[0])
    else:
        print("Could not determine current role")
    
    # Execute setup script
    print("\n--- Setting up SNOWFLAKE_CATALOG database ---\n")
    
    commands = [
        # Create database and schema
        "CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG;",
        "USE DATABASE SNOWFLAKE_CATALOG;",
        "CREATE SCHEMA IF NOT EXISTS PUBLIC;",
        "USE SCHEMA PUBLIC;",
        
        # Create CATALOG_DATABASES table
        """
        CREATE OR REPLACE TABLE CATALOG_DATABASES (
            DATABASE_ID VARCHAR(255) PRIMARY KEY,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            DATABASE_OWNER VARCHAR(255),
            COMMENT TEXT,
            CREATED_AT TIMESTAMP_NTZ,
            LAST_ALTERED TIMESTAMP_NTZ,
            COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        
        # Create CATALOG_SCHEMAS table
        """
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
        );
        """,
        
        # Create CATALOG_TABLES table
        """
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
        );
        """,
        
        # Create CATALOG_COLUMNS table
        """
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
        );
        """,
        
        # Create CATALOG_CONNECTIONS table
        """
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
        );
        """
    ]
    
    # Execute each command and commit after each statement
    for i, cmd in enumerate(commands):
        print(f"Executing command {i+1}/{len(commands)}...")
        try:
            cursor.execute(cmd)
            conn.commit()
            print(f"Command {i+1} executed successfully")
        except Exception as e:
            print(f"Error executing command {i+1}: {str(e)}")
            print(f"Command was: {cmd}")
    
    # Verify tables were created
    print("\n--- Verifying tables ---\n")
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    if tables:
        print("Tables created successfully:")
        for table in tables:
            print(f" - {table[1]}")
            
        # Add explicit grants to ensure all users can access the tables
        print("\n--- Adding explicit grants ---\n")
        try:
            # Grant access to the database and schema
            cursor.execute("GRANT USAGE ON DATABASE SNOWFLAKE_CATALOG TO ROLE PUBLIC")
            cursor.execute("GRANT USAGE ON SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            cursor.execute("GRANT USAGE ON SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE SYSADMIN")
            
            # Grant access to all tables
            cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE SYSADMIN")
            
            # Grant access to future tables
            cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE SYSADMIN")
            
            # Grant specific access to each table
            for table in tables:
                table_name = table[1]
                cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SNOWFLAKE_CATALOG.PUBLIC.{table_name} TO ROLE PUBLIC")
                cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SNOWFLAKE_CATALOG.PUBLIC.{table_name} TO ROLE SYSADMIN")
            
            conn.commit()
            print("Grants added successfully")
        except Exception as e:
            print(f"Error adding grants: {str(e)}")
    else:
        print("No tables were created!")
    
    # Close connection
    cursor.close()
    conn.close()
    
    print("\nSetup complete!")

def main():
    """Run the setup script with command line arguments."""
    if len(sys.argv) < 4:
        print("Usage: python setup_catalog.py <account> <username> <password> [warehouse] [role]")
        sys.exit(1)
    
    account = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    warehouse = sys.argv[4] if len(sys.argv) > 4 else None
    role = sys.argv[5] if len(sys.argv) > 5 else None
    
    setup_snowflake_catalog(account, username, password, warehouse, role)

if __name__ == "__main__":
    main() 