"""
Python-based approach to set up the Snowflake catalog.

This module uses the Snowflake Python connector to create the catalog tables
without relying heavily on direct SQL commands. This approach may be more
compatible with different Snowflake environments.
"""
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from typing import Dict, Any, List, Optional
import uuid

class SnowflakeCatalogCreator:
    """Class for creating and setting up Snowflake catalog using Python API."""
    
    def __init__(self, account: str, username: str, password: str, warehouse: Optional[str] = None, role: Optional[str] = None):
        """
        Initialize with Snowflake connection parameters.
        
        Args:
            account: Snowflake account identifier
            username: Snowflake username
            password: Snowflake password
            warehouse: Snowflake warehouse (optional)
            role: Snowflake role (optional)
        """
        self.account = account
        self.username = username
        self.password = password
        self.warehouse = warehouse
        self.role = role
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Establish connection to Snowflake.
        
        Returns:
            bool: True if connection succeeded, False otherwise
        """
        try:
            print(f"Connecting to Snowflake account: {self.account}")
            
            # Create connection
            self.connection = snowflake.connector.connect(
                user=self.username,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                role=self.role,
                client_session_keep_alive=True
            )
            
            # Create cursor
            self.cursor = self.connection.cursor()
            
            print("Connected successfully to Snowflake!")
            self._show_current_role()
            return True
            
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            return False
    
    def _show_current_role(self) -> None:
        """Display the current role."""
        if not self.cursor:
            return
            
        try:
            self.cursor.execute("SELECT CURRENT_ROLE()")
            role_result = self.cursor.fetchone()
            if role_result:
                print(f"Current role: {role_result[0]}")
            else:
                print("Could not determine current role")
        except Exception as e:
            print(f"Error determining current role: {str(e)}")
    
    def create_database(self) -> bool:
        """
        Create the SNOWFLAKE_CATALOG database.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.cursor or not self.connection:
            print("No active connection")
            return False
            
        try:
            print("Creating SNOWFLAKE_CATALOG database...")
            self.cursor.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
            self.cursor.execute("USE DATABASE SNOWFLAKE_CATALOG")
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
            self.cursor.execute("USE SCHEMA PUBLIC")
            self.connection.commit()
            print("Database and schema created successfully")
            return True
        except Exception as e:
            print(f"Error creating database: {str(e)}")
            return False
    
    def create_tables(self) -> bool:
        """
        Create all necessary catalog tables.
        
        Returns:
            bool: True if all tables were created successfully, False otherwise
        """
        if not self.cursor or not self.connection:
            print("No active connection")
            return False
        
        # Define table structures
        tables = {
            "CATALOG_DATABASES": [
                ("DATABASE_ID", "VARCHAR(255) PRIMARY KEY"),
                ("DATABASE_NAME", "VARCHAR(255) NOT NULL"),
                ("DATABASE_OWNER", "VARCHAR(255)"),
                ("COMMENT", "TEXT"),
                ("CREATED_AT", "TIMESTAMP_NTZ"),
                ("LAST_ALTERED", "TIMESTAMP_NTZ"),
                ("COLLECTED_AT", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
            ],
            "CATALOG_SCHEMAS": [
                ("SCHEMA_ID", "VARCHAR(255) PRIMARY KEY"),
                ("SCHEMA_NAME", "VARCHAR(255) NOT NULL"),
                ("DATABASE_ID", "VARCHAR(255) NOT NULL"),
                ("DATABASE_NAME", "VARCHAR(255) NOT NULL"),
                ("SCHEMA_OWNER", "VARCHAR(255)"),
                ("COMMENT", "TEXT"),
                ("CREATED_AT", "TIMESTAMP_NTZ"),
                ("LAST_ALTERED", "TIMESTAMP_NTZ"),
                ("COLLECTED_AT", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
            ],
            "CATALOG_TABLES": [
                ("TABLE_ID", "VARCHAR(255) PRIMARY KEY"),
                ("TABLE_NAME", "VARCHAR(255) NOT NULL"),
                ("SCHEMA_ID", "VARCHAR(255) NOT NULL"),
                ("SCHEMA_NAME", "VARCHAR(255) NOT NULL"),
                ("DATABASE_ID", "VARCHAR(255) NOT NULL"),
                ("DATABASE_NAME", "VARCHAR(255) NOT NULL"),
                ("TABLE_TYPE", "VARCHAR(50)"),
                ("TABLE_OWNER", "VARCHAR(255)"),
                ("COMMENT", "TEXT"),
                ("ROW_COUNT", "NUMBER"),
                ("BYTES", "NUMBER"),
                ("CREATED_AT", "TIMESTAMP_NTZ"),
                ("LAST_ALTERED", "TIMESTAMP_NTZ"),
                ("COLLECTED_AT", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
            ],
            "CATALOG_COLUMNS": [
                ("COLUMN_ID", "VARCHAR(255) PRIMARY KEY"),
                ("COLUMN_NAME", "VARCHAR(255) NOT NULL"),
                ("TABLE_ID", "VARCHAR(255) NOT NULL"),
                ("TABLE_NAME", "VARCHAR(255) NOT NULL"),
                ("SCHEMA_ID", "VARCHAR(255) NOT NULL"),
                ("SCHEMA_NAME", "VARCHAR(255) NOT NULL"),
                ("DATABASE_ID", "VARCHAR(255) NOT NULL"),
                ("DATABASE_NAME", "VARCHAR(255) NOT NULL"),
                ("ORDINAL_POSITION", "NUMBER"),
                ("DATA_TYPE", "VARCHAR(255)"),
                ("CHARACTER_MAXIMUM_LENGTH", "NUMBER"),
                ("NUMERIC_PRECISION", "NUMBER"),
                ("NUMERIC_SCALE", "NUMBER"),
                ("IS_NULLABLE", "BOOLEAN"),
                ("COLUMN_DEFAULT", "TEXT"),
                ("COMMENT", "TEXT"),
                ("IS_PRIMARY_KEY", "BOOLEAN"),
                ("IS_FOREIGN_KEY", "BOOLEAN"),
                ("COLLECTED_AT", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
            ],
            "CATALOG_CONNECTIONS": [
                ("CONNECTION_ID", "VARCHAR(255) PRIMARY KEY"),
                ("ACCOUNT", "VARCHAR(255) NOT NULL"),
                ("USERNAME", "VARCHAR(255) NOT NULL"),
                ("WAREHOUSE", "VARCHAR(255)"),
                ("ROLE", "VARCHAR(255)"),
                ("DATABASE_NAME", "VARCHAR(255)"),
                ("SCHEMA_NAME", "VARCHAR(255)"),
                ("CREATED_AT", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
                ("LAST_USED", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
                ("STATUS", "VARCHAR(50) DEFAULT 'ACTIVE'")
            ]
        }
        
        success = True
        for table_name, columns in tables.items():
            try:
                print(f"Creating table {table_name}...")
                
                # Generate CREATE TABLE statement
                column_definitions = ", ".join([f"{name} {data_type}" for name, data_type in columns])
                create_stmt = f"CREATE OR REPLACE TABLE {table_name} ({column_definitions})"
                
                # Execute create statement
                self.cursor.execute(create_stmt)
                self.connection.commit()
                
                print(f"Table {table_name} created successfully")
                
                # Insert a test record to verify it works
                if table_name == "CATALOG_DATABASES":
                    self._insert_test_record(table_name)
                
            except Exception as e:
                print(f"Error creating table {table_name}: {str(e)}")
                success = False
        
        return success
    
    def _insert_test_record(self, table_name: str) -> None:
        """Insert a test record to verify the table works."""
        if not self.cursor or not self.connection:
            print("No active connection")
            return
            
        try:
            print(f"Inserting test record into {table_name}...")
            
            if table_name == "CATALOG_DATABASES":
                self.cursor.execute(
                    "INSERT INTO CATALOG_DATABASES (DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, COMMENT) "
                    "VALUES (%s, %s, %s, %s)",
                    (str(uuid.uuid4()), "TEST_DATABASE", "TEST_OWNER", "Test record")
                )
            
            self.connection.commit()
            print("Test record inserted successfully")
            
            # Verify by selecting it back
            self.cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            result = self.cursor.fetchone()
            if result:
                print(f"Successfully retrieved test record: {result}")
            else:
                print("No test record found")
                
        except Exception as e:
            print(f"Error inserting test record: {str(e)}")
    
    def grant_permissions(self) -> bool:
        """
        Grant permissions on all catalog objects.
        
        Returns:
            bool: True if permissions were granted successfully, False otherwise
        """
        if not self.cursor or not self.connection:
            print("No active connection")
            return False
            
        try:
            print("Granting permissions...")
            
            # Grant usage on database and schema
            self.cursor.execute("GRANT USAGE ON DATABASE SNOWFLAKE_CATALOG TO ROLE PUBLIC")
            self.cursor.execute("GRANT USAGE ON SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            
            # Grant all privileges on tables
            self.cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            
            # Grant privileges on future tables
            self.cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA SNOWFLAKE_CATALOG.PUBLIC TO ROLE PUBLIC")
            
            self.connection.commit()
            print("Permissions granted successfully")
            return True
            
        except Exception as e:
            print(f"Error granting permissions: {str(e)}")
            return False
    
    def verify_setup(self) -> bool:
        """
        Verify that the setup completed successfully.
        
        Returns:
            bool: True if verification passed, False otherwise
        """
        if not self.cursor or not self.connection:
            print("No active connection")
            return False
            
        try:
            print("Verifying setup...")
            
            # Check if tables exist
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            
            if not tables:
                print("No tables found in the schema")
                return False
            
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f" - {table[1]}")
            
            # Try to query each table
            for table in tables:
                table_name = table[1]
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = self.cursor.fetchone()
                    if result is not None:
                        count = result[0]
                        print(f"Table {table_name} has {count} rows")
                    else:
                        print(f"Table {table_name} has no data (result was None)")
                except Exception as e:
                    print(f"Error querying table {table_name}: {str(e)}")
                    return False
            
            print("All tables verified successfully")
            return True
            
        except Exception as e:
            print(f"Error verifying setup: {str(e)}")
            return False
    
    def _initialize_sample_data(self) -> None:
        """Initialize sample data for all catalog tables."""
        if not self.cursor or not self.connection:
            print("No active connection")
            return
            
        try:
            print("Initializing sample data for catalog tables...")
            
            # Add a sample database record
            database_id = str(uuid.uuid4())
            self.cursor.execute(
                "INSERT INTO CATALOG_DATABASES (DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, COMMENT) "
                "VALUES (%s, %s, %s, %s)",
                (database_id, "SAMPLE_DB", "ACCOUNTADMIN", "A sample database with customer data")
            )
            print("Added sample database record")
            
            # Add a sample schema record
            schema_id = str(uuid.uuid4())
            self.cursor.execute(
                "INSERT INTO CATALOG_SCHEMAS (SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, SCHEMA_OWNER, COMMENT) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (schema_id, "PUBLIC", database_id, "SAMPLE_DB", "ACCOUNTADMIN", "Default public schema")
            )
            print("Added sample schema record")
            
            # Add a sample table record
            table_id = str(uuid.uuid4())
            self.cursor.execute(
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
                self.cursor.execute(
                    "INSERT INTO CATALOG_COLUMNS (COLUMN_ID, COLUMN_NAME, TABLE_ID, TABLE_NAME, "
                    "SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, ORDINAL_POSITION, "
                    "DATA_TYPE, IS_NULLABLE, COMMENT, IS_PRIMARY_KEY, IS_FOREIGN_KEY) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (column_id, col_name, table_id, "CUSTOMERS", schema_id, "PUBLIC", 
                     database_id, "SAMPLE_DB", idx + 1, data_type, not is_primary, 
                     comment, is_primary, is_foreign)
                )
            print("Added sample column records")
            
            # Add a sample connection record
            connection_id = str(uuid.uuid4())
            self.cursor.execute(
                "INSERT INTO CATALOG_CONNECTIONS (CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, DATABASE_NAME, SCHEMA_NAME, ROLE) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (connection_id, "myaccount", "myuser", "compute_wh", "SAMPLE_DB", "PUBLIC", "ACCOUNTADMIN")
            )
            print("Added sample connection record")
            
            # Commit all changes
            self.connection.commit()
            print("Sample data initialization completed successfully")
            
        except Exception as e:
            print(f"Error initializing sample data: {str(e)}")
            # Don't raise the exception, as this is not critical to setup
    
    def setup(self) -> bool:
        """
        Run the complete setup process.
        
        Returns:
            bool: True if setup completed successfully, False otherwise
        """
        try:
            if not self.connect():
                return False
                
            if not self.create_database():
                return False
                
            # Try to create tables using our method first
            tables_created = self.create_tables()
            
            if not tables_created:
                print("Failed to create all tables using standard method, trying alternative method...")
                
                # Use the helper function to create all tables with proper error handling
                try:
                    from .snowflake_metadata_helper import initialize_snowflake_catalog
                    print("Using initialize_snowflake_catalog to create tables...")
                    if self.connection is not None:
                        initialize_snowflake_catalog(self.connection)
                        print("Tables created successfully using alternative method")
                        tables_created = True
                    else:
                        print("Cannot create tables: connection is None")
                except Exception as e:
                    print(f"Error using alternative method: {str(e)}")
                    
            if not tables_created:
                return False
                
            # Initialize sample data after tables are created
            self._initialize_sample_data()
                
            if not self.grant_permissions():
                return False
                
            if not self.verify_setup():
                return False
            
            print("\nSetup completed successfully!")
            return True
            
        except Exception as e:
            print(f"Error during setup: {str(e)}")
            return False
            
        finally:
            if self.connection:
                self.connection.close()
                print("Connection closed")

def setup_snowflake_catalog_python(account: str, username: str, password: str, warehouse: Optional[str] = None, role: Optional[str] = None) -> bool:
    """
    Python function to set up the Snowflake catalog.
    
    Args:
        account: Snowflake account identifier
        username: Snowflake username
        password: Snowflake password
        warehouse: Snowflake warehouse (optional)
        role: Snowflake role (optional)
        
    Returns:
        bool: True if setup completed successfully, False otherwise
    """
    creator = SnowflakeCatalogCreator(account, username, password, warehouse, role)
    return creator.setup()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python python_catalog_setup.py <account> <username> <password> [warehouse] [role]")
        sys.exit(1)
    
    account = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    warehouse = sys.argv[4] if len(sys.argv) > 4 else None
    role = sys.argv[5] if len(sys.argv) > 5 else None
    
    success = setup_snowflake_catalog_python(account, username, password, warehouse, role)
    
    if success:
        print("Catalog setup completed successfully!")
        sys.exit(0)
    else:
        print("Catalog setup failed!")
        sys.exit(1) 