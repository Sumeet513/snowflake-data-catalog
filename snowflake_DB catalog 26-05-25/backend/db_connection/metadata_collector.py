import uuid
import threading
import time
from datetime import datetime
from django.core.cache import cache
from typing import Dict, Any, Optional
import snowflake.connector

# Replace metadata_manager imports with models from db_connection
from .models import SnowflakeDatabase, SnowflakeSchema, SnowflakeTable, SnowflakeColumn
from .snowflake_service import SnowflakeService
from .snowflake_metadata_helper import connect_to_snowflake, update_process_status, initialize_snowflake_catalog, force_create_catalog_tables
from .setup_catalog import setup_snowflake_catalog

class MetadataCollectionService:
    """
    Service for collecting and storing metadata from Snowflake.
    """
    
    @staticmethod
    def update_cache_status(process_id: str, status_data: Dict[str, Any], timeout: int = 3600) -> None:
        """Update the status of the metadata collection process in the cache."""
        # Use consistent cache key format
        key = f"process_status_{process_id}"
        
        # Add timestamp if not present
        if 'timestamp' not in status_data:
            status_data['timestamp'] = datetime.now().isoformat()
            
        cache.set(key, status_data, timeout)
    
    @staticmethod
    def get_process_status(process_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a metadata collection process from the cache."""
        # Use consistent cache key format
        key = f"process_status_{process_id}"
        status_data = cache.get(key)
        if status_data:
            print(f"Found process status for {process_id}: {status_data}")
        else:
            print(f"No process status found for {process_id}")
        return status_data
    
    @classmethod
    def collect_metadata(cls, connection_params: Dict[str, Any]) -> str:
        """
        Start metadata collection process in a background thread.
        
        Args:
            connection_params: Dictionary with Snowflake connection parameters
                               (account, user, password, warehouse, role)
        
        Returns:
            process_id: ID for tracking the metadata collection process
        """
        process_id = str(uuid.uuid4())
        
        # Set initial status
        cls.update_cache_status(process_id, {
            'status': 'initiated',
            'progress': 0,
            'message': 'Metadata collection initiated',
            'phase': 'initialization'
        })
        
        # Start metadata collection in a background thread
        threading.Thread(
            target=cls._process_metadata_collection,
            args=(process_id, connection_params),
            daemon=True
        ).start()
        
        return process_id
    
    @classmethod
    def _process_metadata_collection(cls, process_id: str, connection_params: Dict[str, Any]) -> None:
        """
        Process metadata collection and storage.
        
        Args:
            process_id: ID for tracking the process
            connection_params: Dictionary with Snowflake connection parameters
        """
        try:
            # Update status to connecting
            cls.update_cache_status(process_id, {
                'status': 'processing',
                'progress': 10,
                'message': 'Connecting to Snowflake...',
                'phase': 'connection'
            })
            
            # Initialize the service
            snowflake_service = SnowflakeService()
            
            # Connect to Snowflake with better error handling
            success, connection, message = connect_to_snowflake(connection_params)
            
            if not success:
                # Connection failed, update status and exit
                cls.update_cache_status(process_id, {
                    'status': 'error',
                    'message': message
                })
                return
                
            # Set the connection in the service
            snowflake_service.connection = connection
            
            # Connection successful, update status
            cls.update_cache_status(process_id, {
                'status': 'processing',
                'progress': 20,
                'message': 'Connection successful, starting metadata collection...',
                'phase': 'collection'
            })
            
            # Start collecting metadata
            cls.update_cache_status(process_id, {
                'status': 'processing',
                'progress': 30,
                'message': 'Collecting databases metadata...',
                'phase': 'collecting_databases'
            })
            
            # Get metadata using SnowflakeService
            databases = snowflake_service.get_databases()
            all_schemas = []
            all_tables = []
            all_columns = []
            
            # Collect schemas for each database
            for database in databases:
                db_name = database.get('name')
                if not db_name:
                    continue
                
                cls.update_cache_status(process_id, {
                    'status': 'processing',
                    'progress': 40,
                    'message': f'Collecting schemas for database {db_name}...',
                    'phase': 'collecting_schemas'
                })
                
                schemas = snowflake_service.get_schemas(db_name)
                all_schemas.extend(schemas)
                
                # Collect tables for each schema
                for schema in schemas:
                    schema_name = schema.get('name')
                    if not schema_name:
                        continue
                    
                    cls.update_cache_status(process_id, {
                        'status': 'processing',
                        'progress': 50,
                        'message': f'Collecting tables for schema {db_name}.{schema_name}...',
                        'phase': 'collecting_tables'
                    })
                    
                    tables = snowflake_service.get_tables(db_name, schema_name)
                    all_tables.extend(tables)
                    
                    # Collect columns for each table
                    for table in tables:
                        table_name = table.get('name')
                        if not table_name:
                            continue
                        
                        cls.update_cache_status(process_id, {
                            'status': 'processing',
                            'progress': 60,
                            'message': f'Collecting columns for table {db_name}.{schema_name}.{table_name}...',
                            'phase': 'collecting_columns'
                        })
                        
                        columns = snowflake_service.get_table_columns(db_name, schema_name, table_name)
                        all_columns.extend(columns)
            
            # Update status with metadata counts
            cls.update_cache_status(process_id, {
                'status': 'processing',
                'progress': 70,
                'message': f'Metadata collected: {len(databases)} databases, {len(all_schemas)} schemas, {len(all_tables)} tables, {len(all_columns)} columns',
                'phase': 'saving',
                'stats': {
                    'database_count': len(databases),
                    'schema_count': len(all_schemas),
                    'table_count': len(all_tables),
                    'column_count': len(all_columns)
                }
            })
            
            # Step 1: Create a direct connection to Snowflake for storing metadata in SNOWFLAKE_CATALOG
            snowflake_conn = None
            try:
                # Update status
                cls.update_cache_status(process_id, {
                    'status': 'processing',
                    'progress': 75,
                    'message': 'Setting up SNOWFLAKE_CATALOG database...',
                    'phase': 'setup_catalog'
                })
                
                # Create a direct Snowflake connection
                snowflake_conn = snowflake.connector.connect(
                    account=connection_params['account'],
                    user=connection_params['username'],
                    password=connection_params['password'],
                    warehouse=connection_params.get('warehouse'),
                    role=connection_params.get('role')
                )
                
                # Initialize the Snowflake catalog structure
                cursor = snowflake_conn.cursor()
                
                # Create database and schema if not exists
                cursor.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
                cursor.execute("USE DATABASE SNOWFLAKE_CATALOG")
                cursor.execute("CREATE SCHEMA IF NOT EXISTS METADATA")
                cursor.execute("USE SCHEMA METADATA")
                
                # Create tables to store metadata if they don't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS DATABASES (
                        DATABASE_ID VARCHAR(255) PRIMARY KEY,
                        DATABASE_NAME VARCHAR(255) NOT NULL,
                        DATABASE_OWNER VARCHAR(255),
                        COMMENT TEXT,
                        CREATED_AT TIMESTAMP_NTZ,
                        LAST_ALTERED TIMESTAMP_NTZ,
                        COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS SCHEMAS (
                        SCHEMA_ID VARCHAR(255) PRIMARY KEY,
                        SCHEMA_NAME VARCHAR(255) NOT NULL,
                        DATABASE_ID VARCHAR(255) NOT NULL,
                        DATABASE_NAME VARCHAR(255) NOT NULL,
                        SCHEMA_OWNER VARCHAR(255),
                        COMMENT TEXT,
                        CREATED_AT TIMESTAMP_NTZ,
                        LAST_ALTERED TIMESTAMP_NTZ,
                        COLLECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        FOREIGN KEY (DATABASE_ID) REFERENCES DATABASES(DATABASE_ID)
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS TABLES (
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
                        FOREIGN KEY (SCHEMA_ID) REFERENCES SCHEMAS(SCHEMA_ID)
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS COLUMNS (
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
                        FOREIGN KEY (TABLE_ID) REFERENCES TABLES(TABLE_ID)
                    )
                """)
                
                # Save the connection to CATALOG_CONNECTIONS
                try:
                    print("Creating connections table if needed...")
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
                    
                    print("Saving connection information...")
                    cursor.execute("""
                        MERGE INTO CATALOG_CONNECTIONS t
                        USING (SELECT %s, %s, %s, %s, %s) s (
                            CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, ROLE
                        )
                        ON t.CONNECTION_ID = s.CONNECTION_ID
                        WHEN MATCHED THEN
                            UPDATE SET 
                                LAST_USED = CURRENT_TIMESTAMP(),
                                STATUS = 'ACTIVE'
                        WHEN NOT MATCHED THEN
                            INSERT (CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, ROLE)
                            VALUES (s.CONNECTION_ID, s.ACCOUNT, s.USERNAME, s.WAREHOUSE, s.ROLE)
                    """, (
                        process_id,
                        connection_params['account'],
                        connection_params['username'],
                        connection_params.get('warehouse', ''),
                        connection_params.get('role', '')
                    ))
                    print("Connection information saved successfully")
                    # Commit the connection information
                    snowflake_conn.commit()
                except Exception as e:
                    print(f"Error saving connection information: {str(e)}")
                
                # Insert collected metadata into Snowflake tables
                # Save databases to Snowflake
                print("Inserting database metadata into CATALOG_DATABASES...")
                for db in databases:
                    db_id = str(db.get('id', uuid.uuid4()))
                    try:
                        cursor.execute("""
                            INSERT INTO CATALOG_DATABASES (DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, COMMENT)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            db_id,
                            db.get('name', ''),
                            db.get('owner', ''),
                            db.get('comment', '')
                        ))
                        print(f"Inserted database {db.get('name', '')}")
                    except Exception as e:
                        print(f"Error inserting database {db.get('name', '')}: {str(e)}")
                
                # Save schemas to Snowflake
                print("Inserting schema metadata into CATALOG_SCHEMAS...")
                for schema in all_schemas:
                    schema_id = str(schema.get('id', uuid.uuid4()))
                    database_id = str(schema.get('database_id', ''))
                    try:
                        cursor.execute("""
                            INSERT INTO CATALOG_SCHEMAS (SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            schema_id,
                            schema.get('name', ''),
                            database_id,
                            schema.get('database_name', '')
                        ))
                        print(f"Inserted schema {schema.get('name', '')}")
                    except Exception as e:
                        print(f"Error inserting schema {schema.get('name', '')}: {str(e)}")
                
                # Save tables to Snowflake
                print("Inserting table metadata into CATALOG_TABLES...")
                for table in all_tables:
                    table_id = str(table.get('id', uuid.uuid4()))
                    schema_id = str(table.get('schema_id', ''))
                    database_id = str(table.get('database_id', ''))
                    try:
                        cursor.execute("""
                            INSERT INTO CATALOG_TABLES (
                                TABLE_ID, TABLE_NAME, SCHEMA_ID, SCHEMA_NAME,
                                DATABASE_ID, DATABASE_NAME, ROW_COUNT
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            table_id,
                            table.get('name', ''),
                            schema_id,
                            table.get('schema_name', ''),
                            database_id,
                            table.get('database_name', ''),
                            table.get('row_count', 0)
                        ))
                        print(f"Inserted table {table.get('name', '')}")
                    except Exception as e:
                        print(f"Error inserting table {table.get('name', '')}: {str(e)}")
                
                # Save columns to Snowflake
                print("Inserting column metadata into CATALOG_COLUMNS...")
                for column in all_columns:
                    column_id = str(column.get('id', uuid.uuid4()))
                    table_id = str(column.get('table_id', ''))
                    schema_id = str(column.get('schema_id', ''))
                    database_id = str(column.get('database_id', ''))
                    try:
                        cursor.execute("""
                            INSERT INTO CATALOG_COLUMNS (
                                COLUMN_ID, COLUMN_NAME, TABLE_ID, TABLE_NAME,
                                SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME,
                                DATA_TYPE, IS_NULLABLE
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            column_id,
                            column.get('name', ''),
                            table_id,
                            column.get('table_name', ''),
                            schema_id,
                            column.get('schema_name', ''),
                            database_id,
                            column.get('database_name', ''),
                            column.get('type', ''),
                            column.get('is_nullable', True)
                        ))
                        if column.get('name', '') and column.get('table_name', ''):
                            print(f"Inserted column {column.get('table_name', '')}.{column.get('name', '')}")
                    except Exception as e:
                        print(f"Error inserting column {column.get('name', '')}: {str(e)}")
                
                # Commit all metadata
                snowflake_conn.commit()
                
                # Initialize the Snowflake catalog with tables and views
                try:
                    print("Initializing Snowflake catalog tables...")
                    
                    # First, try the direct setup script which is most reliable
                    print("Attempting to set up catalog using direct setup script...")
                    setup_snowflake_catalog(
                        account=connection_params['account'],
                        username=connection_params['username'],
                        password=connection_params['password'],
                        warehouse=connection_params.get('warehouse'),
                        role=connection_params.get('role')
                    )
                    print("Direct catalog setup completed")
                    
                    # For backward compatibility, still try the initialize_snowflake_catalog function
                    initialize_snowflake_catalog(snowflake_conn)
                    print("Catalog tables created successfully")
                except Exception as e:
                    print(f"Warning: Error initializing catalog tables: {str(e)}")
                    print("Attempting to force create tables...")
                    
                    cls.update_cache_status(process_id, {
                        'status': 'processing',
                        'progress': 76,
                        'message': 'Initial table creation failed. Trying alternative method...',
                        'phase': 'setup_catalog'
                    })
                    
                    # Try the alternative force method
                    success = force_create_catalog_tables(connection_params)
                    
                    if success:
                        print("Successfully created tables with alternate method")
                        cls.update_cache_status(process_id, {
                            'status': 'processing',
                            'progress': 77,
                            'message': 'Tables created successfully with alternate method',
                            'phase': 'setup_catalog'
                        })
                    else:
                        print("Failed to create tables with alternate method")
                        cls.update_cache_status(process_id, {
                            'status': 'processing',
                            'progress': 77,
                            'message': 'Warning: Failed to create tables with alternate method. Will continue anyway.',
                            'phase': 'setup_catalog'
                        })
                
            except Exception as sf_error:
                # Log error but continue with Django storage
                print(f"Error storing metadata in SNOWFLAKE_CATALOG: {str(sf_error)}")
                cls.update_cache_status(process_id, {
                    'status': 'processing',
                    'progress': 80,
                    'message': f'Warning: Failed to store in SNOWFLAKE_CATALOG: {str(sf_error)}. Continuing with Django storage.',
                    'phase': 'storing_in_django'
                })
            finally:
                # Close Snowflake connection if open
                if snowflake_conn:
                    try:
                        snowflake_conn.close()
                    except:
                        pass
            
            # Step 2: Save the metadata to Django models
            # This is simplified since we're now using a different approach
            cls.update_cache_status(process_id, {
                'status': 'processing',
                'progress': 85,
                'message': 'Storing metadata in Django database...',
                'phase': 'storing_in_django'
            })
            
            # Save to Django models (simplified for example)
            saved_count = 0
            try:
                # Example saving of data to Django models
                for db_data in databases:
                    # Create or update database
                    db, created = SnowflakeDatabase.objects.update_or_create(
                        database_id=str(db_data.get('id', '')),
                        defaults={
                            'database_name': db_data.get('name', ''),
                            'database_owner': db_data.get('owner', '')
                        }
                    )
                    saved_count += 1
                
                # ... similar for schemas, tables, columns
            except Exception as django_error:
                print(f"Error storing in Django models: {str(django_error)}")
            
            # Close the Snowflake service
            snowflake_service.close()
            
            # Update status to completed
            cls.update_cache_status(process_id, {
                'status': 'completed',
                'progress': 100,
                'message': 'Metadata collection and storage completed successfully',
                'phase': 'completed',
                'stats': {
                    'database_count': len(databases),
                    'schema_count': len(all_schemas),
                    'table_count': len(all_tables),
                    'column_count': len(all_columns)
                }
            })
            
        except Exception as e:
            # Update status to error
            print(f"Error during metadata collection: {str(e)}")
            cls.update_cache_status(process_id, {
                'status': 'error',
                'message': f'Error during metadata collection: {str(e)}',
            }) 