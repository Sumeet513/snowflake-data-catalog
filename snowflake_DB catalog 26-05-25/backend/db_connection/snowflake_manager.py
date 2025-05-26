import snowflake.connector
from contextlib import contextmanager
from datetime import datetime
import requests
import json
import time
from django.core.cache import cache
import uuid
from django.db import models

from .snowflake_connection import SnowflakeConnection
from .snowflake_metadata import SnowflakeMetadata
from .snowflake_ai import SnowflakeAI
from .external_storage import DatabaseStorage
from .models import SnowflakeConnection as SnowflakeConnectionModel

class SnowflakeManager:
    """
    Main Snowflake Manager class that coordinates the different components:
    - Connection management
    - Metadata collection and management
    - AI-powered enhancements
    - External database storage
    """
    def __init__(self, ai_api_key=None, ai_provider="openai"):
        self.connection = SnowflakeConnection()
        self.metadata = SnowflakeMetadata()
        self.ai = SnowflakeAI(ai_api_key, ai_provider)
        self.storage = DatabaseStorage()
    
    # Connection management methods
    @contextmanager
    def get_connection(self, connection_params, save_details=True):
        """Pass-through to the connection manager"""
        with self.connection.get_connection(connection_params, save_details) as conn:
            yield conn
    
    @contextmanager
    def get_optimized_connection(self, connection_params):
        """Pass-through to the connection manager"""
        with self.connection.get_optimized_connection(connection_params) as conn:
            yield conn
    
    def execute_query(self, connection_params, query, params=None):
        """Pass-through to the connection manager"""
        return self.connection.execute_query(connection_params, query, params)
    
    # Metadata methods
    def collect_snowflake_metadata(self, connection_params, timeout=3600):
        """Pass-through to the metadata manager"""
        # Add process ID to connection params for tracking if not already there
        if 'process_id' not in connection_params:
            connection_params['process_id'] = str(uuid.uuid4())
            
        # Call the metadata collection function with the timeout
        results = self.metadata.collect_snowflake_metadata(connection_params, timeout)
        
        # Save metadata to external storage instead of Snowflake internal tables
        # This happens after metadata collection to avoid impacting performance
        if results['status'] == 'success':
            try:
                # Save connection details
                self.storage.save_connection(connection_params)
                
                # Get the schema used for metadata
                metadata_schema = connection_params.get('metadata_schema', 'PUBLIC')
                
                # Collect databases, schemas, tables data from the results
                with self.get_connection(connection_params) as conn:
                    cursor = conn.cursor()
                    
                    # Get databases
                    cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_DATABASES")
                    columns = [desc[0] for desc in cursor.description]
                    databases_data = []
                    for row in cursor.fetchall():
                        db_data = dict(zip(columns, row))
                        databases_data.append(db_data)
                        self.storage.save_metadata('database', db_data)
                    
                    # Get schemas
                    cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_SCHEMAS")
                    columns = [desc[0] for desc in cursor.description]
                    schemas_data = []
                    for row in cursor.fetchall():
                        schema_data = dict(zip(columns, row))
                        schemas_data.append(schema_data)
                        self.storage.save_metadata('schema', schema_data)
                    
                    # Get tables
                    cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_TABLES")
                    columns = [desc[0] for desc in cursor.description]
                    tables_data = []
                    for row in cursor.fetchall():
                        table_data = dict(zip(columns, row))
                        tables_data.append(table_data)
                        self.storage.save_metadata('table', table_data)
                    
                    # Get columns
                    cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_COLUMNS")
                    columns = [desc[0] for desc in cursor.description]
                    columns_data = []
                    for row in cursor.fetchall():
                        column_data = dict(zip(columns, row))
                        columns_data.append(column_data)
                        self.storage.save_metadata('column', column_data)
                
                # Update or create Django models
                try:
                    from .models import SnowflakeDatabase, SnowflakeSchema, SnowflakeTable, SnowflakeColumn
                    
                    # Create databases
                    for db_data in databases_data:
                        db_obj, created = SnowflakeDatabase.objects.update_or_create(
                            database_id=db_data['DATABASE_ID'],
                            defaults={
                                'database_name': db_data['DATABASE_NAME'],
                                'database_owner': db_data.get('DATABASE_OWNER'),
                                'database_description': db_data.get('DATABASE_DESCRIPTION'),
                                # Add other fields as needed
                            }
                        )
                    
                    # Create schemas
                    for schema_data in schemas_data:
                        # Find parent database
                        try:
                            database = SnowflakeDatabase.objects.get(database_id=schema_data['DATABASE_ID'])
                            schema_obj, created = SnowflakeSchema.objects.update_or_create(
                                schema_id=schema_data['SCHEMA_ID'],
                                defaults={
                                    'database': database,
                                    'schema_name': schema_data['SCHEMA_NAME'],
                                    'schema_owner': schema_data.get('SCHEMA_OWNER'),
                                    'schema_description': schema_data.get('SCHEMA_DESCRIPTION'),
                                    # Add other fields as needed
                                }
                            )
                        except SnowflakeDatabase.DoesNotExist:
                            print(f"Database not found for schema: {schema_data['SCHEMA_ID']}")
                    
                    # Create tables
                    for table_data in tables_data:
                        # Find parent schema
                        try:
                            schema = SnowflakeSchema.objects.get(schema_id=table_data['SCHEMA_ID'])
                            table_obj, created = SnowflakeTable.objects.update_or_create(
                                table_id=table_data['TABLE_ID'],
                                defaults={
                                    'schema': schema,
                                    'table_name': table_data['TABLE_NAME'],
                                    'table_type': table_data.get('TABLE_TYPE'),
                                    'table_owner': table_data.get('TABLE_OWNER'),
                                    'table_description': table_data.get('TABLE_DESCRIPTION'),
                                    'row_count': table_data.get('ROW_COUNT'),
                                    'byte_size': table_data.get('BYTE_SIZE'),
                                    # Add other fields as needed
                                }
                            )
                        except SnowflakeSchema.DoesNotExist:
                            print(f"Schema not found for table: {table_data['TABLE_ID']}")
                    
                    # Create columns - Add this missing part to populate column data
                    for column_data in columns_data:
                        # Find parent table
                        try:
                            table = SnowflakeTable.objects.get(table_id=column_data['TABLE_ID'])
                            column_obj, created = SnowflakeColumn.objects.update_or_create(
                                column_id=column_data['COLUMN_ID'],
                                defaults={
                                    'table': table,
                                    'column_name': column_data['COLUMN_NAME'],
                                    'ordinal_position': column_data.get('ORDINAL_POSITION'),
                                    'data_type': column_data.get('DATA_TYPE'),
                                    'character_maximum_length': column_data.get('CHARACTER_MAXIMUM_LENGTH'),
                                    'numeric_precision': column_data.get('NUMERIC_PRECISION'),
                                    'numeric_scale': column_data.get('NUMERIC_SCALE'),
                                    'is_nullable': column_data.get('IS_NULLABLE', True),
                                    'column_default': column_data.get('COLUMN_DEFAULT'),
                                    'column_description': column_data.get('COLUMN_DESCRIPTION'),
                                    'comment': column_data.get('COMMENT'),
                                    'is_primary_key': column_data.get('IS_PRIMARY_KEY', False),
                                    'is_foreign_key': column_data.get('IS_FOREIGN_KEY', False),
                                    'min_value': column_data.get('MIN_VALUE'),
                                    'max_value': column_data.get('MAX_VALUE'),
                                    'distinct_values': column_data.get('DISTINCT_VALUES'),
                                    'null_count': column_data.get('NULL_COUNT')
                                }
                            )
                        except SnowflakeTable.DoesNotExist:
                            print(f"Table not found for column: {column_data['COLUMN_ID']}")
                
                except Exception as e:
                    print(f"Error updating Django models: {str(e)}")
                
            except Exception as e:
                print(f"Error saving metadata to external storage: {str(e)}")
        
        return results
    
    def collect_database_metadata(self, connection_params, timeout=600):
        """Collect metadata for a single database only"""
        if 'database' not in connection_params or not connection_params['database']:
            return {
                'status': 'error',
                'message': 'No database specified for single database collection'
            }
            
        # Add process ID to connection params for tracking if not already there
        if 'process_id' not in connection_params:
            connection_params['process_id'] = str(uuid.uuid4())
            
        db_name = connection_params['database']
        metadata_schema = connection_params.get('metadata_schema', 'PUBLIC')
        
        # Get optimization parameters
        max_tables = connection_params.get('max_tables_per_schema', 500)  # Limit tables per schema
        max_schemas = connection_params.get('max_schemas_per_db', 100)    # Limit schemas per db
        collect_stats = connection_params.get('collect_statistics', False) # Skip statistics collection by default
        column_sample_pct = connection_params.get('column_sample_pct', 100) # Percentage of columns to sample
        
        try:
            with self.get_connection(connection_params) as conn:
                cur = conn.cursor()
                
                # Create metadata database and schema if they don't exist
                cur.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
                cur.execute("USE DATABASE SNOWFLAKE_CATALOG")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {metadata_schema}")
                cur.execute(f"USE SCHEMA {metadata_schema}")
                
                # Ensure metadata tables exist
                self.metadata.create_metadata_tables(cur)
                
                # Get schemas for this specific database
                cur.execute(f"SHOW SCHEMAS IN DATABASE {db_name}")
                schemas = cur.fetchall()
                
                # Apply schema limit 
                if len(schemas) > max_schemas:
                    print(f"Limiting schemas to {max_schemas} out of {len(schemas)} for database {db_name}")
                    schemas = schemas[:max_schemas]
                    
                schema_count = len(schemas)
                table_count = 0
                column_count = 0
                
                # Track progress
                total_schemas = len(schemas)
                schema_idx = 0
                
                # Store the database in the catalog 
                try:
                    # Get database details first
                    cur.execute(f"SHOW DATABASES LIKE '{db_name}'")
                    db_row = cur.fetchone()
                    
                    if db_row:
                        # Safe timestamp conversion
                        def safe_timestamp(value):
                            if value is None:
                                return None
                            try:
                                return value.isoformat() if hasattr(value, 'isoformat') else str(value)
                            except:
                                return str(value)
                                
                        # Insert or update database record
                        db_id = db_name
                        try:
                            cur.execute("""
                            INSERT INTO CATALOG_DATABASES (
                                DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, 
                                CREATE_DATE, LAST_ALTERED_DATE, COMMENT
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                db_id,
                                db_name,
                                db_row[5] if len(db_row) > 5 else None,  # Owner
                                safe_timestamp(db_row[6]) if len(db_row) > 6 else None,  # Created
                                safe_timestamp(db_row[7]) if len(db_row) > 7 else None,  # Modified
                                db_row[9] if len(db_row) > 9 else None,  # Comment
                            ))
                        except Exception as db_insert_error:
                            # If database already exists, update it
                            try:
                                cur.execute("""
                                UPDATE CATALOG_DATABASES
                                SET 
                                    DATABASE_OWNER = %s,
                                    CREATE_DATE = %s,
                                    LAST_ALTERED_DATE = %s,
                                    COMMENT = %s,
                                    COLLECTED_AT = CURRENT_TIMESTAMP()
                                WHERE DATABASE_ID = %s
                                """, (
                                    db_row[5] if len(db_row) > 5 else None,  # Owner
                                    safe_timestamp(db_row[6]) if len(db_row) > 6 else None,  # Created
                                    safe_timestamp(db_row[7]) if len(db_row) > 7 else None,  # Modified
                                    db_row[9] if len(db_row) > 9 else None,  # Comment
                                    db_id
                                ))
                            except Exception as update_error:
                                print(f"Error updating database {db_id}: {str(update_error)}")
                                
                    # Process each schema
                    for schema_row in schemas:
                        schema_idx += 1
                        schema_name = schema_row[1]  # Schema name is in the second column
                        
                        # Skip system schemas to improve performance
                        if schema_name in ['INFORMATION_SCHEMA', 'PUBLIC'] and schema_idx > 1:
                            continue
                        
                        try:
                            # Store schema in catalog
                            schema_id = f"{db_name}.{schema_name}"
                            try:
                                cur.execute("""
                                INSERT INTO CATALOG_SCHEMAS (
                                    SCHEMA_ID, DATABASE_ID, SCHEMA_NAME, SCHEMA_OWNER,
                                    CREATE_DATE, LAST_ALTERED_DATE, COMMENT
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    schema_id,
                                    db_name,
                                    schema_name,
                                    schema_row[5] if len(schema_row) > 5 else None,  # Owner
                                    safe_timestamp(schema_row[6]) if len(schema_row) > 6 else None,  # Created
                                    safe_timestamp(schema_row[7]) if len(schema_row) > 7 else None,  # Modified
                                    schema_row[9] if len(schema_row) > 9 else None,  # Comment
                                ))
                            except Exception as schema_insert_error:
                                # If schema already exists, update it
                                try:
                                    cur.execute("""
                                    UPDATE CATALOG_SCHEMAS
                                    SET 
                                        SCHEMA_OWNER = %s,
                                        CREATE_DATE = %s,
                                        LAST_ALTERED_DATE = %s,
                                        COMMENT = %s,
                                        COLLECTED_AT = CURRENT_TIMESTAMP()
                                    WHERE SCHEMA_ID = %s
                                    """, (
                                        schema_row[5] if len(schema_row) > 5 else None,  # Owner
                                        safe_timestamp(schema_row[6]) if len(schema_row) > 6 else None,  # Created
                                        safe_timestamp(schema_row[7]) if len(schema_row) > 7 else None,  # Modified
                                        schema_row[9] if len(schema_row) > 9 else None,  # Comment
                                        schema_id
                                    ))
                                except Exception as update_error:
                                    print(f"Error updating schema {schema_id}: {str(update_error)}")
                            
                            # Get tables in this schema
                            cur.execute(f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}")
                            tables = cur.fetchall()
                            
                            # Apply table limit if needed to improve performance
                            if len(tables) > max_tables:
                                print(f"Limiting tables to {max_tables} out of {len(tables)} for schema {schema_name}")
                                tables = tables[:max_tables]
                                
                            table_count += len(tables)
                            
                            # Commit after each schema's tables are processed
                            conn.commit()
                            
                            # Process each table and its columns
                            for table_row in tables:
                                table_name = table_row[1]  # Table name is in the second column
                                
                                try:
                                    # Store table metadata in catalog table
                                    table_id = f"{db_name}.{schema_name}.{table_name}"
                                    
                                    # Store table details
                                    try:
                                        # Get table details including row count if available
                                        cur.execute("""
                                        INSERT INTO CATALOG_TABLES (
                                            TABLE_ID, SCHEMA_ID, TABLE_NAME, TABLE_TYPE,
                                            TABLE_OWNER, ROW_COUNT, BYTE_SIZE,
                                            CREATE_DATE, LAST_ALTERED_DATE, COMMENT
                                        )
                                        VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                        """, (
                                            table_id,
                                            schema_id,
                                            table_name,
                                            table_row[2] if len(table_row) > 2 else None,  # Table type
                                            table_row[5] if len(table_row) > 5 else None,  # Owner
                                            table_row[3] if len(table_row) > 3 else None,  # Row count
                                            table_row[4] if len(table_row) > 4 else None,  # Bytes
                                            safe_timestamp(table_row[6]) if len(table_row) > 6 else None,  # Created
                                            safe_timestamp(table_row[7]) if len(table_row) > 7 else None,  # Modified
                                            table_row[9] if len(table_row) > 9 else None,  # Comment
                                        ))
                                    except Exception as table_insert_error:
                                        # If table already exists, update it
                                        try:
                                            cur.execute("""
                                            UPDATE CATALOG_TABLES
                                            SET 
                                                TABLE_TYPE = %s,
                                                TABLE_OWNER = %s,
                                                ROW_COUNT = %s,
                                                BYTE_SIZE = %s,
                                                CREATE_DATE = %s,
                                                LAST_ALTERED_DATE = %s,
                                                COMMENT = %s,
                                                COLLECTED_AT = CURRENT_TIMESTAMP()
                                            WHERE TABLE_ID = %s
                                            """, (
                                                table_row[2] if len(table_row) > 2 else None,  # Table type
                                                table_row[5] if len(table_row) > 5 else None,  # Owner
                                                table_row[3] if len(table_row) > 3 else None,  # Row count
                                                table_row[4] if len(table_row) > 4 else None,  # Bytes
                                                safe_timestamp(table_row[6]) if len(table_row) > 6 else None,  # Created
                                                safe_timestamp(table_row[7]) if len(table_row) > 7 else None,  # Modified
                                                table_row[9] if len(table_row) > 9 else None,  # Comment
                                                table_id
                                            ))
                                        except Exception as update_error:
                                            print(f"Error updating table {table_id}: {str(update_error)}")
                                    
                                    # Fast column collection using INFORMATION_SCHEMA
                                    try:
                                        # Get columns using INFORMATION_SCHEMA for better performance
                                        cur.execute(f"""
                                        SELECT 
                                            COLUMN_NAME,
                                            DATA_TYPE,
                                            IS_NULLABLE,
                                            COLUMN_DEFAULT,
                                            COMMENT,
                                            ORDINAL_POSITION
                                        FROM 
                                            {db_name}.INFORMATION_SCHEMA.COLUMNS
                                        WHERE 
                                            TABLE_SCHEMA = '{schema_name}' AND
                                            TABLE_NAME = '{table_name}'
                                        ORDER BY 
                                            ORDINAL_POSITION
                                        """)
                                        
                                        columns = []
                                        for col_row in cur.fetchall():
                                            columns.append({
                                                'name': col_row[0],
                                                'type': col_row[1],
                                                'nullable': col_row[2],
                                                'default': col_row[3],
                                                'comment': col_row[4],
                                                'position': col_row[5],
                                            })
                                        
                                        column_count += len(columns)
                                        
                                        # Apply column sampling if needed to improve performance
                                        if column_sample_pct < 100 and len(columns) > 10:
                                            sample_size = max(1, int(len(columns) * column_sample_pct / 100))
                                            # Always include the first few columns
                                            sampled_columns = columns[:3]
                                            # Add random sample from the rest
                                            import random
                                            rest_columns = columns[3:]
                                            random.shuffle(rest_columns)
                                            sampled_columns.extend(rest_columns[:sample_size-3])
                                            columns = sampled_columns
                                            
                                        # Store column details in CATALOG_COLUMNS
                                        for column in columns:
                                            column_id = f"{table_id}.{column['name']}"
                                            try:
                                                # Insert column data
                                                cur.execute(f"""
                                                INSERT INTO CATALOG_COLUMNS (
                                                    COLUMN_ID, TABLE_ID, COLUMN_NAME, 
                                                    ORDINAL_POSITION, DATA_TYPE, 
                                                    IS_NULLABLE, COLUMN_DEFAULT, COMMENT
                                                )
                                                VALUES (
                                                    %s, %s, %s, %s, %s, %s, %s, %s
                                                )
                                                """, (
                                                    column_id,
                                                    table_id,
                                                    column['name'],
                                                    column.get('position', 0),
                                                    column['type'],
                                                    column['nullable'] == 'YES',
                                                    column['default'],
                                                    column['comment']
                                                ))
                                            except Exception as column_insert_error:
                                                # If column already exists, update it
                                                try:
                                                    cur.execute(f"""
                                                    UPDATE CATALOG_COLUMNS
                                                    SET 
                                                        DATA_TYPE = %s,
                                                        IS_NULLABLE = %s,
                                                        COLUMN_DEFAULT = %s,
                                                        COMMENT = %s,
                                                        COLLECTED_AT = CURRENT_TIMESTAMP()
                                                    WHERE COLUMN_ID = %s
                                                    """, (
                                                        column['type'],
                                                        column['nullable'] == 'YES',
                                                        column['default'],
                                                        column['comment'],
                                                        column_id
                                                    ))
                                                except Exception as column_update_error:
                                                    print(f"Error updating column {column_id}: {str(column_update_error)}")
                                        
                                        # Only collect statistics if explicitly requested (performance optimization)
                                        if collect_stats:
                                            # Try to get basic statistics if available and requested
                                            try:
                                                # Get row count
                                                cur.execute(f"SELECT COUNT(*) FROM {db_name}.{schema_name}.{table_name} SAMPLE (5 ROWS)")
                                                row_count = cur.fetchone()[0]
                                                
                                                # Update table with row count
                                                cur.execute("""
                                                UPDATE CATALOG_TABLES
                                                SET ROW_COUNT = %s
                                                WHERE TABLE_ID = %s
                                                """, (row_count, table_id))
                                            except Exception as stats_error:
                                                print(f"Error getting statistics for {table_id}: {str(stats_error)}")
                                    
                                    except Exception as column_error:
                                        print(f"Error processing columns for table {db_name}.{schema_name}.{table_name}: {str(column_error)}")
                                    
                                except Exception as table_error:
                                    print(f"Error processing table {db_name}.{schema_name}.{table_name}: {str(table_error)}")
                                    
                        except Exception as schema_error:
                            print(f"Error processing schema {db_name}.{schema_name}: {str(schema_error)}")
                
                except Exception as proc_error:
                    print(f"Error during database processing: {str(proc_error)}")
                
                # Commit all changes
                conn.commit()
                
                # Now extract the data from Snowflake to Django models
                self.sync_snowflake_to_django(connection_params)
                
                return {
                    'status': 'success',
                    'database_count': 1,
                    'schema_count': schema_count,
                    'table_count': table_count,
                    'column_count': column_count,
                    'database': db_name,
                    'optimized': True
                }
        
        except Exception as e:
            print(f"Error collecting metadata for database {db_name}: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error collecting metadata for database {db_name}: {str(e)}"
            }
            
    def sync_snowflake_to_django(self, connection_params):
        """Sync metadata from Snowflake to Django models"""
        try:
            # Get the schema used for metadata
            metadata_schema = connection_params.get('metadata_schema', 'PUBLIC')
            
            # Check if we're processing a specific database only
            db_filter = ""
            if 'database' in connection_params and connection_params['database']:
                db_filter = f" WHERE DATABASE_ID = '{connection_params['database']}'"
            
            with self.get_connection(connection_params) as conn:
                cursor = conn.cursor()
                
                # Get databases
                cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_DATABASES{db_filter}")
                columns = [desc[0] for desc in cursor.description]
                databases_data = []
                for row in cursor.fetchall():
                    db_data = dict(zip(columns, row))
                    databases_data.append(db_data)
                    self.storage.save_metadata('database', db_data)
                
                # If processing a specific database, create schema filter
                schema_filter = ""
                if db_filter:
                    schema_filter = f" WHERE DATABASE_ID = '{connection_params['database']}'"
                
                # Get schemas
                cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_SCHEMAS{schema_filter}")
                columns = [desc[0] for desc in cursor.description]
                schemas_data = []
                for row in cursor.fetchall():
                    schema_data = dict(zip(columns, row))
                    schemas_data.append(schema_data)
                    self.storage.save_metadata('schema', schema_data)
                
                # Get schemas IDs for table filter
                schema_ids = []
                for schema in schemas_data:
                    schema_ids.append(f"'{schema['SCHEMA_ID']}'")
                
                # If we have schemas, create table filter
                table_filter = ""
                if schema_ids:
                    schema_list = ", ".join(schema_ids)
                    table_filter = f" WHERE SCHEMA_ID IN ({schema_list})"
                
                # Get tables
                cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_TABLES{table_filter}")
                columns = [desc[0] for desc in cursor.description]
                tables_data = []
                for row in cursor.fetchall():
                    table_data = dict(zip(columns, row))
                    tables_data.append(table_data)
                    self.storage.save_metadata('table', table_data)
                
                # Get table IDs for column filter
                table_ids = []
                for table in tables_data:
                    table_ids.append(f"'{table['TABLE_ID']}'")
                
                # If we have tables, create column filter
                column_filter = ""
                if table_ids:
                    table_list = ", ".join(table_ids)
                    column_filter = f" WHERE TABLE_ID IN ({table_list})"
                
                # Get columns
                cursor.execute(f"SELECT * FROM SNOWFLAKE_CATALOG.{metadata_schema}.CATALOG_COLUMNS{column_filter}")
                columns = [desc[0] for desc in cursor.description]
                columns_data = []
                for row in cursor.fetchall():
                    column_data = dict(zip(columns, row))
                    columns_data.append(column_data)
                    self.storage.save_metadata('column', column_data)
            
            # Update or create Django models
            try:
                from .models import SnowflakeDatabase, SnowflakeSchema, SnowflakeTable, SnowflakeColumn
                
                # Create databases
                for db_data in databases_data:
                    db_obj, created = SnowflakeDatabase.objects.update_or_create(
                        database_id=db_data['DATABASE_ID'],
                        defaults={
                            'database_name': db_data['DATABASE_NAME'],
                            'database_owner': db_data.get('DATABASE_OWNER'),
                            'database_description': db_data.get('DATABASE_DESCRIPTION'),
                            # Add other fields as needed
                        }
                    )
                
                # Create schemas
                for schema_data in schemas_data:
                    # Find parent database
                    try:
                        database = SnowflakeDatabase.objects.get(database_id=schema_data['DATABASE_ID'])
                        schema_obj, created = SnowflakeSchema.objects.update_or_create(
                            schema_id=schema_data['SCHEMA_ID'],
                            defaults={
                                'database': database,
                                'schema_name': schema_data['SCHEMA_NAME'],
                                'schema_owner': schema_data.get('SCHEMA_OWNER'),
                                'schema_description': schema_data.get('SCHEMA_DESCRIPTION'),
                                # Add other fields as needed
                            }
                        )
                    except SnowflakeDatabase.DoesNotExist:
                        print(f"Database not found for schema: {schema_data['SCHEMA_ID']}")
                
                # Create tables
                for table_data in tables_data:
                    # Find parent schema
                    try:
                        schema = SnowflakeSchema.objects.get(schema_id=table_data['SCHEMA_ID'])
                        table_obj, created = SnowflakeTable.objects.update_or_create(
                            table_id=table_data['TABLE_ID'],
                            defaults={
                                'schema': schema,
                                'table_name': table_data['TABLE_NAME'],
                                'table_type': table_data.get('TABLE_TYPE'),
                                'table_owner': table_data.get('TABLE_OWNER'),
                                'table_description': table_data.get('TABLE_DESCRIPTION'),
                                'row_count': table_data.get('ROW_COUNT'),
                                'byte_size': table_data.get('BYTE_SIZE'),
                                # Add other fields as needed
                            }
                        )
                    except SnowflakeSchema.DoesNotExist:
                        print(f"Schema not found for table: {table_data['TABLE_ID']}")
                
                # Create columns
                for column_data in columns_data:
                    # Find parent table
                    try:
                        table = SnowflakeTable.objects.get(table_id=column_data['TABLE_ID'])
                        column_obj, created = SnowflakeColumn.objects.update_or_create(
                            column_id=column_data['COLUMN_ID'],
                            defaults={
                                'table': table,
                                'column_name': column_data['COLUMN_NAME'],
                                'ordinal_position': column_data.get('ORDINAL_POSITION'),
                                'data_type': column_data.get('DATA_TYPE'),
                                'character_maximum_length': column_data.get('CHARACTER_MAXIMUM_LENGTH'),
                                'numeric_precision': column_data.get('NUMERIC_PRECISION'),
                                'numeric_scale': column_data.get('NUMERIC_SCALE'),
                                'is_nullable': column_data.get('IS_NULLABLE', True),
                                'column_default': column_data.get('COLUMN_DEFAULT'),
                                'column_description': column_data.get('COLUMN_DESCRIPTION'),
                                'comment': column_data.get('COMMENT'),
                                'is_primary_key': column_data.get('IS_PRIMARY_KEY', False),
                                'is_foreign_key': column_data.get('IS_FOREIGN_KEY', False),
                                'min_value': column_data.get('MIN_VALUE'),
                                'max_value': column_data.get('MAX_VALUE'),
                                'distinct_values': column_data.get('DISTINCT_VALUES'),
                                'null_count': column_data.get('NULL_COUNT')
                            }
                        )
                    except SnowflakeTable.DoesNotExist:
                        print(f"Table not found for column: {column_data['COLUMN_ID']}")
                
            except Exception as e:
                print(f"Error updating Django models: {str(e)}")
            
            return True
        except Exception as e:
            print(f"Error syncing Snowflake to Django: {str(e)}")
            return False
    
    # AI methods
    def generate_table_descriptions(self, connection_params, batch_size=5):
        """
        Generate and store table descriptions using AI
        """
        # Get tables without descriptions from external storage
        tables_to_process = []
        
        try:
            # Using Django ORM to query tables without descriptions
            from .models import SnowflakeTable
            tables = SnowflakeTable.objects.filter(
                table_description__isnull=True
            ).select_related('schema__database')[:batch_size]
            
            for table in tables:
                tables_to_process.append({
                    'table_id': table.table_id,
                    'table_name': table.table_name,
                    'schema_id': table.schema.schema_id,
                    'schema_name': table.schema.schema_name,
                    'database_name': table.schema.database.database_name
                })
            
            if not tables_to_process:
                return {
                    'status': 'success',
                    'message': 'No tables found without descriptions',
                    'processed_count': 0
                }
            
            # Generate descriptions with AI
            results = self.ai.generate_table_descriptions(connection_params, batch_size)
            
            # Update descriptions in our external database
            for i, table in enumerate(tables):
                if i < len(tables_to_process) and i < results.get('success_count', 0):
                    # Get the AI generated description
                    description = results.get('descriptions', {}).get(table.table_id)
                    keywords = results.get('keywords', {}).get(table.table_id, [])
                    
                    if description:
                        # Update using Django ORM
                        table.table_description = description
                        table.keywords = keywords
                        table.save(update_fields=['table_description', 'keywords'])
            
            return results
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error generating descriptions: {str(e)}'
            }
    
    def generate_tags_and_glossary(self, connection_params, batch_size=5):
        """
        Generate and store tags and business glossary terms using AI
        
        Args:
            connection_params: Dictionary with connection parameters
            batch_size: Number of objects to process in this batch
            
        Returns:
            Dictionary with results
        """
        # Process tables and databases from external storage
        try:
            # Generate tags and glossary terms with AI
            results = self.ai.generate_tags_and_glossary(connection_params, batch_size)
            
            # If using Django models, also update the local database
            try:
                from .models import SnowflakeTable, SnowflakeDatabase
                
                # Update tables without tags or business glossary terms
                tables = SnowflakeTable.objects.filter(
                    models.Q(tags={}) | 
                    models.Q(business_glossary_terms=[])
                ).select_related('schema__database')[:batch_size]
                
                for table in tables:
                    # Get column information for context
                    columns = []
                    for column in table.columns.all():
                        columns.append({
                            'name': column.column_name,
                            'type': column.data_type,
                            'description': column.column_description or "",
                            'comment': column.comment or ""
                        })
                    
                    # Generate tags and glossary terms with AI
                    ai_result = self.ai._generate_tags_and_glossary(
                        table.table_name, 
                        table.table_description or "", 
                        columns
                    )
                    
                    # Update table record
                    if not table.tags or table.tags == {}:
                        table.tags = ai_result.get('tags', {})
                    
                    if not table.business_glossary_terms or table.business_glossary_terms == []:
                        table.business_glossary_terms = ai_result.get('business_glossary_terms', [])
                    
                    table.save()
                
                # Update databases without tags or descriptions
                databases = SnowflakeDatabase.objects.filter(
                    models.Q(tags={}) | 
                    models.Q(database_description__isnull=True) |
                    models.Q(database_description="")
                )[:batch_size]
                
                for database in databases:
                    # Get schemas for context
                    schemas = []
                    for schema in database.schemas.all():
                        schemas.append({
                            'name': schema.schema_name,
                            'description': schema.schema_description or ""
                        })
                    
                    # Generate metadata for the database
                    ai_result = self.ai._generate_database_metadata(
                        database.database_name,
                        schemas
                    )
                    
                    # Update database record
                    if not database.tags or database.tags == {}:
                        database.tags = ai_result.get('tags', {})
                    
                    if not database.database_description or database.database_description == "":
                        database.database_description = ai_result.get('description', "")
                    
                    database.save()
                    
            except Exception as e:
                print(f"Error updating Django models: {str(e)}")
                # Continue with the process even if Django update fails
            
            return results
            
        except Exception as e:
            error_message = f"Error in generate_tags_and_glossary: {str(e)}"
            print(error_message)
            return {
                'status': 'error',
                'message': error_message
            }
    
    def set_ai_api_key(self, api_key, provider="openai"):
        """Set the API key for AI services"""
        self.ai.set_ai_api_key(api_key, provider)
    
    # Progress tracking and caching
    def _track_progress(self, current, total, process_id):
        """Track progress of long-running operations"""
        return self.metadata._track_progress(current, total, process_id)
    
    def _cache_metadata(self, cache_key, metadata):
        """Cache metadata for faster retrieval"""
        self.metadata._cache_metadata(cache_key, metadata)
    
    # Save connection implementation using external database
    def save_connection_impl(self, request_data, process_logger=None):
        """Save Snowflake connection details to external database"""
        try:
            # Extract and validate required fields
            account = request_data.get('account')
            username = request_data.get('username')
            password = request_data.get('password')
            warehouse = request_data.get('warehouse')
            
            # Check each field individually to avoid "None is not iterable" error
            if account is None or username is None or password is None or warehouse is None:
                return {
                    'status': 'error',
                    'message': 'Missing required fields: account, username, password, warehouse'
                }
            
            if not account or not username or not password or not warehouse:
                return {
                    'status': 'error',
                    'message': 'Required fields cannot be empty: account, username, password, warehouse'
                }
            
            # Test the connection before saving
            connection_params = {
                'account': account,
                'username': username,
                'password': password,
                'warehouse': warehouse,
                'database': request_data.get('database'),
                'schema': request_data.get('schema'),
                'role': request_data.get('role')
            }
            
            # Try connecting
            with self.get_connection(connection_params, save_details=False) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT()")
                result = cursor.fetchone()
                
                # Check if result is None before unpacking
                if result is None:
                    return {
                        'status': 'error',
                        'message': 'Failed to retrieve user and account information'
                    }
                
                user, acct = result
                
                # Connection successful, save to external database
                try:
                    # Use Django ORM to save connection
                    connection_obj, created = SnowflakeConnectionModel.objects.update_or_create(
                        account=account,
                        username=username,
                        defaults={
                            'name': request_data.get('name', 'Default Connection'),
                            'password': password,
                            'warehouse': warehouse,
                            'database_name': request_data.get('database'),
                            'schema_name': request_data.get('schema'),
                            'role': request_data.get('role'),
                            'is_active': True
                        }
                    )
                    
                    # Get the ID of the connection object, using pk (primary key) which is guaranteed to exist
                    connection_id = connection_obj.pk
                    
                    return {
                        'status': 'success',
                        'connection_id': connection_id,
                        'message': f'Connection {"created" if created else "updated"} successfully',
                        'details': {
                            'user': user,
                            'account': acct
                        }
                    }
                    
                except Exception as db_error:
                    if process_logger:
                        process_logger.error(f"Database error: {str(db_error)}")
                    return {
                        'status': 'error',
                        'message': f'Error saving connection to database: {str(db_error)}'
                    }
                
        except Exception as e:
            if process_logger:
                process_logger.error(f"Error saving connection: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error saving connection: {str(e)}'
            }


