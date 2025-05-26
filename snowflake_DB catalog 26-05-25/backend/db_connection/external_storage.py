from django.db import models, connection
from django.conf import settings
import json
from datetime import datetime

class DatabaseStorage:
    """
    Handle metadata storage in an external database instead of inside Snowflake.
    This allows for more flexible storage options and removes dependency on
    maintaining metadata tables in the user's Snowflake account.
    """
    
    def __init__(self):
        """Initialize the database storage handler"""
        pass
    
    def save_connection(self, connection_data):
        """
        Save Snowflake connection details to external database
        
        Args:
            connection_data: Dictionary with connection parameters
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Use Django's connection to execute SQL
            with connection.cursor() as cursor:
                # Check if connection already exists
                cursor.execute(
                    """
                    SELECT id FROM snowflake_connections 
                    WHERE account = %s AND username = %s
                    """, 
                    [connection_data.get('account'), connection_data.get('username')]
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing connection
                    cursor.execute(
                        """
                        UPDATE snowflake_connections
                        SET 
                            name = %s,
                            password = %s,
                            warehouse = %s,
                            database_name = %s,
                            schema_name = %s,
                            role = %s,
                            last_used = %s,
                            is_active = %s
                        WHERE id = %s
                        """,
                        [
                            connection_data.get('name', 'Default Connection'),
                            connection_data.get('password'),
                            connection_data.get('warehouse'),
                            connection_data.get('database'),
                            connection_data.get('schema'),
                            connection_data.get('role'),
                            datetime.now(),
                            True,
                            existing[0]
                        ]
                    )
                    connection_id = existing[0]
                    is_new = False
                else:
                    # Insert new connection
                    try:
                        cursor.execute(
                            """
                            INSERT INTO snowflake_connections
                            (name, account, username, password, warehouse, database_name, 
                             schema_name, role, created_at, last_used, is_active)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            [
                                connection_data.get('name', 'Default Connection'),
                                connection_data.get('account'),
                                connection_data.get('username'),
                                connection_data.get('password'),
                                connection_data.get('warehouse'),
                                connection_data.get('database'),
                                connection_data.get('schema'),
                                connection_data.get('role'),
                                datetime.now(),
                                datetime.now(),
                                True
                            ]
                        )
                        result = cursor.fetchone()
                        connection_id = result[0] if result else None
                    except Exception as e:
                        # If the RETURNING clause isn't supported, get the ID using the last insert ID
                        print(f"Error with RETURNING clause: {e}")
                        cursor.execute(
                            """
                            INSERT INTO snowflake_connections
                            (name, account, username, password, warehouse, database_name, 
                             schema_name, role, created_at, last_used, is_active)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            [
                                connection_data.get('name', 'Default Connection'),
                                connection_data.get('account'),
                                connection_data.get('username'),
                                connection_data.get('password'),
                                connection_data.get('warehouse'),
                                connection_data.get('database'),
                                connection_data.get('schema'),
                                connection_data.get('role'),
                                datetime.now(),
                                datetime.now(),
                                True
                            ]
                        )
                        # Get the last inserted ID
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        result = cursor.fetchone()
                        connection_id = result[0] if result else None
                    
                    is_new = True
                
                return {
                    'status': 'success',
                    'connection_id': connection_id,
                    'message': f'Connection {"created" if is_new else "updated"} successfully',
                    'is_new': is_new
                }
        
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error saving connection: {str(e)}'
            }
    
    def get_connection(self, connection_id=None, account=None, username=None):
        """
        Retrieve connection details by ID or account/username
        
        Args:
            connection_id: Optional ID of the connection to retrieve
            account: Optional account name
            username: Optional username
            
        Returns:
            Dictionary with connection details or None if not found
        """
        try:
            with connection.cursor() as cursor:
                if connection_id:
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_connections 
                        WHERE id = %s AND is_active = TRUE
                        """, 
                        [connection_id]
                    )
                elif account and username:
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_connections 
                        WHERE account = %s AND username = %s AND is_active = TRUE
                        """, 
                        [account, username]
                    )
                else:
                    return None
                
                # Get column names before fetching the row
                if cursor.description is None:
                    return None
                
                columns = [col[0] for col in cursor.description]
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                # Update last_used timestamp
                cursor.execute(
                    """
                    UPDATE snowflake_connections
                    SET last_used = %s
                    WHERE id = %s
                    """,
                    [datetime.now(), row[0]]
                )
                
                # Convert row to dictionary
                connection_data = dict(zip(columns, row))
                return connection_data
        
        except Exception as e:
            print(f"Error retrieving connection: {str(e)}")
            return None
    
    def save_metadata(self, metadata_type, metadata):
        """
        Save metadata to external database
        
        Args:
            metadata_type: Type of metadata (database, schema, table, column)
            metadata: Dictionary with metadata
            
        Returns:
            Dictionary with operation results
        """
        try:
            with connection.cursor() as cursor:
                if metadata_type == 'database':
                    # Check if database already exists
                    cursor.execute(
                        """
                        SELECT id FROM snowflake_databases 
                        WHERE database_id = %s
                        """, 
                        [metadata.get('database_id')]
                    )
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing database
                        cursor.execute(
                            """
                            UPDATE snowflake_databases
                            SET 
                                database_name = %s,
                                database_owner = %s,
                                database_description = %s,
                                create_date = %s,
                                last_altered_date = %s,
                                comment = %s,
                                tags = %s,
                                collected_at = %s
                            WHERE id = %s
                            """,
                            [
                                metadata.get('database_name'),
                                metadata.get('database_owner'),
                                metadata.get('database_description'),
                                metadata.get('create_date'),
                                metadata.get('last_altered_date'),
                                metadata.get('comment'),
                                json.dumps(metadata.get('tags', {})),
                                datetime.now(),
                                existing[0]
                            ]
                        )
                        return {'status': 'success', 'message': 'Database metadata updated successfully'}
                    else:
                        # Insert new database
                        cursor.execute(
                            """
                            INSERT INTO snowflake_databases
                            (database_id, database_name, database_owner, database_description, 
                             create_date, last_altered_date, comment, tags, collected_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            [
                                metadata.get('database_id'),
                                metadata.get('database_name'),
                                metadata.get('database_owner'),
                                metadata.get('database_description'),
                                metadata.get('create_date'),
                                metadata.get('last_altered_date'),
                                metadata.get('comment'),
                                json.dumps(metadata.get('tags', {})),
                                datetime.now()
                            ]
                        )
                        return {'status': 'success', 'message': 'Database metadata saved successfully'}
                
                elif metadata_type == 'table':
                    # Check if table already exists
                    cursor.execute(
                        """
                        SELECT id FROM snowflake_tables 
                        WHERE table_id = %s
                        """, 
                        [metadata.get('table_id')]
                    )
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing table
                        cursor.execute(
                            """
                            UPDATE snowflake_tables
                            SET 
                                schema_id = %s,
                                table_name = %s,
                                table_type = %s,
                                table_owner = %s,
                                table_description = %s,
                                row_count = %s,
                                byte_size = %s,
                                create_date = %s,
                                last_altered_date = %s,
                                comment = %s,
                                tags = %s,
                                sensitivity_level = %s,
                                data_domain = %s,
                                keywords = %s,
                                business_glossary_terms = %s,
                                collected_at = %s
                            WHERE id = %s
                            """,
                            [
                                metadata.get('schema_id'),
                                metadata.get('table_name'),
                                metadata.get('table_type'),
                                metadata.get('table_owner'),
                                metadata.get('table_description'),
                                metadata.get('row_count'),
                                metadata.get('byte_size'),
                                metadata.get('create_date'),
                                metadata.get('last_altered_date'),
                                metadata.get('comment'),
                                json.dumps(metadata.get('tags', {})),
                                metadata.get('sensitivity_level'),
                                metadata.get('data_domain'),
                                json.dumps(metadata.get('keywords', [])),
                                json.dumps(metadata.get('business_glossary_terms', [])),
                                datetime.now(),
                                existing[0]
                            ]
                        )
                        return {'status': 'success', 'message': 'Table metadata updated successfully'}
                    else:
                        # Insert new table
                        cursor.execute(
                            """
                            INSERT INTO snowflake_tables
                            (table_id, schema_id, table_name, table_type, table_owner,
                             table_description, row_count, byte_size, create_date,
                             last_altered_date, comment, tags, sensitivity_level,
                             data_domain, keywords, business_glossary_terms, collected_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            [
                                metadata.get('table_id'),
                                metadata.get('schema_id'),
                                metadata.get('table_name'),
                                metadata.get('table_type'),
                                metadata.get('table_owner'),
                                metadata.get('table_description'),
                                metadata.get('row_count'),
                                metadata.get('byte_size'),
                                metadata.get('create_date'),
                                metadata.get('last_altered_date'),
                                metadata.get('comment'),
                                json.dumps(metadata.get('tags', {})),
                                metadata.get('sensitivity_level'),
                                metadata.get('data_domain'),
                                json.dumps(metadata.get('keywords', [])),
                                json.dumps(metadata.get('business_glossary_terms', [])),
                                datetime.now()
                            ]
                        )
                        return {'status': 'success', 'message': 'Table metadata saved successfully'}
                
                # Similar patterns can be implemented for schema and column types
                else:
                    return {'status': 'error', 'message': f'Unsupported metadata type: {metadata_type}'}
        
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error saving metadata: {str(e)}'
            }
    
    def get_metadata(self, metadata_type, identifier):
        """
        Retrieve metadata from external database
        
        Args:
            metadata_type: Type of metadata (database, schema, table, column)
            identifier: Identifier for the metadata
            
        Returns:
            Dictionary with metadata or None if not found
        """
        try:
            with connection.cursor() as cursor:
                if metadata_type == 'database':
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_databases 
                        WHERE database_id = %s
                        """, 
                        [identifier]
                    )
                elif metadata_type == 'schema':
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_schemas 
                        WHERE schema_id = %s
                        """, 
                        [identifier]
                    )
                elif metadata_type == 'table':
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_tables 
                        WHERE table_id = %s
                        """, 
                        [identifier]
                    )
                elif metadata_type == 'column':
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_columns 
                        WHERE column_id = %s
                        """, 
                        [identifier]
                    )
                else:
                    return None
                
                # Get column names before fetching the row
                if cursor.description is None:
                    return None
                
                columns = [col[0] for col in cursor.description]
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                # Convert row to dictionary
                metadata = dict(zip(columns, row))
                
                # Convert JSON strings back to objects
                for field in ['tags', 'keywords', 'business_glossary_terms']:
                    if field in metadata and metadata[field]:
                        try:
                            metadata[field] = json.loads(metadata[field])
                        except:
                            metadata[field] = {}
                
                return metadata
        
        except Exception as e:
            print(f"Error retrieving metadata: {str(e)}")
            return None
    
    def search_tables(self, query, connection_id=None):
        """
        Search for tables in the external database
        
        Args:
            query: Search query
            connection_id: Optional connection ID to filter results
            
        Returns:
            List of tables matching the query
        """
        try:
            with connection.cursor() as cursor:
                if connection_id:
                    cursor.execute(
                        """
                        SELECT t.* FROM snowflake_tables t
                        JOIN snowflake_schemas s ON t.schema_id = s.schema_id
                        JOIN snowflake_databases d ON s.database_id = d.database_id
                        WHERE 
                            (t.table_name ILIKE %s OR 
                             t.table_description ILIKE %s OR
                             t.comment ILIKE %s OR
                             CAST(t.keywords AS TEXT) ILIKE %s) AND
                            connection_id = %s
                        """, 
                        [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", connection_id]
                    )
                else:
                    cursor.execute(
                        """
                        SELECT * FROM snowflake_tables
                        WHERE 
                            table_name ILIKE %s OR 
                            table_description ILIKE %s OR
                            comment ILIKE %s OR
                            CAST(keywords AS TEXT) ILIKE %s
                        """, 
                        [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%"]
                    )
                
                # Check if we got a result set with columns before proceeding
                if cursor.description is None:
                    return []
                
                columns = [col[0] for col in cursor.description]
                results = []
                
                for row in cursor.fetchall():
                    table = dict(zip(columns, row))
                    
                    # Convert JSON strings back to objects
                    for field in ['tags', 'keywords', 'business_glossary_terms']:
                        if field in table and table[field]:
                            try:
                                table[field] = json.loads(table[field])
                            except:
                                table[field] = {}
                    
                    results.append(table)
                
                return results
        
        except Exception as e:
            print(f"Error searching tables: {str(e)}")
            return [] 