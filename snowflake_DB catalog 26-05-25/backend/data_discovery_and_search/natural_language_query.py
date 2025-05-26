import json
import time
from typing import Dict, Any, List, Optional
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from .connection_manager import SearchConnectionManager
from .ai_utils import generate_sql_from_natural_language, OPENAI_AVAILABLE

# Create a connection manager instance
connection_manager = SearchConnectionManager()

def get_schema_information(credentials: dict) -> Dict[str, Any]:
    """
    Get comprehensive schema information directly from Snowflake's INFORMATION_SCHEMA
    """
    try:
        # Query for table and column details
        base_schema_query = """ 
        SELECT 
            t.TABLE_NAME,
            t.TABLE_TYPE,
            t.COMMENT as TABLE_DESCRIPTION,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.COMMENT as COLUMN_DESCRIPTION,
            c.ORDINAL_POSITION
        FROM 
            INFORMATION_SCHEMA.TABLES t
        LEFT JOIN 
            INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE 
            t.TABLE_SCHEMA = CURRENT_SCHEMA()
        ORDER BY 
            t.TABLE_NAME, c.ORDINAL_POSITION
        """

        # Execute the base query
        result = connection_manager.execute_query(credentials, base_schema_query)
        
        # Process schema information
        schema_info = {}
        
        # Ensure result['data'] is a list before iterating
        result_data = result.get('data', [])
        if result_data is None:
            result_data = []
            
        # Process tables and columns
        for row in result_data:
            table_name, table_type, table_desc, col_name, col_type, char_max_len, num_precision, num_scale, is_nullable, col_default, col_desc, ordinal_pos = row
            
            if table_name not in schema_info:
                schema_info[table_name] = {
                    'table_type': table_type,
                    'description': table_desc or 'No description',
                    'columns': {},
                    'primary_keys': [],
                    'foreign_keys': [],
                    'unique_constraints': [],
                    'row_count': 0
                }
            
            if col_name:
                schema_info[table_name]['columns'][col_name] = {
                    'type': col_type,
                    'description': col_desc or 'No description',
                    'ordinal_position': ordinal_pos,
                    'nullable': is_nullable == 'YES',
                    'default': col_default
                }
                
                # Add additional type information if available
                if char_max_len is not None:
                    schema_info[table_name]['columns'][col_name]['max_length'] = char_max_len
                if num_precision is not None:
                    schema_info[table_name]['columns'][col_name]['precision'] = num_precision
                if num_scale is not None:
                    schema_info[table_name]['columns'][col_name]['scale'] = num_scale
        
        # Query for primary keys
        pk_query = """
        SELECT 
            c.TABLE_NAME, 
            c.COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c 
            ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME 
            AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE 
            tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.TABLE_SCHEMA = CURRENT_SCHEMA()
        ORDER BY 
            c.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        try:
            pk_result = connection_manager.execute_query(credentials, pk_query)
            pk_data = pk_result.get('data', [])
            
            if pk_data:
                for row in pk_data:
                    table_name, column_name = row
                    if table_name in schema_info:
                        schema_info[table_name]['primary_keys'].append(column_name)
                        if column_name in schema_info[table_name]['columns']:
                            schema_info[table_name]['columns'][column_name]['is_primary_key'] = True
        except Exception as e:
            print(f"Error getting primary key information: {str(e)}")
        
        # Query for foreign keys
        fk_query = """
        SELECT 
            c.TABLE_NAME, 
            c.COLUMN_NAME,
            rc.TABLE_NAME as REFERENCED_TABLE_NAME,
            rc.COLUMN_NAME as REFERENCED_COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c 
            ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME 
            AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
        JOIN 
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
            ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
            AND tc.TABLE_SCHEMA = rc.TABLE_SCHEMA
        WHERE 
            tc.CONSTRAINT_TYPE = 'FOREIGN KEY' 
            AND tc.TABLE_SCHEMA = CURRENT_SCHEMA()
        ORDER BY 
            c.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        try:
            fk_result = connection_manager.execute_query(credentials, fk_query)
            fk_data = fk_result.get('data', [])
            
            if fk_data:
                for row in fk_data:
                    table_name, column_name, ref_table_name, ref_column_name = row
                    if table_name in schema_info:
                        fk_info = {
                            'column': column_name,
                            'referenced_table': ref_table_name,
                            'referenced_column': ref_column_name
                        }
                        schema_info[table_name]['foreign_keys'].append(fk_info)
                        if column_name in schema_info[table_name]['columns']:
                            schema_info[table_name]['columns'][column_name]['is_foreign_key'] = True
                            schema_info[table_name]['columns'][column_name]['references'] = {
                                'table': ref_table_name,
                                'column': ref_column_name
                            }
        except Exception as e:
            print(f"Error getting foreign key information: {str(e)}")
            
        # Get approximate row counts for each table
        for table_name in schema_info:
            try:
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                count_result = connection_manager.execute_query(credentials, count_query)
                count_data = count_result.get('data', [])
                if count_data and count_data[0]:
                    row_count = count_data[0][0]
                    schema_info[table_name]['row_count'] = row_count
            except Exception as e:
                print(f"Error getting row count for table {table_name}: {str(e)}")
                # Continue with other tables if one fails

        # If no tables found, throw an error
        if not schema_info:
            raise Exception(f"No tables found in schema. Please use a schema with tables.")

        print(f"Retrieved metadata for {len(schema_info)} tables from INFORMATION_SCHEMA")
        return schema_info

    except Exception as e:
        print(f"Error getting schema information: {str(e)}")
        raise

def execute_natural_language_query(credentials: dict, query: str, user_identifier: str = None) -> Dict[str, Any]:
    """
    Execute natural language query against Snowflake
    """
    try:
        # Get schema information
        schema_info = get_schema_information(credentials)
        
        # Generate SQL from natural language
        sql_query = generate_sql_from_natural_language(query, schema_info)
        
        if not sql_query:
            return {
                'status': 'error',
                'message': 'Failed to generate SQL query from natural language'
            }
            
        # Execute the SQL query
        start_time = time.time()
        result = connection_manager.execute_query(credentials, sql_query)
        execution_time = time.time() - start_time
        
        # Format the response
        columns = result.get('columns', [])
        
        # Ensure rows is a list before iterating
        rows = result.get('data', [])
        if rows is None:
            rows = []
        
        # Convert rows to list of dictionaries
        formatted_rows = []
        for row in rows:
            formatted_rows.append(dict(zip(columns, row)))
            
        return {
            'status': 'success',
            'result': {
                'columns': columns,
                'rows': formatted_rows,
                'sql': sql_query,
                'execution_time': execution_time,
                'natural_language_query': query
            }
        }
        
    except Exception as e:
        print(f"Error executing natural language query: {str(e)}")
        raise

def test_snowflake_connection(credentials: Dict[str, str]) -> bool:
    """
    Test Snowflake connection for data discovery and search
    """
    try:
        connection_manager.execute_query(credentials, "SELECT 1")
        return True
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
        return False

@csrf_exempt
@require_http_methods(["POST"])
def natural_language_query_endpoint(request):
    """
    Handle natural language query requests
    """
    try:
        data = json.loads(request.body)
        print(f"Request data: {data.keys()}")
        
        # Extract credentials and query based on request format
        # Handle both new format (with 'credentials' object) and old format (flat structure)
        credentials = {}
        query = ''
        user_identifier = None
        
        if 'credentials' in data:
            # New format
            credentials = data.get('credentials', {})
            query = data.get('query', '')
            user_identifier = data.get('user_identifier', None)
        else:
            # Old format (flat structure from frontend)
            query = data.get('query', '')
            user_identifier = data.get('user_identifier', None)
            
            # Extract credentials from flat structure
            for field in ['account', 'username', 'password', 'warehouse', 'role', 'database', 'schema']:
                if field in data:
                    credentials[field] = data[field]
        
        print(f"Extracted query: {query}")
        print(f"Extracted credentials: {', '.join(credentials.keys())}")
        
        if not credentials:
            return JsonResponse({
                'status': 'error',
                'message': 'No Snowflake credentials provided'
            }, status=400)
            
        if not query:
            return JsonResponse({
                'status': 'error',
                'message': 'No natural language query provided'
            }, status=400)
            
        # Check for required credentials
        required_fields = ['account', 'username', 'password', 'warehouse']
        missing_fields = [field for field in required_fields if field not in credentials]
        
        if missing_fields:
            return JsonResponse({
                'status': 'error',
                'message': f'Missing required credentials: {", ".join(missing_fields)}'
            }, status=400)
            
        # Check if OpenAI is available
        if not OPENAI_AVAILABLE:
            return JsonResponse({
                'status': 'error',
                'message': 'OpenAI is not available. Please check your API key configuration.'
            }, status=500)
            
        # Execute the natural language query
        result = execute_natural_language_query(credentials, query, user_identifier)
        
        return JsonResponse(result)
        
    except Exception as e:
        error_message = str(e)
        print(f"Error processing request: {error_message}")
        return JsonResponse({
            'status': 'error',
            'message': error_message
        }, status=500) 