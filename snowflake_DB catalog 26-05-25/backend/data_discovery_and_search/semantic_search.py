import json
import time
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings

from .connection_manager import SearchConnectionManager
from .ai_utils import OPENAI_AVAILABLE, get_openai_client

# Create a connection manager instance
connection_manager = SearchConnectionManager()

# Constants for vector dimensions and similarity threshold
VECTOR_DIMENSIONS = 1536  # OpenAI embedding dimensions
SIMILARITY_THRESHOLD = 0.6  # Minimum similarity score to include in results

def generate_embeddings(text: str) -> List[float]:
    """
    Generate vector embeddings for text using OpenAI's embeddings API
    """
    try:
        if not OPENAI_AVAILABLE:
            print("OpenAI is not available for embeddings generation")
            return [0.0] * VECTOR_DIMENSIONS  # Return zero vector as fallback
            
        openai_client = get_openai_client()
        
        # Clean and prepare the text
        clean_text = text.strip().replace('\n', ' ').replace('\t', ' ')
        
        # Generate embeddings using OpenAI
        response = openai_client.embeddings.create(
            model="text-embedding-ada-002",
            input=clean_text
        )
        
        # Extract the embedding vector
        embedding = response.data[0].embedding
        return embedding
        
    except Exception as e:
        print(f"Error generating embeddings: {str(e)}")
        # Return zero vector as fallback
        return [0.0] * VECTOR_DIMENSIONS

def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Calculate cosine similarity between two vectors
    """
    if not vec1 or not vec2:
        return 0.0
        
    # Convert to numpy arrays for efficient computation
    a = np.array(vec1)
    b = np.array(vec2)
    
    # Calculate cosine similarity
    dot_product = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    
    if norm_a == 0 or norm_b == 0:
        return 0.0
        
    return dot_product / (norm_a * norm_b)

def expand_business_terms(query: str) -> List[str]:
    """
    Expand business terms in the query to related concepts
    """
    # Dictionary of business terms and their related concepts
    business_terms = {
        "spend": ["expense", "cost", "payment", "budget", "transaction", "purchase"],
        "customer": ["client", "buyer", "consumer", "purchaser", "user", "account"],
        "sales": ["revenue", "income", "earnings", "profit", "transaction"],
        "product": ["item", "merchandise", "goods", "offering", "commodity"],
        "employee": ["staff", "personnel", "worker", "associate", "team member"],
        "marketing": ["advertising", "promotion", "campaign", "branding"],
        "finance": ["accounting", "treasury", "budget", "fiscal", "monetary"],
        "inventory": ["stock", "supply", "goods", "merchandise", "assets"],
        "order": ["purchase", "transaction", "requisition", "booking"],
        "shipping": ["delivery", "transport", "logistics", "freight", "fulfillment"]
    }
    
    expanded_terms = [query]  # Always include the original query
    
    # Check if any business terms are in the query
    query_lower = query.lower()
    for term, synonyms in business_terms.items():
        if term in query_lower:
            expanded_terms.extend(synonyms)
            
    return list(set(expanded_terms))  # Remove duplicates

def get_schema_information(credentials: dict) -> Dict[str, Any]:
    """
    Get comprehensive schema information from Snowflake with additional metadata for search
    """
    try:
        # Query for table and column details with additional metadata
        # Using a more basic query that should work across all Snowflake editions
        schema_query = """
        SELECT
            t.TABLE_NAME,
            t.TABLE_TYPE,
            COALESCE(t.COMMENT, '') as TABLE_DESCRIPTION,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            COALESCE(c.COMMENT, '') as COLUMN_DESCRIPTION,
            c.ORDINAL_POSITION,
            COALESCE(t.ROW_COUNT, 0) as ROW_COUNT,
            COALESCE(t.BYTES, 0) as SIZE_BYTES,
            COALESCE(t.CREATED, CURRENT_TIMESTAMP()) as CREATED,
            COALESCE(t.LAST_ALTERED, CURRENT_TIMESTAMP()) as LAST_ALTERED
        FROM
            INFORMATION_SCHEMA.TABLES t
        LEFT JOIN
            INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE
            t.TABLE_SCHEMA = CURRENT_SCHEMA()
            AND t.TABLE_TYPE = 'BASE TABLE'  -- Only include actual tables, not views
        ORDER BY
            t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        # Fallback query if the above fails - simpler version with fewer columns
        fallback_schema_query = """
        SELECT
            t.TABLE_NAME,
            t.TABLE_TYPE,
            '' as TABLE_DESCRIPTION,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            NULL as CHARACTER_MAXIMUM_LENGTH,
            NULL as NUMERIC_PRECISION,
            NULL as NUMERIC_SCALE,
            c.IS_NULLABLE,
            NULL as COLUMN_DEFAULT,
            '' as COLUMN_DESCRIPTION,
            c.ORDINAL_POSITION,
            0 as ROW_COUNT,
            0 as SIZE_BYTES,
            CURRENT_TIMESTAMP() as CREATED,
            CURRENT_TIMESTAMP() as LAST_ALTERED
        FROM
            INFORMATION_SCHEMA.TABLES t
        LEFT JOIN
            INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE
            t.TABLE_SCHEMA = CURRENT_SCHEMA()
        ORDER BY
            t.TABLE_NAME, c.ORDINAL_POSITION
        """

        # Make sure to set the database context before executing the query
        if credentials.get('database'):
            use_database_query = f"USE DATABASE {credentials.get('database')}"
            connection_manager.execute_query(credentials, use_database_query)
        
        # Set the schema context if provided
        if credentials.get('schema'):
            use_schema_query = f"USE SCHEMA {credentials.get('schema')}"
            connection_manager.execute_query(credentials, use_schema_query)
        
        # Try the main schema query first
        try:
            result = connection_manager.execute_query(credentials, schema_query)
            result_data = result.get('data', [])
            
            # If we didn't get any data, try the fallback query
            if not result_data:
                print("Main schema query returned no data, trying fallback query")
                result = connection_manager.execute_query(credentials, fallback_schema_query)
        except Exception as e:
            print(f"Main schema query failed: {str(e)}, trying fallback query")
            # Try the fallback query if the main one fails
            result = connection_manager.execute_query(credentials, fallback_schema_query)
        
        # Process schema information
        schema_info = {}
        
        # Ensure result['data'] is a list before iterating
        result_data = result.get('data', [])
        if result_data is None:
            result_data = []
            
        # Process tables and columns
        for row in result_data:
            table_name, table_type, table_desc, col_name, col_type, char_max_len, num_precision, num_scale, is_nullable, col_default, col_desc, ordinal_pos, row_count, size_bytes, created, last_altered = row
            
            if table_name not in schema_info:
                schema_info[table_name] = {
                    'table_type': table_type,
                    'description': table_desc or 'No description',
                    'columns': {},
                    'primary_keys': [],
                    'foreign_keys': [],
                    'unique_constraints': [],
                    'row_count': row_count or 0,
                    'size_bytes': size_bytes or 0,
                    'created': created,
                    'last_altered': last_altered,
                    'search_text': f"{table_name} {table_desc or ''}"  # Text for search indexing
                }
            
            if col_name:
                schema_info[table_name]['columns'][col_name] = {
                    'type': col_type,
                    'description': col_desc or 'No description',
                    'ordinal_position': ordinal_pos,
                    'nullable': is_nullable == 'YES',
                    'default': col_default
                }
                
                # Add column info to the search text
                schema_info[table_name]['search_text'] += f" {col_name} {col_desc or ''} {col_type}"
                
                # Add additional type information if available
                if char_max_len is not None:
                    schema_info[table_name]['columns'][col_name]['max_length'] = char_max_len
                if num_precision is not None:
                    schema_info[table_name]['columns'][col_name]['precision'] = num_precision
                if num_scale is not None:
                    schema_info[table_name]['columns'][col_name]['scale'] = num_scale
        
        # Query for primary keys - using a simpler approach that's more likely to work across Snowflake editions
        pk_query = """
        SELECT
            TABLE_NAME,
            COLUMN_NAME
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = CURRENT_SCHEMA()
            AND IS_IDENTITY = 'YES'
        ORDER BY
            TABLE_NAME, ORDINAL_POSITION
        """
        
        # Alternative query if the above fails - some Snowflake accounts might use a different approach
        alt_pk_query = """
        SELECT
            TABLE_NAME,
            COLUMN_NAME
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = CURRENT_SCHEMA()
            AND COLUMN_KEY = 'PRI'
        ORDER BY
            TABLE_NAME, ORDINAL_POSITION
        """
        
        try:
            # Make sure database and schema context is still set
            if credentials.get('database'):
                use_database_query = f"USE DATABASE {credentials.get('database')}"
                connection_manager.execute_query(credentials, use_database_query)
            
            if credentials.get('schema'):
                use_schema_query = f"USE SCHEMA {credentials.get('schema')}"
                connection_manager.execute_query(credentials, use_schema_query)
            
            # Try the first primary key query approach
            try:
                pk_result = connection_manager.execute_query(credentials, pk_query)
                pk_data = pk_result.get('data', [])
            except Exception as e:
                print(f"First primary key query failed: {str(e)}")
                pk_data = []
                
            # If the first approach didn't work, try the alternative
            if not pk_data:
                try:
                    pk_result = connection_manager.execute_query(credentials, alt_pk_query)
                    pk_data = pk_result.get('data', [])
                except Exception as e:
                    print(f"Alternative primary key query failed: {str(e)}")
                    pk_data = []
            
            # Process the primary key data if we got any
            if pk_data:
                for row in pk_data:
                    table_name, column_name = row
                    if table_name in schema_info:
                        schema_info[table_name]['primary_keys'].append(column_name)
                        if column_name in schema_info[table_name]['columns']:
                            schema_info[table_name]['columns'][column_name]['is_primary_key'] = True
                            # Add to search text
                            schema_info[table_name]['search_text'] += f" primary key {column_name}"
        except Exception as e:
            print(f"Error getting primary key information: {str(e)}")
            # Continue without primary key information
        
        # We'll make the foreign key query optional since it might not be available in all Snowflake editions
        # or the user might not have permissions to access these views
        
        # Simplified foreign key query that's more likely to work
        fk_query = """
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            -- These fields might not be available, so we'll use placeholders
            'UNKNOWN' as REFERENCED_TABLE_NAME,
            'UNKNOWN' as REFERENCED_COLUMN_NAME
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = CURRENT_SCHEMA()
            AND COLUMN_NAME LIKE '%_ID' OR COLUMN_NAME LIKE '%_FK'
        ORDER BY
            TABLE_NAME, ORDINAL_POSITION
        """
        try:
            # Make this section completely optional - if it fails, we'll still have the basic table info
            try:
                # Make sure database and schema context is still set
                if credentials.get('database'):
                    use_database_query = f"USE DATABASE {credentials.get('database')}"
                    connection_manager.execute_query(credentials, use_database_query)
                
                if credentials.get('schema'):
                    use_schema_query = f"USE SCHEMA {credentials.get('schema')}"
                    connection_manager.execute_query(credentials, use_schema_query)
                    
                # Execute the foreign key query
                fk_result = connection_manager.execute_query(credentials, fk_query)
                fk_data = fk_result.get('data', [])
                
                # Process the foreign key data
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
                                # Add to search text
                                schema_info[table_name]['search_text'] += f" foreign key {column_name} references {ref_table_name} {ref_column_name}"
            except Exception as e:
                print(f"Foreign key query failed, but continuing without foreign key information: {str(e)}")
                # We'll continue without foreign key information
        except Exception as e:
            print(f"Error in foreign key processing: {str(e)}")
            # Continue without foreign key information
            
        # If no tables found, log a warning but don't throw an error
        if not schema_info:
            print("Warning: No tables found in schema. The schema might be empty or you might not have access to it.")
            # Create a dummy table entry so the search can still function
            schema_info = {
                "DUMMY_TABLE": {
                    "table_type": "BASE TABLE",
                    "description": "No tables found in this schema. This is a placeholder.",
                    "columns": {},
                    "primary_keys": [],
                    "foreign_keys": [],
                    "row_count": 0,
                    "size_bytes": 0,
                    "created": "",
                    "last_altered": "",
                    "search_text": "No tables found in this schema. This is a placeholder."
                }
            }

        print(f"Retrieved metadata for {len(schema_info)} tables from INFORMATION_SCHEMA")
        return schema_info
    
    except Exception as e:
        print(f"Error getting schema information: {str(e)}")
        # Instead of raising the error, return a minimal schema info object
        return {
            "ERROR_TABLE": {
                "table_type": "BASE TABLE",
                "description": f"Error retrieving schema information: {str(e)}",
                "columns": {},
                "primary_keys": [],
                "foreign_keys": [],
                "row_count": 0,
                "size_bytes": 0,
                "created": "",
                "last_altered": "",
                "search_text": f"Error retrieving schema information: {str(e)}"
            }
        }

def search_tables_semantic(credentials: dict, query: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Perform semantic search across tables, columns, and metadata
    
    Args:
        credentials: Snowflake connection credentials
        query: Natural language search query
        filters: Optional filters to narrow results (database, schema, etc.)
        
    Returns:
        List of matching tables with relevance scores and metadata
    """
    try:
        # Get schema information
        schema_info = get_schema_information(credentials)
        
        # Generate query embedding
        query_embedding = generate_embeddings(query)
        
        # Expand business terms in the query
        expanded_terms = expand_business_terms(query)
        print(f"Expanded search terms: {expanded_terms}")
        
        # Calculate search results with relevance scores
        search_results = []
        
        for table_name, table_info in schema_info.items():
            # Generate embedding for table's search text
            table_search_text = table_info['search_text']
            table_embedding = generate_embeddings(table_search_text)
            
            # Calculate similarity score
            similarity = cosine_similarity(query_embedding, table_embedding)
            
            # Check for direct matches in expanded terms
            direct_match_score = 0.0
            for term in expanded_terms:
                term_lower = term.lower()
                
                # Check table name (highest priority)
                if term_lower in table_name.lower():
                    direct_match_score += 0.8
                
                # Check table description
                if table_info['description'] and term_lower in table_info['description'].lower():
                    direct_match_score += 0.6
                
                # Check column names and descriptions
                for col_name, col_info in table_info['columns'].items():
                    if term_lower in col_name.lower():
                        direct_match_score += 0.5
                    
                    if col_info['description'] and term_lower in col_info['description'].lower():
                        direct_match_score += 0.4
            
            # Normalize direct match score
            if direct_match_score > 0:
                direct_match_score = min(direct_match_score, 1.0)
            
            # Combine semantic similarity with direct match score
            # Weight: 60% semantic similarity, 40% direct match
            combined_score = (similarity * 0.6) + (direct_match_score * 0.4)
            
            # Only include results above threshold
            if combined_score >= SIMILARITY_THRESHOLD:
                # Create result object with all relevant metadata
                result = {
                    'table_name': table_name,
                    'database_name': credentials.get('database', ''),
                    'schema_name': credentials.get('schema', ''),
                    'description': table_info['description'],
                    'relevance_score': round(combined_score, 4),
                    'column_count': len(table_info['columns']),
                    'row_count': table_info['row_count'],
                    'size_bytes': table_info['size_bytes'],
                    'created': table_info['created'],
                    'last_altered': table_info['last_altered'],
                    'match_reasons': []
                }
                
                # Add match reasons for explainability
                if direct_match_score > 0:
                    for term in expanded_terms:
                        term_lower = term.lower()
                        
                        if term_lower in table_name.lower():
                            result['match_reasons'].append(f"Table name contains '{term}'")
                        
                        if table_info['description'] and term_lower in table_info['description'].lower():
                            result['match_reasons'].append(f"Table description contains '{term}'")
                        
                        matching_columns = []
                        for col_name, col_info in table_info['columns'].items():
                            if term_lower in col_name.lower():
                                matching_columns.append(col_name)
                        
                        if matching_columns:
                            result['match_reasons'].append(f"Columns contain '{term}': {', '.join(matching_columns[:3])}" + 
                                                         (f" and {len(matching_columns) - 3} more" if len(matching_columns) > 3 else ""))
                else:
                    result['match_reasons'].append("Semantically related to search terms")
                
                search_results.append(result)
        
        # Sort results by relevance score (descending)
        search_results.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        return search_results
        
    except Exception as e:
        print(f"Error in semantic search: {str(e)}")
        raise

@csrf_exempt
@require_http_methods(["POST"])
def semantic_search_endpoint(request):
    """
    Handle semantic search requests
    """
    try:
        data = json.loads(request.body)
        print(f"Semantic search request data: {data.keys()}")
        
        # Extract credentials and query
        credentials = {}
        query = data.get('query', '').strip()
        filters = data.get('filters', {})
        
        # Extract credentials from request
        for field in ['account', 'username', 'password', 'warehouse', 'role', 'database', 'schema']:
            if field in data:
                credentials[field] = data[field]
        
        print(f"Extracted query: {query}")
        print(f"Extracted credentials: {', '.join(credentials.keys())}")
        print(f"Extracted filters: {filters}")
        
        if not credentials:
            return JsonResponse({
                'status': 'error',
                'message': 'No Snowflake credentials provided'
            }, status=400)
            
        if not query:
            return JsonResponse({
                'status': 'error',
                'message': 'No search query provided'
            }, status=400)
            
        # Check for required credentials
        required_fields = ['account', 'username', 'password', 'warehouse']
        missing_fields = [field for field in required_fields if field not in credentials]
        
        if missing_fields:
            return JsonResponse({
                'status': 'error',
                'message': f'Missing required credentials: {", ".join(missing_fields)}'
            }, status=400)
            
        # Check if OpenAI is available for embeddings
        if not OPENAI_AVAILABLE:
            return JsonResponse({
                'status': 'error',
                'message': 'OpenAI is not available for semantic search. Please check your API key configuration.'
            }, status=500)
            
        # Execute the semantic search
        start_time = time.time()
        results = search_tables_semantic(credentials, query, filters)
        execution_time = time.time() - start_time
        
        return JsonResponse({
            'status': 'success',
            'result': {
                'query': query,
                'result_count': len(results),
                'execution_time': execution_time,
                'results': results
            }
        })
        
    except Exception as e:
        error_message = str(e)
        print(f"Error processing semantic search request: {error_message}")
        return JsonResponse({
            'status': 'error',
            'message': error_message
        }, status=500)