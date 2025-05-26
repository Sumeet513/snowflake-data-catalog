import snowflake.connector
from contextlib import contextmanager

class SearchConnectionManager:
    """
    Handles Snowflake database connections for data discovery and search operations
    """
    def __init__(self):
        self.connections = {}

    @contextmanager
    def get_connection(self, connection_params):
        """Create a connection using provided credentials"""
        conn_key = f"{connection_params['account']}_{connection_params.get('database', '')}"
        
        try:
            # Format account identifier if needed
            account = connection_params['account']
            if '.snowflakecomputing.com' in account:
                # Extract the account identifier only
                account = account.replace('.snowflakecomputing.com', '')
            
            # Create connection with explicit role and timeouts
            connect_timeout = connection_params.get('connect_timeout', 30)
            login_timeout = connection_params.get('login_timeout', 60)
            
            # Configure session parameters for better search performance
            session_parameters = {
                # Disable client-side caching for faster retrieval
                'CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX': True,
                # Reduce metadata query timeouts
                'STATEMENT_TIMEOUT_IN_SECONDS': connection_params.get('query_timeout', 300),
                # Set working parameters
                'TIMESTAMP_OUTPUT_FORMAT': 'YYYY-MM-DD HH24:MI:SS.FF',
                'DATE_OUTPUT_FORMAT': 'YYYY-MM-DD'
            }

            conn = snowflake.connector.connect(
                user=connection_params['username'],
                password=connection_params['password'],
                account=account,
                warehouse=connection_params['warehouse'],
                role=connection_params.get('role', ''),
                database=connection_params.get('database'),
                schema=connection_params.get('schema'),
                login_timeout=login_timeout,
                network_timeout=connect_timeout,
                client_session_keep_alive=True,
                client_prefetch_threads=4,
                session_parameters=session_parameters
            )

            self.connections[conn_key] = conn
            yield conn
            
        except Exception as e:
            raise Exception(f"Error establishing connection: {str(e)}")
            
        finally:
            if conn_key in self.connections:
                try:
                    self.connections[conn_key].close()
                except:
                    pass
                del self.connections[conn_key]

    def execute_query(self, connection_params, query, params=None):
        """Execute a query using provided connection details"""
        with self.get_connection(connection_params) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                
                # For SELECT queries
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    return {
                        'columns': columns, 
                        'data': results,
                        'row_count': len(results)
                    }
                
                # For other queries
                conn.commit()
                return {'affected_rows': cursor.rowcount}
                
            finally:
                cursor.close() 