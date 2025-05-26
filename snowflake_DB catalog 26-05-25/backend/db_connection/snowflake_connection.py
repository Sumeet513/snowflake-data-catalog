import snowflake.connector
from contextlib import contextmanager

class SnowflakeConnection:
    """
    Handles Snowflake database connections with connection pooling and management
    """
    def __init__(self):
        self.connections = {}

    @contextmanager
    def get_connection(self, connection_params, save_details=True):
        """Create a dynamic connection using provided credentials"""
        conn_key = f"{connection_params['account']}_{connection_params.get('database', '')}"
        
        try:
            # Format account identifier if needed
            account = connection_params['account']
            if not any(char in account for char in ['-', '.']):
                account = f"{account}.ap-south-1"

            # Create connection with explicit role and timeouts
            connect_timeout = connection_params.get('connect_timeout', 30)
            login_timeout = connection_params.get('login_timeout', 60)
            
            # Configure session parameters for better performance
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
                account=account,
                user=connection_params['username'],
                password=connection_params['password'],
                warehouse=connection_params['warehouse'],
                role=connection_params.get('role', 'ACCOUNTADMIN'),  # Default to ACCOUNTADMIN
                database=connection_params.get('database'),
                schema=connection_params.get('schema'),
                login_timeout=login_timeout,  # Timeout for login
                network_timeout=connect_timeout,  # Timeout for network operations
                client_session_keep_alive=True,  # Keep session alive
                client_prefetch_threads=4,  # Use multiple threads for prefetching
                session_parameters=session_parameters
            )

            # Test connection and role
            cur = conn.cursor()
            try:
                cur.execute("SELECT CURRENT_ROLE()")
                current_role = cur.fetchone()[0]
                print(f"Connected successfully with role: {current_role}")
            except Exception as e:
                print(f"Error checking role: {str(e)}")
            finally:
                cur.close()
            self.connections[conn_key] = conn
            
            yield conn
            
        finally:
            if conn_key in self.connections:
                self.connections[conn_key].close()
                del self.connections[conn_key]

    @contextmanager
    def get_optimized_connection(self, connection_params):
        """Optimized connection manager that reuses connections"""
        conn_key = f"{connection_params['account']}_{connection_params['warehouse']}"
        
        if conn_key in self.connections:
            try:
                # Test if connection is still valid
                self.connections[conn_key].cursor().execute("SELECT 1")
                yield self.connections[conn_key]
                return
            except:
                # Connection expired, remove it
                del self.connections[conn_key]
        
        # Create new connection
        conn = self.get_connection(connection_params).__enter__()
        self.connections[conn_key] = conn
        try:
            yield conn
        finally:
            # Keep connection alive for reuse
            pass

    def execute_query(self, connection_params, query, params=None):
        """Execute a query using provided connection details"""
        with self.get_connection(connection_params) as conn:
            cur = conn.cursor()
            try:
                cur.execute(query, params)
                
                # For SELECT queries
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    results = cur.fetchall()
                    return {'columns': columns, 'data': results}
                
                # For INSERT, UPDATE, DELETE queries
                conn.commit()
                return {'affected_rows': cur.rowcount}
            finally:
                cur.close() 