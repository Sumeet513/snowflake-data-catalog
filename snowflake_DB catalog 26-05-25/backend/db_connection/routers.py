import snowflake.connector
from contextlib import contextmanager

# in routers.py
class SnowflakeRouter:
    """Router to handle external Snowflake connections"""
    
    def db_for_read(self, model, **hints):
        if getattr(model, '_meta', None) and model._meta.app_label == 'db_connection':
            return 'snowflake'
        return None

    def db_for_write(self, model, **hints):
        # Prevent Django from writing to Snowflake through ORM
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return False

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        # Prevent migrations for db_connection app
        return False

class SnowflakeManager:
    def __init__(self):
        self.connections = {}
    
    @contextmanager
    def get_connection(self, connection_params):
        """Create a dynamic connection using provided credentials"""
        conn_key = f"{connection_params['account']}_{connection_params['database']}"
        
        try:
            conn = snowflake.connector.connect(
                account=connection_params['account'],
                user=connection_params['username'],
                password=connection_params['password'],
                warehouse=connection_params['warehouse'],
                database=connection_params['database'],
                schema=connection_params['schema'],
                role=connection_params.get('role')
            )
            self.connections[conn_key] = conn
            yield conn
            
        finally:
            if conn_key in self.connections:
                self.connections[conn_key].close()
                del self.connections[conn_key]

    def execute_query(self, connection_params, query, params=None):
        """Execute a query using provided connection details"""
        with self.get_connection(connection_params) as conn:
            cur = conn.cursor()
            try:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                results = cur.fetchall() if cur.description else []
                return {'columns': columns, 'data': results}
            finally:
                cur.close()