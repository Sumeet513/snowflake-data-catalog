import snowflake.connector
from ..models import SnowflakeConnection

class SnowflakeService:
    def __init__(self, connection_id=None, connection_data=None):
        self.connection = None
        self.snowflake_connection = None
        
        if connection_id:
            # Get connection details from database
            try:
                self.snowflake_connection = SnowflakeConnection.objects.get(id=connection_id, is_active=True)
            except SnowflakeConnection.DoesNotExist:
                raise ValueError(f"Connection with ID {connection_id} not found or inactive")
        elif connection_data:
            # Use provided connection data directly
            self.connection_data = connection_data
        else:
            raise ValueError("Either connection_id or connection_data must be provided")
        
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            if self.snowflake_connection:
                # Use saved connection details
                self.connection = snowflake.connector.connect(
                    user=self.snowflake_connection.username,
                    password=self.snowflake_connection.password,
                    account=self.snowflake_connection.account,
                    warehouse=self.snowflake_connection.warehouse,
                    database=self.snowflake_connection.database,
                    schema=self.snowflake_connection.schema
                )
            else:
                # Use provided connection details
                self.connection = snowflake.connector.connect(
                    user=self.connection_data.get('username'),
                    password=self.connection_data.get('password'),
                    account=self.connection_data.get('account'),
                    warehouse=self.connection_data.get('warehouse'),
                    database=self.connection_data.get('database'),
                    schema=self.connection_data.get('schema')
                )
            return True
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            return False
            
    def disconnect(self):
        """Close the Snowflake connection"""
        if self.connection:
            self.connection.close()
            
    def execute_query(self, query, params=None):
        """Execute a query and return the results"""
        if not self.connection:
            self.connect()
            
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            cursor.close()
            
            # Convert to list of dictionaries
            results_as_dicts = []
            for row in results:
                row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
                results_as_dicts.append(row_dict)
                
            return results_as_dicts
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return None