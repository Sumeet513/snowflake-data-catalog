import snowflake.connector
from django.conf import settings
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class SnowflakeService:
    def __init__(self):
        self.config = settings.SNOWFLAKE_CONFIG
        self.connection = None

    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query and return results as a list of dictionaries"""
        if not self.connection:
            if not self.connect():
                return []

        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [col[0] for col in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()

    def get_databases(self) -> List[Dict]:
        """Get all databases"""
        query = "SHOW DATABASES"
        return self.execute_query(query)

    def get_schemas(self, database: str) -> List[Dict]:
        """Get all schemas in a database"""
        query = f"SHOW SCHEMAS IN DATABASE {database}"
        return self.execute_query(query)

    def get_tables(self, database: str, schema: str) -> List[Dict]:
        """Get all tables in a schema"""
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        return self.execute_query(query)

    def get_table_columns(self, database: str, schema: str, table: str) -> List[Dict]:
        """Get all columns in a table"""
        query = f"DESCRIBE TABLE {database}.{schema}.{table}"
        columns_raw = self.execute_query(query)

# Fix: get columns dynamically
        columns = []
        for col in columns_raw:
            column_info = {
                "column_name": col.get("name"),
                "data_type": col.get("type"),
                "nullable": col.get("null?"),
                "default": col.get("default"),
                "primary_key": col.get("primary key"),
                "unique_key": col.get("unique key"),
            }
            columns.append(column_info)

        # Merge constraint information with column data
        if constraints:
            column_constraints = {}
            for constraint in constraints:
                column_name = constraint.get('COLUMN_NAME')
                if column_name:
                    if column_name not in column_constraints:
                        column_constraints[column_name] = []
                    column_constraints[column_name].append(constraint)
            
            for column in columns:
                column_name = column.get('column_name')
                if column_name and column_name in column_constraints:
                    column['constraints'] = column_constraints[column_name]

        return columns
    def get_table_constraints(self, database: str, schema: str, table: str) -> List[Dict]:
        """Get all constraints for a table from INFORMATION_SCHEMA"""
        try:
            # Try multiple approaches to get constraint information
            constraints = []
            
            # 1. First try with a simpler query for identity columns (often primary keys)
            try:
                identity_query = f"""
                SELECT
                    'PRIMARY KEY' as CONSTRAINT_TYPE,
                    COLUMN_NAME,
                    '{schema}' as CONSTRAINT_SCHEMA,
                    '{database}' as TABLE_CATALOG,
                    '{schema}' as TABLE_SCHEMA,
                    '{table}' as TABLE_NAME,
                    'PK_' || '{table}' || '_' || COLUMN_NAME as CONSTRAINT_NAME
                FROM
                    {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{schema}'
                    AND TABLE_NAME = '{table}'
                    AND IS_IDENTITY = 'YES'
                """
                identity_constraints = self.execute_query(identity_query)
                if identity_constraints:
                    constraints.extend(identity_constraints)
            except Exception as e1:
                logger.error(f"Error fetching identity constraints: {str(e1)}")
            
            # 2. Try to get primary key constraints based on column names
            try:
                pk_query = f"""
                SELECT
                    'PRIMARY KEY' as CONSTRAINT_TYPE,
                    COLUMN_NAME,
                    '{schema}' as CONSTRAINT_SCHEMA,
                    '{database}' as TABLE_CATALOG,
                    '{schema}' as TABLE_SCHEMA,
                    '{table}' as TABLE_NAME,
                    'PK_' || '{table}' || '_' || COLUMN_NAME as CONSTRAINT_NAME
                FROM
                    {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{schema}'
                    AND TABLE_NAME = '{table}'
                    AND COLUMN_NAME IN ('ID', '{table}_ID', '{table.rstrip("S")}_ID')
                    AND NULLABLE = 'N'
                """
                pk_constraints = self.execute_query(pk_query)
                if pk_constraints:
                    # Filter out any duplicates from the identity query
                    for pk in pk_constraints:
                        if not any(c.get('COLUMN_NAME') == pk.get('COLUMN_NAME') for c in constraints):
                            constraints.append(pk)
            except Exception as e2:
                logger.error(f"Error fetching primary key constraints: {str(e2)}")
            
            # 3. Try to get foreign key constraints based on column names
            try:
                fk_query = f"""
                SELECT
                    'FOREIGN KEY' as CONSTRAINT_TYPE,
                    COLUMN_NAME,
                    '{schema}' as CONSTRAINT_SCHEMA,
                    '{database}' as TABLE_CATALOG,
                    '{schema}' as TABLE_SCHEMA,
                    '{table}' as TABLE_NAME,
                    'FK_' || '{table}' || '_' || COLUMN_NAME as CONSTRAINT_NAME,
                    REGEXP_REPLACE(COLUMN_NAME, '_ID$', '') as REFERENCED_TABLE_NAME
                FROM
                    {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{schema}'
                    AND TABLE_NAME = '{table}'
                    AND COLUMN_NAME LIKE '%_ID'
                    AND COLUMN_NAME NOT IN ('ID', '{table}_ID', '{table.rstrip("S")}_ID')
                """
                fk_constraints = self.execute_query(fk_query)
                if fk_constraints:
                    for fk in fk_constraints:
                        if not any(c.get('COLUMN_NAME') == fk.get('COLUMN_NAME') for c in constraints):
                            constraints.append(fk)
            except Exception as e3:
                logger.error(f"Error fetching foreign key constraints: {str(e3)}")
            
            # 4. Try to get unique constraints based on column names
            try:
                unique_query = f"""
                SELECT
                    'UNIQUE' as CONSTRAINT_TYPE,
                    COLUMN_NAME,
                    '{schema}' as CONSTRAINT_SCHEMA,
                    '{database}' as TABLE_CATALOG,
                    '{schema}' as TABLE_SCHEMA,
                    '{table}' as TABLE_NAME,
                    'UQ_' || '{table}' || '_' || COLUMN_NAME as CONSTRAINT_NAME
                FROM
                    {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{schema}'
                    AND TABLE_NAME = '{table}'
                    AND COLUMN_NAME IN ('EMAIL', 'USERNAME', 'PHONE', 'SSN', 'LICENSE_NUMBER')
                """
                unique_constraints = self.execute_query(unique_query)
                if unique_constraints:
                    for uq in unique_constraints:
                        if not any(c.get('COLUMN_NAME') == uq.get('COLUMN_NAME') for c in constraints):
                            constraints.append(uq)
            except Exception as e4:
                logger.error(f"Error fetching unique constraints: {str(e4)}")
            
            # 5. Finally, try the standard INFORMATION_SCHEMA query if we have no constraints yet
            if not constraints:
                try:
                    full_query = f"""
                    SELECT
                        tc.CONSTRAINT_NAME,
                        tc.CONSTRAINT_TYPE,
                        kcu.COLUMN_NAME,
                        tc.CONSTRAINT_SCHEMA,
                        tc.TABLE_CATALOG,
                        tc.TABLE_SCHEMA,
                        tc.TABLE_NAME,
                        rc.UNIQUE_CONSTRAINT_NAME as REFERENCED_CONSTRAINT_NAME,
                        rc.UNIQUE_CONSTRAINT_SCHEMA as REFERENCED_CONSTRAINT_SCHEMA,
                        rc.UNIQUE_CONSTRAINT_CATALOG as REFERENCED_CONSTRAINT_CATALOG
                    FROM
                        {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    LEFT JOIN
                        {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                        ON tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG
                        AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                        AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    LEFT JOIN
                        {database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                        ON tc.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
                        AND tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                        AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    WHERE
                        tc.TABLE_SCHEMA = '{schema}'
                        AND tc.TABLE_NAME = '{table}'
                    ORDER BY
                        tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME
                    """
                    std_constraints = self.execute_query(full_query)
                    if std_constraints:
                        constraints.extend(std_constraints)
                except Exception as e5:
                    logger.error(f"Error fetching standard constraints: {str(e5)}")
            
            return constraints
        except Exception as e:
            logger.error(f"Error in get_table_constraints: {str(e)}")
            return []
        return self.execute_query(query)

    def close(self):
        """Close the Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None 