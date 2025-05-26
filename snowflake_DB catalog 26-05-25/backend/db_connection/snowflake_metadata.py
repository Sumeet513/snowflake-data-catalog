from .snowflake_connection import SnowflakeConnection
from django.core.cache import cache
import time
from datetime import datetime
import os
import sys
import json
from snowflake.connector.errors import DatabaseError, ProgrammingError
from typing import Dict, List, Any, Optional, Union, TypedDict, cast

# Type definitions for better type checking
class BusinessTerm(TypedDict, total=False):
    term_id: str
    term_name: str
    definition: str
    status: str  # draft, approved, deprecated
    steward: str  # person responsible for the term
    domain: str  # business domain/area this term belongs to
    synonyms: List[str]  # alternative names for the term
    related_terms: List[str]  # related business terms
    created_on: Optional[str]
    last_modified: Optional[str]
    last_modified_by: Optional[str]
    approved_by: Optional[str]
    approved_on: Optional[str]
    examples: Optional[str]  # example values
    source: Optional[str]  # source system/document

class Tag(TypedDict, total=False):
    tag_id: str
    tag_name: str
    tag_category: str  # e.g., PII, Regulatory, Business, Technical
    tag_description: Optional[str]
    created_on: Optional[str]
    created_by: Optional[str]
    is_active: bool

class TagAssignment(TypedDict, total=False):
    assignment_id: str
    tag_id: str
    tag_name: str
    object_type: str  # database, schema, table, column
    object_id: str  # full path to object
    assigned_on: Optional[str]
    assigned_by: Optional[str]
    assignment_note: Optional[str]

class LineageNode(TypedDict, total=False):
    node_id: str
    node_type: str  # table, view, procedure, query, job
    object_id: str  # reference to actual object
    object_name: str
    object_type: str  # table, view, etc.
    database_name: Optional[str]
    schema_name: Optional[str]

class LineageEdge(TypedDict, total=False):
    edge_id: str
    source_node_id: str
    target_node_id: str
    transformation_type: Optional[str]  # copy, join, aggregate, custom
    transformation_details: Optional[str]  # details of the transformation
    confidence_score: Optional[float]  # how confident we are about this relationship
    created_on: Optional[str]
    last_modified: Optional[str]
    is_active: bool

class ProfileStats(TypedDict, total=False):
    column_id: str
    column_name: str
    table_id: str
    table_name: str
    schema_name: str
    database_name: str
    profiling_date: str
    row_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    min_value: Optional[str]
    max_value: Optional[str]
    avg_value: Optional[float]
    median_value: Optional[float]
    min_length: Optional[int]
    max_length: Optional[int]
    avg_length: Optional[float]
    histogram: Optional[str]  # JSON string with histogram data
    statistical_type: Optional[str]  # inferred type (categorical, continuous, etc.)
    patterns: Optional[str]  # common patterns in the data
    outliers_count: Optional[int]
    potential_issues: Optional[str]  # JSON string with detected data quality issues

class DatabaseDict(TypedDict, total=False):
    database_id: str
    database_name: str
    database_owner: Optional[str]
    database_type: Optional[str]  # Added: Oracle, SQL Server, PostgreSQL, etc.
    server_location: Optional[str]  # Added: location of the server
    connection_details: Optional[str]  # Added: encrypted connection details
    environment: Optional[str]  # Added: dev, test, prod
    version: Optional[str]  # Added: database version
    created_on: Optional[str]
    last_altered: Optional[str]
    comment: Optional[str]
    business_terms: List[str]  # List of associated business term IDs
    tags: List[str]  # List of associated tag IDs
    schemas: List['SchemaDict']

class SchemaDict(TypedDict, total=False):
    schema_id: str
    schema_name: str
    database_name: Optional[str]
    database_id: Optional[str]  # Added: reference to parent database
    schema_owner: Optional[str]
    created_on: Optional[str]
    last_altered: Optional[str]
    comment: Optional[str]
    business_terms: List[str]  # List of associated business term IDs
    tags: List[str]  # List of associated tag IDs
    tables: List['TableDict']

class TableDict(TypedDict, total=False):
    table_id: str
    table_name: str
    schema_name: Optional[str]
    schema_id: Optional[str]  # Added: reference to parent schema
    database_name: Optional[str]
    database_id: Optional[str]  # Added: reference to parent database
    table_type: str
    table_owner: Optional[str]
    row_count: Optional[int]
    byte_size: Optional[int]
    created_on: Optional[str]
    last_altered: Optional[str]
    refresh_frequency: Optional[str]  # Added: how often the table is refreshed
    is_sensitive: Optional[bool]  # Added: whether table contains sensitive data
    comment: Optional[str]
    business_terms: List[str]  # List of associated business term IDs
    tags: List[str]  # List of associated tag IDs
    source_lineage: List[str]  # List of source node IDs (where this data comes from)
    target_lineage: List[str]  # List of target node IDs (where this data goes to)
    profile_summary: Optional[Dict[str, Any]]  # Summary of profile statistics
    columns: List['ColumnDict']

class ColumnDict(TypedDict, total=False):
    name: str
    column_id: str  # Added: unique identifier for the column
    column_name: str  # Added: renamed from name for consistency
    table_id: Optional[str]  # Added: reference to parent table
    table_name: Optional[str]  # Added: reference to parent table
    schema_id: Optional[str]  # Added: reference to parent schema
    schema_name: Optional[str]  # Added: reference to parent schema
    database_id: Optional[str]  # Added: reference to parent database
    database_name: Optional[str]  # Added: reference to parent database
    type: str
    data_type: str  # Added: renamed from type for consistency
    nullable: str
    is_nullable: bool  # Added: boolean version of nullable
    default: Optional[str]
    column_default: Optional[str]  # Added: renamed from default for consistency
    max_length: Optional[int]  # Added: character maximum length
    character_maximum_length: Optional[int]  # Added: alternate name for max_length
    precision: Optional[int]  # Added: numeric precision
    numeric_precision: Optional[int]  # Added: alternate name for precision
    scale: Optional[int]  # Added: numeric scale
    numeric_scale: Optional[int]  # Added: alternate name for scale
    ordinal_position: Optional[int]  # Added: position of column in table
    is_primary_key: Optional[bool]  # Added: whether column is a primary key
    is_foreign_key: Optional[bool]  # Added: whether column is a foreign key
    referenced_table: Optional[str]  # Added: for foreign keys
    referenced_column: Optional[str]  # Added: for foreign keys
    is_pii: Optional[bool]  # Added: whether column contains PII
    sensitivity_level: Optional[str]  # Added: classification of data sensitivity
    business_description: Optional[str]  # Business-friendly description
    business_terms: List[str]  # List of associated business term IDs
    tags: List[str]  # List of associated tag IDs
    source_lineage: List[str]  # List of source column IDs (where this data comes from)
    profile_stats: Optional[str]  # Reference to profile statistics (or embedded)
    comment: Optional[str]
    distinct_values: Optional[int]  # Added: number of distinct values
    row_count: Optional[int]  # Added: total number of rows

class MetadataResult(TypedDict):
    status: str
    message: Optional[str]
    databases: List[DatabaseDict]
    schemas: List[SchemaDict]
    tables: List[TableDict]
    columns: List[ColumnDict]
    business_terms: List[BusinessTerm]
    tags: List[Tag]
    tag_assignments: List[TagAssignment]
    lineage_nodes: List[LineageNode]
    lineage_edges: List[LineageEdge]
    profile_stats: List[ProfileStats]
    count: int

class SnowflakeMetadata:
    """
    Handles retrieval and processing of Snowflake metadata directly from INFORMATION_SCHEMA
    instead of creating a separate SNOWFLAKE_CATALOG database.
    """
    
    def __init__(self):
        """Initialize the metadata manager"""
        self.connection = SnowflakeConnection()
    
    def get_database_metadata(self, cursor, database_name=None):
        """
        Get metadata for a database directly from INFORMATION_SCHEMA
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional name of database to get metadata for
            
        Returns:
            Dictionary with database metadata
        """
        try:
            # Get database information from INFORMATION_SCHEMA
            if database_name:
                cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            else:
                cursor.execute("SHOW DATABASES")
                
            databases = []
            for row in cursor.fetchall():
                db_name = row[1]  # Database name is in second column
                
                # Skip system databases
                if db_name in ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA']:
                    continue
                    
                # Safe timestamp conversion
                def safe_timestamp(value):
                    if value is None:
                        return None
                    try:
                        return value.isoformat() if hasattr(value, 'isoformat') else str(value)
                    except:
                        return str(value)
                
                # Try to get database version
                version = None
                try:
                    cursor.execute(f"SELECT CURRENT_VERSION() AS VERSION")
                    version_row = cursor.fetchone()
                    if version_row:
                        version = version_row[0]
                except Exception as e:
                    print(f"Error getting database version: {str(e)}")
                
                databases.append({
                    'database_id': db_name,
                    'database_name': db_name,
                    'database_owner': row[5] if len(row) > 5 else None,
                    'database_type': 'Snowflake',  # Hardcoded for Snowflake
                    'server_location': row[4] if len(row) > 4 else None,  # Region
                    'version': version,
                    'environment': None,  # Need to determine environment from naming convention or parameters
                    'connection_details': None,  # This would be encrypted separately
                    'created_on': safe_timestamp(row[6]) if len(row) > 6 else None,
                    'last_altered': safe_timestamp(row[7]) if len(row) > 7 else None,
                    'comment': row[9] if len(row) > 9 else None
                })
                
            return {
                'status': 'success',
                'databases': databases,
                'count': len(databases)
            }
                
        except Exception as e:
            print(f"Error getting database metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_schema_metadata(self, cursor, database_name=None, schema_name=None):
        """
        Get metadata for schemas directly from INFORMATION_SCHEMA
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional name of database to get schemas for
            schema_name: Optional name of schema to filter by
            
        Returns:
            Dictionary with schema metadata
        """
        try:
            # Use the specified database if provided
            if database_name:
                cursor.execute(f"USE DATABASE {database_name}")
            
            # Get schema information
            if schema_name:
                cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name}'")
            else:
                cursor.execute("SHOW SCHEMAS")
                
            schemas = []
            for row in cursor.fetchall():
                schema_name = row[1]  # Schema name is in second column
                
                # Skip system schemas
                if schema_name in ['INFORMATION_SCHEMA']:
                    continue
                    
                # Safe timestamp conversion
                def safe_timestamp(value):
                    if value is None:
                        return None
                    try:
                        return value.isoformat() if hasattr(value, 'isoformat') else str(value)
                    except:
                        return str(value)
                
                schemas.append({
                    'schema_id': f"{database_name}.{schema_name}" if database_name else schema_name,
                    'schema_name': schema_name,
                    'database_name': database_name if database_name else row[3] if len(row) > 3 else None,
                    'schema_owner': row[5] if len(row) > 5 else None,
                    'created_on': safe_timestamp(row[6]) if len(row) > 6 else None,
                    'last_altered': safe_timestamp(row[7]) if len(row) > 7 else None,
                    'comment': row[9] if len(row) > 9 else None
                })
                
            return {
                'status': 'success',
                'schemas': schemas,
                'count': len(schemas)
            }
                
        except Exception as e:
            print(f"Error getting schema metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_table_metadata(self, cursor, database_name=None, schema_name=None, table_name=None):
        """
        Get metadata for tables directly from INFORMATION_SCHEMA
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional name of database
            schema_name: Optional name of schema
            table_name: Optional name of table to filter by
            
        Returns:
            Dictionary with table metadata
        """
        try:
            # Use the specified database and schema if provided
            if database_name:
                cursor.execute(f"USE DATABASE {database_name}")
            if schema_name:
                cursor.execute(f"USE SCHEMA {schema_name}")
            
            # Get table information
            if table_name:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            else:
                cursor.execute("SHOW TABLES")
                
            tables = []
            for row in cursor.fetchall():
                curr_table_name = row[1]  # Table name is in second column
                table_type = row[2] if len(row) > 2 else "TABLE"
                    
                # Safe timestamp conversion
                def safe_timestamp(value):
                    if value is None:
                        return None
                    try:
                        return value.isoformat() if hasattr(value, 'isoformat') else str(value)
                    except:
                        return str(value)
                
                # Try to get row count if available
                row_count = row[3] if len(row) > 3 else None
                byte_size = row[4] if len(row) > 4 else None
                
                # Check if table has sensitive data - looking for tags or column names suggesting PII
                is_sensitive = False
                
                # Try to get table tags if the feature is available
                try:
                    if database_name and schema_name:
                        cursor.execute(f"""
                        SELECT TAG_NAME, TAG_VALUE 
                        FROM TABLE({database_name}.INFORMATION_SCHEMA.TAG_REFERENCES(
                            'TABLE', '{database_name}.{schema_name}.{curr_table_name}'
                        ))
                        """)
                        
                        tags = cursor.fetchall()
                        for tag in tags:
                            # Look for tags indicating sensitive data
                            if tag and len(tag) >= 2:
                                tag_name = tag[0].upper() if tag[0] else ""
                                tag_value = tag[1].upper() if tag[1] else ""
                                
                                if ('PII' in tag_name or 'SENSITIVE' in tag_name or 
                                    'PERSONAL' in tag_name or 'CONFIDENTIAL' in tag_name or
                                    'PII' in tag_value or 'SENSITIVE' in tag_value or 
                                    'PERSONAL' in tag_value or 'CONFIDENTIAL' in tag_value):
                                    is_sensitive = True
                                    break
                except Exception as e:
                    print(f"Error getting table tags: {str(e)}")
                
                # Get refresh frequency from table comment if available
                refresh_frequency = None
                comment = row[9] if len(row) > 9 else None
                
                if comment:
                    # Look for refresh frequency pattern in comment like "Refresh: Daily" or "Frequency: Weekly"
                    import re
                    match = re.search(r'(?:refresh|frequency)[:\s]+(\w+)', comment, re.IGNORECASE)
                    if match:
                        refresh_frequency = match.group(1)
                
                tables.append({
                    'table_id': f"{database_name}.{schema_name}.{curr_table_name}" if database_name and schema_name else curr_table_name,
                    'table_name': curr_table_name,
                    'schema_name': schema_name if schema_name else row[3] if len(row) > 3 else None,
                    'schema_id': f"{database_name}.{schema_name}" if database_name and schema_name else None,
                    'database_name': database_name if database_name else row[2] if len(row) > 2 else None,
                    'database_id': database_name if database_name else None,
                    'table_type': table_type,
                    'table_owner': row[5] if len(row) > 5 else None,
                    'row_count': row_count,
                    'byte_size': byte_size,
                    'created_on': safe_timestamp(row[6]) if len(row) > 6 else None,
                    'last_altered': safe_timestamp(row[7]) if len(row) > 7 else None,
                    'refresh_frequency': refresh_frequency,
                    'is_sensitive': is_sensitive,
                    'comment': comment
                })
                
            return {
                'status': 'success',
                'tables': tables,
                'count': len(tables)
            }
                
        except Exception as e:
            print(f"Error getting table metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_column_metadata(self, cursor, database_name=None, schema_name=None, table_name=None):
        """
        Get metadata for columns directly from INFORMATION_SCHEMA
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional name of database
            schema_name: Optional name of schema
            table_name: Optional name of table
            
        Returns:
            Dictionary with column metadata
        """
        try:
            # Use the specified database and schema if provided
            if database_name:
                cursor.execute(f"USE DATABASE {database_name}")
            if schema_name:
                cursor.execute(f"USE SCHEMA {schema_name}")
            
            # Build filter conditions for INFORMATION_SCHEMA query
            filters = []
            params = []
            
            if database_name:
                filters.append("TABLE_CATALOG = %s")
                params.append(database_name)
            if schema_name:
                filters.append("TABLE_SCHEMA = %s")
                params.append(schema_name)
            if table_name:
                filters.append("TABLE_NAME = %s")
                params.append(table_name)
                
            where_clause = " AND ".join(filters) if filters else ""
            if where_clause:
                where_clause = f"WHERE {where_clause}"
            
            # Query INFORMATION_SCHEMA for column information
            query = f"""
            SELECT 
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                ORDINAL_POSITION,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COMMENT
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            {where_clause}
            ORDER BY 
                TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
            
            cursor.execute(query, params)
            
            columns = []
            for row in cursor.fetchall():
                db_name = row[0]
                schema = row[1]
                tbl_name = row[2]
                col_name = row[3]
                
                columns.append({
                    'column_id': f"{db_name}.{schema}.{tbl_name}.{col_name}",
                    'column_name': col_name,
                    'table_name': tbl_name,
                    'schema_name': schema,
                    'database_name': db_name,
                    'ordinal_position': row[4],
                    'data_type': row[5],
                    'is_nullable': row[6] == 'YES',
                    'column_default': row[7],
                    'character_maximum_length': row[8],
                    'numeric_precision': row[9],
                    'numeric_scale': row[10],
                    'comment': row[11]
                })
                
            # Get primary key information
            try:
                pk_query = f"""
                SELECT 
                    tc.TABLE_CATALOG,
                    tc.TABLE_SCHEMA,
                    tc.TABLE_NAME,
                    ccu.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN 
                    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                    ON tc.CONSTRAINT_CATALOG = ccu.CONSTRAINT_CATALOG
                    AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
                    AND tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    {' AND tc.TABLE_CATALOG = %s' if database_name else ''}
                    {' AND tc.TABLE_SCHEMA = %s' if schema_name else ''}
                    {' AND tc.TABLE_NAME = %s' if table_name else ''}
                ORDER BY 
                    tc.TABLE_CATALOG, tc.TABLE_SCHEMA, tc.TABLE_NAME, ccu.ORDINAL_POSITION
                """
                
                pk_params = []
                if database_name:
                    pk_params.append(database_name)
                if schema_name:
                    pk_params.append(schema_name)
                if table_name:
                    pk_params.append(table_name)
                
                cursor.execute(pk_query, pk_params)
                
                for row in cursor.fetchall():
                    db_name = row[0]
                    schema = row[1]
                    tbl_name = row[2]
                    col_name = row[3]
                    
                    # Find the column in our list
                    for col in columns:
                        if (col['database_name'] == db_name and 
                            col['schema_name'] == schema and 
                            col['table_name'] == tbl_name and 
                            col['column_name'] == col_name):
                            col['is_primary_key'] = True
                            break
            except Exception as pk_error:
                print(f"Error getting primary key information: {str(pk_error)}")
            
            # Get foreign key information
            try:
                fk_query = f"""
                SELECT 
                    tc.TABLE_CATALOG,
                    tc.TABLE_SCHEMA,
                    tc.TABLE_NAME,
                    ccu.COLUMN_NAME,
                    rc.REFERENCED_TABLE_CATALOG,
                    rc.REFERENCED_TABLE_SCHEMA,
                    rc.REFERENCED_TABLE_NAME,
                    rc.REFERENCED_COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN 
                    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                    ON tc.CONSTRAINT_CATALOG = ccu.CONSTRAINT_CATALOG
                    AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
                    AND tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                JOIN 
                    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                    ON tc.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
                    AND tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                    AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                    {' AND tc.TABLE_CATALOG = %s' if database_name else ''}
                    {' AND tc.TABLE_SCHEMA = %s' if schema_name else ''}
                    {' AND tc.TABLE_NAME = %s' if table_name else ''}
                ORDER BY 
                    tc.TABLE_CATALOG, tc.TABLE_SCHEMA, tc.TABLE_NAME, ccu.ORDINAL_POSITION
                """
                
                fk_params = []
                if database_name:
                    fk_params.append(database_name)
                if schema_name:
                    fk_params.append(schema_name)
                if table_name:
                    fk_params.append(table_name)
                
                cursor.execute(fk_query, fk_params)
                
                for row in cursor.fetchall():
                    db_name = row[0]
                    schema = row[1]
                    tbl_name = row[2]
                    col_name = row[3]
                    ref_db = row[4]
                    ref_schema = row[5]
                    ref_table = row[6]
                    ref_column = row[7]
                    
                    # Find the column in our list
                    for col in columns:
                        if (col['database_name'] == db_name and 
                            col['schema_name'] == schema and 
                            col['table_name'] == tbl_name and 
                            col['column_name'] == col_name):
                            col['is_foreign_key'] = True
                            col['referenced_table'] = f"{ref_db}.{ref_schema}.{ref_table}"
                            col['referenced_column'] = ref_column
                            break
            except Exception as fk_error:
                print(f"Error getting foreign key information: {str(fk_error)}")
                
            return {
                'status': 'success',
                'columns': columns,
                'count': len(columns)
            }
                
        except Exception as e:
            print(f"Error getting column metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_complete_metadata(self, cursor, database_name=None, schema_name=None) -> Dict[str, Any]:
        """
        Get complete metadata (databases, schemas, tables, columns) from INFORMATION_SCHEMA
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional name of database to filter by
            schema_name: Optional name of schema to filter by
            
        Returns:
            Dictionary with complete metadata
        """
        try:
            # Get database metadata
            db_metadata = self.get_database_metadata(cursor, database_name)
            if db_metadata['status'] != 'success':
                return db_metadata
                
            databases = db_metadata['databases']
            
            # Process each database
            for db in databases:
                db_dict = cast(DatabaseDict, db)
                db_name = db_dict.get('database_name')
                
                # Get schema metadata for this database
                schema_metadata = self.get_schema_metadata(cursor, db_name, schema_name)
                if schema_metadata['status'] != 'success':
                    continue
                
                db_dict['schemas'] = schema_metadata['schemas']
                
                # Process each schema
                schemas = db_dict.get('schemas', [])
                for schema in schemas:
                    schema_dict = cast(SchemaDict, schema)
                    schema_name = schema_dict.get('schema_name')
                    
                    # Get table metadata for this schema
                    table_metadata = self.get_table_metadata(cursor, db_name, schema_name)
                    if table_metadata['status'] != 'success':
                        continue
                        
                    schema_dict['tables'] = table_metadata['tables']
                    
                    # Process each table
                    tables = schema_dict.get('tables', [])
                    for table in tables:
                        table_dict = cast(TableDict, table)
                        table_name = table_dict.get('table_name')
                        
                        # Get column metadata for this table
                        column_metadata = self.get_column_metadata(cursor, db_name, schema_name, table_name)
                        if column_metadata['status'] != 'success':
                            continue
                            
                        table_dict['columns'] = column_metadata['columns']
            
            return {
                'status': 'success',
                'databases': databases,
                'count': len(databases)
            }
                
        except Exception as e:
            print(f"Error getting complete metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    def create_metadata_tables(self, cur):
        # Function is no longer needed since we're not creating catalog tables
        pass

    def collect_metadata(self, connection, database_name=None, schema_name=None, timeout: int = 3600) -> Dict[str, Any]:
        """
        Collect metadata from Snowflake database
        
        Args:
            connection: Active Snowflake connection
            database_name: Optional name of database to filter by
            schema_name: Optional name of schema to filter by
            timeout: Maximum execution time in seconds
            
        Returns:
            Dictionary with metadata information
        """
        result: Dict[str, Any] = {
            'status': 'success',
            'databases': [],
            'schemas': [],
            'tables': [],
            'columns': [],
            'business_terms': [],
            'tags': [],
            'tag_assignments': [],
            'lineage_nodes': [],
            'lineage_edges': [],
            'profile_stats': [],
            'count': 0
        }
        
        start_time = time.time()
        
        try:
            with self.connection.get_connection(connection) as conn:
                cur = conn.cursor()
                
                # Get complete metadata
                complete_metadata = self.get_complete_metadata(cur, database_name, schema_name)
                
                if complete_metadata['status'] != 'success':
                    result['status'] = 'error'
                    result['message'] = complete_metadata.get('message', 'Unknown error')
                    return result
                
                databases = complete_metadata['databases']
                
                result['database_count'] = len(databases)
                
                # Calculate schema, table, and column counts
                schema_count = 0
                table_count = 0
                column_count = 0
                
                for db in databases:
                    db_dict = cast(DatabaseDict, db)
                    if 'schemas' in db_dict:
                        schemas = db_dict.get('schemas', [])
                        schema_count += len(schemas)
                        
                        for schema in schemas:
                            schema_dict = cast(SchemaDict, schema)
                            if 'tables' in schema_dict:
                                tables = schema_dict.get('tables', [])
                                table_count += len(tables)
                                
                                for table in tables:
                                    table_dict = cast(TableDict, table)
                                    if 'columns' in table_dict:
                                        column_count += len(table_dict.get('columns', []))
                
                result['schema_count'] = schema_count
                result['table_count'] = table_count
                result['column_count'] = column_count
                
                # Collect business glossary, tags, and lineage information
                try:
                    # Try to collect business glossary terms if available
                    business_terms = self.get_business_terms(cur)
                    if business_terms and business_terms.get('status') == 'success':
                        result['business_terms'] = business_terms.get('terms', [])
                        print(f"Collected {len(business_terms.get('terms', []))} business terms")
                except Exception as e:
                    print(f"Error collecting business terms: {str(e)}")
                
                try:
                    # Try to collect tags if available
                    tags = self.get_tags(cur)
                    if tags and tags.get('status') == 'success':
                        result['tags'] = tags.get('tags', [])
                        print(f"Collected {len(tags.get('tags', []))} tags")
                        
                    # Try to collect tag assignments if available
                    tag_assignments = self.get_tag_assignments(cur)
                    if tag_assignments and tag_assignments.get('status') == 'success':
                        result['tag_assignments'] = tag_assignments.get('assignments', [])
                        print(f"Collected {len(tag_assignments.get('assignments', []))} tag assignments")
                except Exception as e:
                    print(f"Error collecting tags: {str(e)}")
                
                try:
                    # Try to collect lineage information if available
                    lineage = self.get_lineage_information(cur)
                    if lineage and lineage.get('status') == 'success':
                        result['lineage_nodes'] = lineage.get('nodes', [])
                        result['lineage_edges'] = lineage.get('edges', [])
                        print(f"Collected {len(lineage.get('nodes', []))} lineage nodes and {len(lineage.get('edges', []))} lineage edges")
                except Exception as e:
                    print(f"Error collecting lineage information: {str(e)}")
                
                # Try to associate business terms, tags, and lineage with databases, schemas, tables, and columns
                try:
                    if result['tag_assignments']:
                        self._associate_tags_with_objects(result)
                    
                    if result['business_terms']:
                        self._associate_business_terms_with_objects(result)
                    
                    if result['lineage_nodes'] and result['lineage_edges']:
                        self._associate_lineage_with_objects(result)
                except Exception as e:
                    print(f"Error associating metadata: {str(e)}")
                
                # Collect profile statistics for columns
                try:
                    profile_stats = self.get_profile_stats(cur)
                    if profile_stats and profile_stats.get('status') == 'success':
                        result['profile_stats'] = profile_stats.get('stats', [])
                        print(f"Collected {len(profile_stats.get('stats', []))} column profile statistics")
                        
                        # Associate profile stats with columns
                        self._associate_profile_stats_with_columns(result)
                except Exception as e:
                    print(f"Error collecting profile statistics: {str(e)}")
                
                for db in databases:
                    if time.time() - start_time > timeout:
                        result['status'] = 'timeout'
                        result['message'] = f"Metadata collection timed out after {timeout} seconds."
                        break
                    
                    # The rest of the processing is already done
                    
            return result
                    
        except Exception as e:
            print(f"Error collecting metadata: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'databases': [],
                'schemas': [],
                'tables': [],
                'columns': [],
                'business_terms': [],
                'tags': [],
                'tag_assignments': [],
                'lineage_nodes': [],
                'lineage_edges': [],
                'profile_stats': [],
                'count': 0
            }
    
    def _associate_tags_with_objects(self, result: Dict[str, Any]) -> None:
        """
        Associate tags with databases, schemas, tables, and columns
        
        Args:
            result: The metadata result dictionary
        """
        # Create lookup maps for tags
        tag_map = {tag['tag_id']: tag for tag in result.get('tags', [])}
        
        # Process tag assignments
        for assignment in result.get('tag_assignments', []):
            tag_id = assignment.get('tag_id')
            object_type = assignment.get('object_type', '').lower()
            object_id = assignment.get('object_id')
            
            if not tag_id or not object_type or not object_id:
                continue
                
            # Find the object and add the tag to it
            if object_type == 'database':
                for db in result.get('databases', []):
                    if db.get('database_id') == object_id:
                        if 'tags' not in db:
                            db['tags'] = []
                        db['tags'].append(tag_id)
                        break
            
            elif object_type == 'schema':
                for db in result.get('databases', []):
                    for schema in db.get('schemas', []):
                        if schema.get('schema_id') == object_id:
                            if 'tags' not in schema:
                                schema['tags'] = []
                            schema['tags'].append(tag_id)
                            break
            
            elif object_type == 'table':
                for db in result.get('databases', []):
                    for schema in db.get('schemas', []):
                        for table in schema.get('tables', []):
                            if table.get('table_id') == object_id:
                                if 'tags' not in table:
                                    table['tags'] = []
                                table['tags'].append(tag_id)
                                break
            
            elif object_type == 'column':
                for db in result.get('databases', []):
                    for schema in db.get('schemas', []):
                        for table in schema.get('tables', []):
                            for column in table.get('columns', []):
                                if column.get('column_id') == object_id:
                                    if 'tags' not in column:
                                        column['tags'] = []
                                    column['tags'].append(tag_id)
                                    break
    
    def _associate_business_terms_with_objects(self, result: Dict[str, Any]) -> None:
        """
        Associate business terms with databases, schemas, tables, and columns
        
        Args:
            result: The metadata result dictionary
        """
        # This function would use a business_term_assignments table or similar
        # For now just simulate with a simple lookup based on name matching
        
        # Create term lookup by name (case insensitive)
        term_map = {term['term_name'].lower(): term['term_id'] 
                   for term in result.get('business_terms', [])}
        
        # Look for term references in comments and descriptions
        for db in result.get('databases', []):
            comment = db.get('comment', '').lower() if db.get('comment') else ''
            self._check_and_add_terms(db, comment, term_map)
            
            for schema in db.get('schemas', []):
                comment = schema.get('comment', '').lower() if schema.get('comment') else ''
                self._check_and_add_terms(schema, comment, term_map)
                
                for table in schema.get('tables', []):
                    comment = table.get('comment', '').lower() if table.get('comment') else ''
                    self._check_and_add_terms(table, comment, term_map)
                    
                    for column in table.get('columns', []):
                        comment = column.get('comment', '').lower() if column.get('comment') else ''
                        business_desc = column.get('business_description', '').lower() if column.get('business_description') else ''
                        self._check_and_add_terms(column, comment + " " + business_desc, term_map)
    
    def _check_and_add_terms(self, obj: Dict[str, Any], text: str, term_map: Dict[str, str]) -> None:
        """
        Check text for term references and add them to the object
        
        Args:
            obj: The object to add terms to
            text: The text to check for term references
            term_map: The term lookup map
        """
        if not text:
            return
            
        if 'business_terms' not in obj:
            obj['business_terms'] = []
            
        for term_name, term_id in term_map.items():
            if term_name in text and term_id not in obj['business_terms']:
                obj['business_terms'].append(term_id)
    
    def _associate_lineage_with_objects(self, result: Dict[str, Any]) -> None:
        """
        Associate lineage information with tables and columns
        
        Args:
            result: The metadata result dictionary
        """
        # Create node lookup by object_id
        node_map = {node['object_id']: node['node_id'] 
                   for node in result.get('lineage_nodes', [])}
        
        # Create source and target lineage maps
        source_lineage = {}  # target_node_id -> [source_node_ids]
        target_lineage = {}  # source_node_id -> [target_node_ids]
        
        for edge in result.get('lineage_edges', []):
            source_id = edge['source_node_id']
            target_id = edge['target_node_id']
            
            if target_id not in source_lineage:
                source_lineage[target_id] = []
            source_lineage[target_id].append(source_id)
            
            if source_id not in target_lineage:
                target_lineage[source_id] = []
            target_lineage[source_id].append(target_id)
        
        # Assign lineage to tables
        for db in result.get('databases', []):
            for schema in db.get('schemas', []):
                for table in schema.get('tables', []):
                    table_id = table.get('table_id')
                    if table_id in node_map:
                        node_id = node_map[table_id]
                        
                        # Get source lineage
                        if node_id in source_lineage:
                            table['source_lineage'] = source_lineage[node_id]
                        else:
                            table['source_lineage'] = []
                            
                        # Get target lineage
                        if node_id in target_lineage:
                            table['target_lineage'] = target_lineage[node_id]
                        else:
                            table['target_lineage'] = []
    
    def _associate_profile_stats_with_columns(self, result: Dict[str, Any]) -> None:
        """
        Associate profile statistics with columns
        
        Args:
            result: The metadata result dictionary
        """
        # Create stats lookup by column_id
        stats_by_column = {}
        for stats in result.get('profile_stats', []):
            column_id = stats.get('column_id')
            if column_id:
                if column_id not in stats_by_column:
                    stats_by_column[column_id] = []
                stats_by_column[column_id].append(stats)
        
        # Assign profile stats to columns
        for db in result.get('databases', []):
            for schema in db.get('schemas', []):
                for table in schema.get('tables', []):
                    table_profile_summary = {
                        'row_count': 0,
                        'total_columns': 0,
                        'pii_columns': 0,
                        'profiling_date': None
                    }
                    
                    for column in table.get('columns', []):
                        column_id = column.get('column_id')
                        if column_id and column_id in stats_by_column:
                            # Get most recent profile stats
                            sorted_stats = sorted(
                                stats_by_column[column_id],
                                key=lambda x: x.get('profiling_date', ''),
                                reverse=True
                            )
                            if sorted_stats:
                                column['profile_stats'] = sorted_stats[0]
                                
                                # Update table profile summary
                                if table_profile_summary['row_count'] == 0:
                                    table_profile_summary['row_count'] = sorted_stats[0].get('row_count', 0)
                                    
                                table_profile_summary['total_columns'] += 1
                                
                                if column.get('is_pii'):
                                    table_profile_summary['pii_columns'] += 1
                                    
                                if not table_profile_summary['profiling_date'] or (
                                    sorted_stats[0].get('profiling_date', '') > table_profile_summary['profiling_date']
                                ):
                                    table_profile_summary['profiling_date'] = sorted_stats[0].get('profiling_date')
                    
                    # Add profile summary to table
                    if table_profile_summary['total_columns'] > 0:
                        table['profile_summary'] = table_profile_summary

    def get_business_terms(self, cursor) -> Dict[str, Any]:
        """
        Get business glossary terms from a dedicated table or schema
        
        Args:
            cursor: Active Snowflake cursor
            
        Returns:
            Dictionary with business terms
        """
        try:
            # Check if business glossary table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'BUSINESS_GLOSSARY'
            LIMIT 1
            """)
            
            if not cursor.fetchone():
                # Table doesn't exist, return empty results
                return {
                    'status': 'success',
                    'terms': [],
                    'count': 0
                }
            
            # Query business glossary table
            cursor.execute("""
            SELECT 
                TERM_ID,
                TERM_NAME,
                DEFINITION,
                STATUS,
                STEWARD,
                DOMAIN,
                SYNONYMS,
                RELATED_TERMS,
                CREATED_ON,
                LAST_MODIFIED,
                LAST_MODIFIED_BY,
                APPROVED_BY,
                APPROVED_ON,
                EXAMPLES,
                SOURCE
            FROM 
                BUSINESS_GLOSSARY
            ORDER BY 
                TERM_NAME
            """)
            
            terms = []
            for row in cursor.fetchall():
                # Parse JSON arrays from string fields
                synonyms = []
                related_terms = []
                
                if row[6]:  # SYNONYMS
                    try:
                        import json
                        synonyms = json.loads(row[6])
                    except:
                        # If JSON parsing fails, try comma-separated string
                        synonyms = [s.strip() for s in row[6].split(",")]
                
                if row[7]:  # RELATED_TERMS
                    try:
                        import json
                        related_terms = json.loads(row[7])
                    except:
                        # If JSON parsing fails, try comma-separated string
                        related_terms = [s.strip() for s in row[7].split(",")]
                
                terms.append({
                    'term_id': row[0],
                    'term_name': row[1],
                    'definition': row[2],
                    'status': row[3],
                    'steward': row[4],
                    'domain': row[5],
                    'synonyms': synonyms,
                    'related_terms': related_terms,
                    'created_on': row[8].isoformat() if row[8] else None,
                    'last_modified': row[9].isoformat() if row[9] else None,
                    'last_modified_by': row[10],
                    'approved_by': row[11],
                    'approved_on': row[12].isoformat() if row[12] else None,
                    'examples': row[13],
                    'source': row[14]
                })
            
            return {
                'status': 'success',
                'terms': terms,
                'count': len(terms)
            }
        

        except Exception as e:
            print(f"Error getting business terms: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'terms': []
            }

    def get_tags(self, cursor) -> Dict[str, Any]:
        """
        Get tag definitions from a dedicated table or schema
        
        Args:
            cursor: Active Snowflake cursor
            
        Returns:
            Dictionary with tags
        """
        try:
            # Check if tags table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'TAGS'
            LIMIT 1
            """)
            
            if not cursor.fetchone():
                # Table doesn't exist, return empty results
                return {
                    'status': 'success',
                    'tags': [],
                    'count': 0
                }
            
            # Query tags table
            cursor.execute("""
                    SELECT 
                TAG_ID,
                TAG_NAME,
                TAG_CATEGORY,
                TAG_DESCRIPTION,
                CREATED_ON,
                CREATED_BY,
                IS_ACTIVE
            FROM 
                TAGS
                    ORDER BY 
                TAG_CATEGORY, TAG_NAME
            """)
            
            tags = []
            for row in cursor.fetchall():
                tags.append({
                    'tag_id': row[0],
                    'tag_name': row[1],
                    'tag_category': row[2],
                    'tag_description': row[3],
                    'created_on': row[4].isoformat() if row[4] else None,
                    'created_by': row[5],
                    'is_active': row[6] == 'Y' or row[6] == 1 or row[6] == True
                })
            
            return {
                'status': 'success',
                'tags': tags,
                'count': len(tags)
            }
                
        except Exception as e:
            print(f"Error getting tags: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'tags': []
            }

    def get_tag_assignments(self, cursor) -> Dict[str, Any]:
        """
        Get tag assignments from a dedicated table or schema
        
        Args:
            cursor: Active Snowflake cursor
            
        Returns:
            Dictionary with tag assignments
        """
        try:
            # Check if tag assignments table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'TAG_ASSIGNMENTS'
            LIMIT 1
            """)
            
            if not cursor.fetchone():
                # Table doesn't exist, return empty results
                return {
                    'status': 'success',
                    'assignments': [],
                    'count': 0
                }
            
            # Query tag assignments table
            cursor.execute("""
                SELECT 
                ASSIGNMENT_ID,
                TAG_ID,
                TAG_NAME,
                OBJECT_TYPE,
                OBJECT_ID,
                ASSIGNED_ON,
                ASSIGNED_BY,
                ASSIGNMENT_NOTE
            FROM 
                TAG_ASSIGNMENTS
            """)
            
            assignments = []
            for row in cursor.fetchall():
                assignments.append({
                    'assignment_id': row[0],
                    'tag_id': row[1],
                    'tag_name': row[2],
                    'object_type': row[3],
                    'object_id': row[4],
                    'assigned_on': row[5].isoformat() if row[5] else None,
                    'assigned_by': row[6],
                    'assignment_note': row[7]
                })
            
            return {
                'status': 'success',
                'assignments': assignments,
                'count': len(assignments)
            }
                
        except Exception as e:
            print(f"Error getting tag assignments: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'assignments': []
            }

    def get_lineage_information(self, cursor) -> Dict[str, Any]:
        """
        Get data lineage information from dedicated tables or views
        
        Args:
            cursor: Active Snowflake cursor
            
        Returns:
            Dictionary with lineage nodes and edges
        """
        try:
            nodes = []
            edges = []
            
            # Check if lineage nodes table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'LINEAGE_NODES'
            LIMIT 1
            """)
            
            if cursor.fetchone():
                # Query lineage nodes
                cursor.execute("""
                SELECT 
                    NODE_ID,
                    NODE_TYPE,
                    OBJECT_ID,
                    OBJECT_NAME,
                    OBJECT_TYPE,
                    DATABASE_NAME,
                    SCHEMA_NAME
                FROM 
                    LINEAGE_NODES
                """)
                
                for row in cursor.fetchall():
                    nodes.append({
                        'node_id': row[0],
                        'node_type': row[1],
                        'object_id': row[2],
                        'object_name': row[3],
                        'object_type': row[4],
                        'database_name': row[5],
                        'schema_name': row[6]
                    })
            
            # Check if lineage edges table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'LINEAGE_EDGES'
            LIMIT 1
            """)
            
            if cursor.fetchone():
                # Query lineage edges
                cursor.execute("""
                SELECT 
                    EDGE_ID,
                    SOURCE_NODE_ID,
                    TARGET_NODE_ID,
                    TRANSFORMATION_TYPE,
                    TRANSFORMATION_DETAILS,
                    CONFIDENCE_SCORE,
                    CREATED_ON,
                    LAST_MODIFIED,
                    IS_ACTIVE
                FROM 
                    LINEAGE_EDGES
                """)
                
                for row in cursor.fetchall():
                    edges.append({
                        'edge_id': row[0],
                        'source_node_id': row[1],
                        'target_node_id': row[2],
                        'transformation_type': row[3],
                        'transformation_details': row[4],
                        'confidence_score': float(row[5]) if row[5] is not None else None,
                        'created_on': row[6].isoformat() if row[6] else None,
                        'last_modified': row[7].isoformat() if row[7] else None,
                        'is_active': row[8] == 'Y' or row[8] == 1 or row[8] == True
                    })
            
            return {
                'status': 'success',
                'nodes': nodes,
                'edges': edges,
                'count': len(nodes) + len(edges)
            }
                
        except Exception as e:
            print(f"Error getting lineage information: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'nodes': [],
                'edges': []
            }

    def get_profile_stats(self, cursor, database_name=None, schema_name=None, table_name=None, column_name=None) -> Dict[str, Any]:
        """
        Get data profiling statistics from dedicated tables
        
        Args:
            cursor: Active Snowflake cursor
            database_name: Optional database name to filter
            schema_name: Optional schema name to filter
            table_name: Optional table name to filter
            column_name: Optional column name to filter
            
        Returns:
            Dictionary with profiling statistics
        """
        try:
            # Check if profile statistics table exists
            cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'COLUMN_PROFILE_STATS'
            LIMIT 1
            """)
            
            if not cursor.fetchone():
                # Table doesn't exist, return empty results
                return {
                    'status': 'success',
                    'stats': [],
                    'count': 0
                }
            
            # Build query filters
            filters = []
            params = []
            
            if database_name:
                filters.append("DATABASE_NAME = %s")
                params.append(database_name)
            if schema_name:
                filters.append("SCHEMA_NAME = %s")
                params.append(schema_name)
            if table_name:
                filters.append("TABLE_NAME = %s")
                params.append(table_name)
            if column_name:
                filters.append("COLUMN_NAME = %s")
                params.append(column_name)
                
            where_clause = ""
            if filters:
                where_clause = "WHERE " + " AND ".join(filters)
            
            # Query profile statistics
            query = f"""
            SELECT 
                COLUMN_ID,
                COLUMN_NAME,
                TABLE_ID,
                TABLE_NAME,
                SCHEMA_NAME,
                DATABASE_NAME,
                PROFILING_DATE,
                ROW_COUNT,
                NULL_COUNT,
                NULL_PERCENTAGE,
                DISTINCT_COUNT,
                DISTINCT_PERCENTAGE,
                MIN_VALUE,
                MAX_VALUE,
                AVG_VALUE,
                MEDIAN_VALUE,
                MIN_LENGTH,
                MAX_LENGTH,
                AVG_LENGTH,
                HISTOGRAM,
                STATISTICAL_TYPE,
                PATTERNS,
                OUTLIERS_COUNT,
                POTENTIAL_ISSUES
            FROM 
                COLUMN_PROFILE_STATS
            {where_clause}
            ORDER BY 
                DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, PROFILING_DATE DESC
            """
            
            cursor.execute(query, params)
            
            stats = []
            for row in cursor.fetchall():
                stats.append({
                    'column_id': row[0],
                    'column_name': row[1],
                    'table_id': row[2],
                    'table_name': row[3],
                    'schema_name': row[4],
                    'database_name': row[5],
                    'profiling_date': row[6].isoformat() if row[6] else None,
                    'row_count': row[7],
                    'null_count': row[8],
                    'null_percentage': row[9],
                    'distinct_count': row[10],
                    'distinct_percentage': row[11],
                    'min_value': row[12],
                    'max_value': row[13],
                    'avg_value': row[14],
                    'median_value': row[15],
                    'min_length': row[16],
                    'max_length': row[17],
                    'avg_length': row[18],
                    'histogram': row[19],
                    'statistical_type': row[20],
                    'patterns': row[21],
                    'outliers_count': row[22],
                    'potential_issues': row[23]
                })
            
            return {
                'status': 'success',
                'stats': stats,
                'count': len(stats)
            }
                
        except Exception as e:
            print(f"Error getting profile statistics: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'stats': []
            }