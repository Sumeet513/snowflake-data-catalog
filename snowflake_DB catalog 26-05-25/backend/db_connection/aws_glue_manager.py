import boto3
import logging
import time
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class AWSGlueManager:
    """
    Manager class for AWS Glue operations
    """
    
    def __init__(self):
        self.session = None
        self.glue_client = None
    
    def create_session(self, connection_params: Dict[str, Any]) -> bool:
        """
        Create a boto3 session with the provided credentials
        
        Args:
            connection_params: Dictionary containing AWS credentials
                - aws_region: AWS region
                - access_key: AWS access key ID
                - secret_key: AWS secret access key
                - session_token: (Optional) AWS session token
                - role_arn: (Optional) AWS IAM role ARN to assume
                
        Returns:
            bool: True if session created successfully, False otherwise
        """
        try:
            # Extract parameters
            aws_region = connection_params.get('aws_region')
            access_key = connection_params.get('access_key')
            secret_key = connection_params.get('secret_key')
            session_token = connection_params.get('session_token')
            role_arn = connection_params.get('role_arn')
            
            # Validate required parameters
            if not aws_region or not access_key or not secret_key:
                logger.error("Missing required AWS credentials")
                return False
            
            # Create session with provided credentials
            self.session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=aws_region
            )
            
            # If role ARN is provided, assume the role
            if role_arn:
                sts_client = self.session.client('sts')
                assumed_role = sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="GlueCatalogSession"
                )
                
                # Create a new session with the assumed role credentials
                self.session = boto3.Session(
                    aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                    aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                    aws_session_token=assumed_role['Credentials']['SessionToken'],
                    region_name=aws_region
                )
            
            # Create Glue client
            self.glue_client = self.session.client('glue')
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating AWS session: {str(e)}")
            return False
    
    def test_connection(self, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test the AWS Glue connection
        
        Args:
            connection_params: Dictionary containing AWS credentials
                
        Returns:
            dict: Result of the connection test
        """
        try:
            # Create session
            if not self.create_session(connection_params):
                return {
                    'status': 'error',
                    'message': 'Failed to create AWS session'
                }
            
            # Test connection by listing databases
            response = self.glue_client.get_databases()
            
            return {
                'status': 'success',
                'message': 'AWS Glue connection successful',
                'details': {
                    'database_count': len(response.get('DatabaseList', []))
                }
            }
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            logger.error(f"AWS Glue connection error: {error_code} - {error_message}")
            
            return {
                'status': 'error',
                'message': f"AWS Glue connection failed: {error_code} - {error_message}"
            }
            
        except Exception as e:
            logger.error(f"Unexpected error testing AWS Glue connection: {str(e)}")
            
            return {
                'status': 'error',
                'message': f"AWS Glue connection failed: {str(e)}"
            }
    
    def collect_aws_glue_metadata(self, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect metadata from AWS Glue Data Catalog
        
        Args:
            connection_params: Dictionary containing AWS credentials
                
        Returns:
            dict: Result of the metadata collection
        """
        try:
            # Create session
            if not self.create_session(connection_params):
                return {
                    'status': 'error',
                    'message': 'Failed to create AWS session'
                }
            
            # Get all databases
            databases = self._get_all_databases()
            
            # Initialize counters
            total_databases = len(databases)
            total_tables = 0
            total_columns = 0
            
            # Process each database
            for db_index, database in enumerate(databases):
                database_name = database['Name']
                
                # Get all tables in the database
                tables = self._get_all_tables(database_name)
                total_tables += len(tables)
                
                # Process each table
                for table_index, table in enumerate(tables):
                    table_name = table['Name']
                    
                    # Get table columns
                    columns = self._get_table_columns(table)
                    total_columns += len(columns)
                    
                    # Store the table metadata in the database
                    self._store_table_metadata(database, table, columns)
            
            return {
                'status': 'success',
                'message': 'AWS Glue metadata collection completed',
                'details': {
                    'database_count': total_databases,
                    'table_count': total_tables,
                    'column_count': total_columns
                }
            }
            
        except Exception as e:
            logger.error(f"Error collecting AWS Glue metadata: {str(e)}")
            
            return {
                'status': 'error',
                'message': f"AWS Glue metadata collection failed: {str(e)}"
            }
    
    def _get_all_databases(self) -> List[Dict[str, Any]]:
        """
        Get all databases from AWS Glue Data Catalog
        
        Returns:
            list: List of database dictionaries
        """
        databases = []
        next_token = None
        
        try:
            while True:
                # Get databases with pagination
                if next_token:
                    response = self.glue_client.get_databases(NextToken=next_token)
                else:
                    response = self.glue_client.get_databases()
                
                # Add databases to the list
                databases.extend(response.get('DatabaseList', []))
                
                # Check if there are more databases
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            return databases
            
        except Exception as e:
            logger.error(f"Error getting AWS Glue databases: {str(e)}")
            return []
    
    def _get_all_tables(self, database_name: str) -> List[Dict[str, Any]]:
        """
        Get all tables in a database
        
        Args:
            database_name: Name of the database
            
        Returns:
            list: List of table dictionaries
        """
        tables = []
        next_token = None
        
        try:
            while True:
                # Get tables with pagination
                if next_token:
                    response = self.glue_client.get_tables(
                        DatabaseName=database_name,
                        NextToken=next_token
                    )
                else:
                    response = self.glue_client.get_tables(
                        DatabaseName=database_name
                    )
                
                # Add tables to the list
                tables.extend(response.get('TableList', []))
                
                # Check if there are more tables
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables for database {database_name}: {str(e)}")
            return []
    
    def _get_table_columns(self, table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract column information from a table
        
        Args:
            table: Table dictionary from AWS Glue
            
        Returns:
            list: List of column dictionaries
        """
        columns = []
        
        try:
            # Get storage descriptor
            storage_descriptor = table.get('StorageDescriptor', {})
            
            # Get columns from storage descriptor
            sd_columns = storage_descriptor.get('Columns', [])
            
            # Add columns to the list
            for position, column in enumerate(sd_columns):
                column_info = {
                    'Name': column.get('Name'),
                    'Type': column.get('Type'),
                    'Comment': column.get('Comment', ''),
                    'Position': position,
                    'IsPartitionKey': False
                }
                columns.append(column_info)
            
            # Get partition keys
            partition_keys = table.get('PartitionKeys', [])
            
            # Add partition keys to the list
            for position, column in enumerate(partition_keys):
                column_info = {
                    'Name': column.get('Name'),
                    'Type': column.get('Type'),
                    'Comment': column.get('Comment', ''),
                    'Position': len(sd_columns) + position,
                    'IsPartitionKey': True
                }
                columns.append(column_info)
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting columns for table {table.get('Name')}: {str(e)}")
            return []
    
    def _store_table_metadata(self, database: Dict[str, Any], table: Dict[str, Any], columns: List[Dict[str, Any]]):
        """
        Store table metadata in the database
        
        Args:
            database: Database dictionary from AWS Glue
            table: Table dictionary from AWS Glue
            columns: List of column dictionaries
        """
        try:
            from .models import (
                AWSGlueCatalog,
                AWSGlueDatabase,
                AWSGlueTable,
                AWSGlueColumn
            )
            
            # Get or create catalog
            catalog_id = database.get('CatalogId', 'default')
            catalog, created = AWSGlueCatalog.objects.get_or_create(
                catalog_id=catalog_id,
                defaults={
                    'catalog_name': 'AWS Glue Catalog',
                    'catalog_description': 'AWS Glue Data Catalog'
                }
            )
            
            # Get or create database
            db_name = database.get('Name')
            db_description = database.get('Description', '')
            db_location = database.get('LocationUri', '')
            db_parameters = database.get('Parameters', {})
            db_create_time = database.get('CreateTime')
            
            db, created = AWSGlueDatabase.objects.get_or_create(
                catalog=catalog,
                database_name=db_name,
                defaults={
                    'database_id': f"{catalog_id}_{db_name}",
                    'database_description': db_description,
                    'location_uri': db_location,
                    'parameters': db_parameters,
                    'create_date': db_create_time
                }
            )
            
            # Get or create table
            table_name = table.get('Name')
            table_type = table.get('TableType', '')
            table_description = table.get('Description', '')
            table_owner = table.get('Owner', '')
            table_create_time = table.get('CreateTime')
            table_last_access_time = table.get('LastAccessTime')
            table_last_updated_time = table.get('UpdateTime')
            
            # Get storage information
            storage_descriptor = table.get('StorageDescriptor', {})
            storage_location = storage_descriptor.get('Location', '')
            storage_format = storage_descriptor.get('InputFormat', '').split('.')[-1]
            
            # Get parameters and partition keys
            table_parameters = table.get('Parameters', {})
            partition_keys = [pk.get('Name') for pk in table.get('PartitionKeys', [])]
            
            table_obj, created = AWSGlueTable.objects.get_or_create(
                database=db,
                table_name=table_name,
                defaults={
                    'table_id': f"{catalog_id}_{db_name}_{table_name}",
                    'table_type': table_type,
                    'table_description': table_description,
                    'owner': table_owner,
                    'create_date': table_create_time,
                    'last_access_date': table_last_access_time,
                    'last_altered_date': table_last_updated_time,
                    'storage_location': storage_location,
                    'storage_format': storage_format,
                    'parameters': table_parameters,
                    'partition_keys': partition_keys
                }
            )
            
            # Create columns
            for column in columns:
                column_name = column.get('Name')
                column_type = column.get('Type', '')
                column_description = column.get('Comment', '')
                column_position = column.get('Position', 0)
                is_partition_key = column.get('IsPartitionKey', False)
                
                column_obj, created = AWSGlueColumn.objects.get_or_create(
                    table=table_obj,
                    column_name=column_name,
                    defaults={
                        'column_id': f"{catalog_id}_{db_name}_{table_name}_{column_name}",
                        'data_type': column_type,
                        'column_description': column_description,
                        'ordinal_position': column_position,
                        'is_nullable': True,
                        'is_partition_key': is_partition_key,
                        'parameters': {}
                    }
                )
            
        except Exception as e:
            logger.error(f"Error storing metadata for table {table.get('Name')}: {str(e)}")