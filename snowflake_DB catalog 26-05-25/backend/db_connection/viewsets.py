from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from django.core.cache import cache
import uuid
import threading
import concurrent.futures
from queue import Queue
import time
from .snowflake_manager import SnowflakeManager
from datetime import datetime
from .utils import process_logger

class SnowflakeViewSet(viewsets.ViewSet):
    snowflake_manager = SnowflakeManager()
    processing_queue = Queue()
    
    @action(detail=False, methods=['post'], url_path='connect')
    def connect(self, request):
        """Establish Snowflake connection without collecting metadata"""
        try:
            # Validate connection parameters first
            required_fields = ['account', 'username', 'password', 'warehouse']
            for field in required_fields:
                if field not in request.data:
                    return Response({
                        'status': 'error',
                        'message': f'Missing required field: {field}'
                    }, status=status.HTTP_400_BAD_REQUEST)

            # Generate a process ID for tracking
            process_id = str(uuid.uuid4())
            connection_params = request.data.copy()
            # Set required role and defaults
            connection_params['role'] = 'ACCOUNTADMIN'  # Force ACCOUNTADMIN role
            connection_params.setdefault('database', None)  # Don't set default database
            connection_params.setdefault('schema', None)  # Don't set default schema
            
            # Allow configuration of metadata schema (PUBLIC or METADATA)
            metadata_schema = request.data.get('metadata_schema', 'PUBLIC')
            connection_params['metadata_schema'] = metadata_schema
            
            # Add process ID to connection params for tracking
            connection_params['process_id'] = process_id
            
            # Add timeout parameters for better performance
            connection_params['connect_timeout'] = 30
            connection_params['login_timeout'] = 60
            connection_params['query_timeout'] = 300
            
            process_logger.info(f"Connection parameters set with ACCOUNTADMIN role and {metadata_schema} schema")

            # Format account identifier
            account = connection_params['account']
            account = account.replace('.snowflakecomputing.com', '')
            
            if not any(char in account for char in ['-', '.']):
                account = f"{account}.ap-south-1"  # Default region if not specified
                
            connection_params['account'] = account

            # Set initial status in cache
            self._update_cache_status(process_id, {
                'status': 'initiated',
                'progress': 0,
                'message': 'Connection initiated, testing connection...',
                'timestamp': datetime.now().isoformat()
            })

            # Test connection before proceeding
            try:
                with self.snowflake_manager.get_optimized_connection(connection_params) as conn:
                    # Connection successful, update status
                    self._update_cache_status(process_id, {
                        'status': 'connected',
                        'progress': 100,
                        'message': 'Connection successful! Ready to collect metadata.',
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Save the connection in Django models for future reference
                    self.snowflake_manager.save_connection_impl(connection_params, process_logger)
                    
                    # Initialize the database structure but don't collect metadata yet
                    self._initialize_db_structure(process_id, connection_params)
                    
            except Exception as conn_error:
                process_logger.error(f"Connection test failed: {str(conn_error)}")
                return Response({
                    'status': 'error',
                    'message': f'Connection test failed: {str(conn_error)}'
                }, status=status.HTTP_400_BAD_REQUEST)

            return Response({
                'status': 'success',
                'process_id': process_id,
                'message': 'Connection established successfully',
                'connection_params': {
                    'account': account,
                    'username': connection_params['username'],
                    'warehouse': connection_params['warehouse'],
                    'database': connection_params.get('database'),
                    'schema': connection_params.get('schema'),
                    'metadata_schema': metadata_schema
                }
            })

        except Exception as e:
            process_logger.error(f"Unexpected error in connect: {str(e)}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=False, methods=['post'], url_path='collect-metadata')
    def collect_metadata(self, request):
        """Collect metadata from a successfully established Snowflake connection"""
        try:
            # Validate that we have the connection parameters
            if 'process_id' not in request.data:
                return Response({
                    'status': 'error',
                    'message': 'Missing process_id from the connection step'
                }, status=status.HTTP_400_BAD_REQUEST)
                
            process_id = request.data['process_id']
            
            # Get connection parameters from the request
            connection_params = {}
            for field in ['account', 'username', 'password', 'warehouse', 'database', 'schema', 'metadata_schema']:
                if field in request.data:
                    connection_params[field] = request.data[field]
            
            # Add timeout parameters
            connection_params['connect_timeout'] = request.data.get('connect_timeout', 30)
            connection_params['login_timeout'] = request.data.get('login_timeout', 60)
            connection_params['query_timeout'] = request.data.get('query_timeout', 300)
            
            # Add process ID for tracking
            connection_params['process_id'] = process_id
            
            # Set initial status in cache for metadata collection
            self._update_cache_status(process_id, {
                'status': 'processing',
                'phase': 'metadata_collection',
                'progress': 0,
                'message': 'Starting metadata collection...',
                'timestamp': datetime.now().isoformat()
            })

            # Start parallel processing thread for metadata collection
            threading.Thread(
                target=self._collect_metadata_parallel,
                args=(process_id, connection_params),
                daemon=True
            ).start()

            return Response({
                'status': 'processing',
                'process_id': process_id,
                'message': 'Metadata collection started in background',
                'tracking_url': f'/api/snowflake/process-status/{process_id}/'
            })

        except Exception as e:
            process_logger.error(f"Unexpected error in collect_metadata: {str(e)}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _initialize_db_structure(self, process_id, connection_params):
        """Initialize the database structure without collecting metadata"""
        try:
            # Step 1: Initialize the database structure
            with self.snowflake_manager.get_connection(connection_params) as conn:
                cur = conn.cursor()
                
                try:
                    # Ensure we're using ACCOUNTADMIN role
                    cur.execute("USE ROLE ACCOUNTADMIN")
                    process_logger.info("Successfully switched to ACCOUNTADMIN role")
                    
                    # Create database and schema with explicit error logging
                    process_logger.info("Attempting to create SNOWFLAKE_CATALOG database")
                    cur.execute("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_CATALOG")
                    process_logger.info("SNOWFLAKE_CATALOG database created successfully")
                    
                    cur.execute("USE DATABASE SNOWFLAKE_CATALOG")
                    process_logger.info("Successfully switched to SNOWFLAKE_CATALOG database")
                    
                    # Allow using either PUBLIC schema or METADATA schema
                    schema_name = connection_params.get('metadata_schema', 'PUBLIC')
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    process_logger.info(f"{schema_name} schema created successfully")
                    
                    cur.execute(f"USE SCHEMA {schema_name}")
                    process_logger.info(f"Successfully switched to {schema_name} schema")
                    
                    # Commit the changes
                    conn.commit()
                    process_logger.info("Changes committed successfully")
                    
                except Exception as e:
                    process_logger.error(f"Error during database creation: {str(e)}")
                    raise Exception(f"Failed to create database structure: {str(e)}")
                
                # Create the connections table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS CONNECTIONS (
                        CONNECTION_ID VARCHAR(255) PRIMARY KEY,
                        ACCOUNT VARCHAR(255) NOT NULL,
                        USERNAME VARCHAR(255) NOT NULL,
                        WAREHOUSE VARCHAR(255) NOT NULL,
                        DATABASE_NAME VARCHAR(255),
                        SCHEMA_NAME VARCHAR(255),
                        ROLE VARCHAR(50),
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        LAST_USED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        STATUS VARCHAR(50) DEFAULT 'ACTIVE'
                    )
                """)
                
                # Save the connection in the table
                cur.execute("""
                    MERGE INTO CONNECTIONS t
                    USING (SELECT %s, %s, %s, %s, %s, %s, %s) s (
                        CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, DATABASE_NAME, SCHEMA_NAME, ROLE
                    )
                    ON t.CONNECTION_ID = s.CONNECTION_ID
                    WHEN MATCHED THEN
                        UPDATE SET
                            LAST_USED = CURRENT_TIMESTAMP(),
                            STATUS = 'ACTIVE'
                    WHEN NOT MATCHED THEN
                        INSERT (CONNECTION_ID, ACCOUNT, USERNAME, WAREHOUSE, DATABASE_NAME, SCHEMA_NAME, ROLE)
                        VALUES (s.CONNECTION_ID, s.ACCOUNT, s.USERNAME, s.WAREHOUSE, s.DATABASE_NAME, s.SCHEMA_NAME, s.ROLE)
                """, (
                    process_id,
                    connection_params['account'],
                    connection_params['username'],
                    connection_params['warehouse'],
                    connection_params.get('database'),
                    connection_params.get('schema'),
                    connection_params.get('role', 'ACCOUNTADMIN')
                ))
                
                conn.commit()

            process_logger.info(f"Database structure initialized for process {process_id}")
            return True
            
        except Exception as e:
            error_message = str(e)
            process_logger.error(f"Database initialization failed for process {process_id}: {error_message}")
            
            self._update_cache_status(process_id, {
                'status': 'error',
                'message': f"Database initialization failed: {error_message}",
                'timestamp': datetime.now().isoformat()
            })
            
            return False
    
    def _collect_metadata_parallel(self, process_id, connection_params):
        """Collect metadata in a background thread"""
        try:
            # Update status for metadata collection
            self._update_cache_status(process_id, {
                'status': 'processing',
                'phase': 'metadata_collection',
                'progress': 10,
                'message': 'Collecting metadata from Snowflake...',
                'timestamp': datetime.now().isoformat()
            })
            
            # Set a longer timeout for large databases
            timeout = connection_params.get('metadata_timeout', 3600)  # Default 1 hour, can be overridden
            
            # Get optimization parameters
            max_tables = connection_params.get('max_tables_per_schema', 500)
            max_schemas = connection_params.get('max_schemas_per_db', 100)
            parallelism = connection_params.get('parallel_databases', False)  # Whether to process databases in parallel
            
            # First approach: try to collect all metadata at once
            try:
                # Call the bulk metadata collection method
                metadata_result = self.snowflake_manager.collect_snowflake_metadata(connection_params, timeout=timeout)
                
                if metadata_result.get('status') == 'success':
                    # Success - just report results
                    self._update_cache_status(process_id, {
                        'status': 'completed',
                        'progress': 100,
                        'message': 'Metadata collection completed successfully',
                        'timestamp': datetime.now().isoformat(),
                        'stats': {
                            'database_count': metadata_result.get('database_count', 0),
                            'schema_count': metadata_result.get('schema_count', 0),
                            'table_count': metadata_result.get('table_count', 0),
                            'column_count': metadata_result.get('column_count', 0),
                        }
                    })
                    
                    process_logger.info(f"Metadata collection process {process_id} completed successfully")
                    return
                else:
                    # Failed - try the per-database approach
                    process_logger.warning(f"Bulk metadata collection failed: {metadata_result.get('message', 'Unknown error')}")
                    # Continue to the per-database approach
            except Exception as bulk_error:
                process_logger.error(f"Bulk metadata collection failed: {str(bulk_error)}")
                # Continue to the per-database approach
            
            # If we're here, the bulk approach failed - try a more targeted approach by processing each database separately
            self._update_cache_status(process_id, {
                'status': 'processing',
                'phase': 'metadata_collection_per_db',
                'progress': 15,
                'message': 'Processing databases individually...',
                'timestamp': datetime.now().isoformat()
            })
            
            # Get list of databases
            process_logger.info("Getting list of databases...")
            with self.snowflake_manager.get_connection(connection_params) as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                
                # Filter out system databases to focus on user data
                filtered_databases = []
                for db_row in databases:
                    db_name = db_row[1]
                    if not db_name.startswith('SNOWFLAKE') and not db_name == 'SNOWFLAKE_SAMPLE_DATA':
                        filtered_databases.append(db_row)
                
                # Apply database limit if needed
                if len(filtered_databases) > max_schemas:
                    process_logger.info(f"Limiting to {max_schemas} databases")
                    filtered_databases = filtered_databases[:max_schemas]
                
                total_dbs = len(filtered_databases)
                process_logger.info(f"Found {total_dbs} databases to process")
                
                total_results = {
                    'database_count': 0,
                    'schema_count': 0,
                    'table_count': 0,
                    'column_count': 0,
                    'success_count': 0,
                    'error_count': 0,
                    'databases_processed': []
                }
                
                if parallelism and total_dbs > 1:
                    # Process databases in parallel
                    process_logger.info("Using parallel processing for databases")
                    self._update_cache_status(process_id, {
                        'progress': 20,
                        'message': f'Processing {total_dbs} databases in parallel...',
                    })
                    
                    # Use ThreadPoolExecutor to process databases in parallel
                    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, total_dbs)) as executor:
                        # Map function to process each database
                        futures = {}
                        for idx, db_row in enumerate(filtered_databases):
                            db_name = db_row[1]
                            db_params = connection_params.copy()
                            db_params['database'] = db_name
                            db_params['process_id'] = f"{process_id}_db_{idx}"
                            
                            # Submit task to executor
                            future = executor.submit(
                                self.snowflake_manager.collect_database_metadata,
                                db_params,
                                timeout=300  # Shorter timeout per database
                            )
                            futures[future] = db_name
                        
                        # Process results as they complete
                        completed = 0
                        for future in concurrent.futures.as_completed(futures):
                            db_name = futures[future]
                            completed += 1
                            progress = int((completed / total_dbs) * 80) + 20  # 20-100% progress
                            
                            try:
                                db_result = future.result()
                                if db_result.get('status') == 'success':
                                    total_results['success_count'] += 1
                                    total_results['database_count'] += 1
                                    total_results['schema_count'] += db_result.get('schema_count', 0)
                                    total_results['table_count'] += db_result.get('table_count', 0)
                                    total_results['column_count'] += db_result.get('column_count', 0)
                                    total_results['databases_processed'].append(db_name)
                                    
                                    self._update_cache_status(process_id, {
                                        'progress': progress,
                                        'message': f'Processed {completed}/{total_dbs} databases. Current: {db_name}',
                                    })
                                else:
                                    total_results['error_count'] += 1
                                    process_logger.error(f"Error processing database {db_name}: {db_result.get('message')}")
                            except Exception as db_error:
                                total_results['error_count'] += 1
                                process_logger.error(f"Error processing database {db_name}: {str(db_error)}")
                                
                                self._update_cache_status(process_id, {
                                    'progress': progress,
                                    'message': f'Error processing {db_name}, continuing with others ({completed}/{total_dbs})',
                                })
                else:
                    # Process databases sequentially
                    process_logger.info("Processing databases sequentially")
                    for idx, db_row in enumerate(filtered_databases):
                        db_name = db_row[1]
                        progress = int((idx / total_dbs) * 80) + 20  # 20-100% progress
                        
                        self._update_cache_status(process_id, {
                            'progress': progress,
                            'message': f'Processing database {idx+1}/{total_dbs}: {db_name}',
                            'timestamp': datetime.now().isoformat()
                        })
                        
                        try:
                            # Clone connection params and set the current database
                            db_params = connection_params.copy()
                            db_params['database'] = db_name
                            db_params['process_id'] = f"{process_id}_db_{idx}"
                            
                            # Collect metadata just for this database
                            db_result = self.snowflake_manager.collect_database_metadata(db_params, timeout=timeout/2)
                            
                            # Accumulate stats
                            if db_result.get('status') == 'success':
                                total_results['success_count'] += 1
                                total_results['database_count'] += 1
                                total_results['schema_count'] += db_result.get('schema_count', 0)
                                total_results['table_count'] += db_result.get('table_count', 0)
                                total_results['column_count'] += db_result.get('column_count', 0)
                                total_results['databases_processed'].append(db_name)
                            else:
                                total_results['error_count'] += 1
                                process_logger.error(f"Error processing database {db_name}: {db_result.get('message')}")
                        except Exception as db_error:
                            total_results['error_count'] += 1
                            process_logger.error(f"Error processing database {db_name}: {str(db_error)}")
                
                # Final successful completion
                success_message = 'Metadata collection completed'
                if total_results['error_count'] > 0:
                    success_message += f' with {total_results["error_count"]} errors'
                
                if total_results['success_count'] == 0:
                    self._update_cache_status(process_id, {
                        'status': 'error',
                        'progress': 100,
                        'message': 'Failed to collect metadata from any database',
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    self._update_cache_status(process_id, {
                        'status': 'completed',
                        'progress': 100,
                        'message': success_message,
                        'timestamp': datetime.now().isoformat(),
                        'stats': {
                            'database_count': total_results['database_count'],
                            'schema_count': total_results['schema_count'],
                            'table_count': total_results['table_count'],
                            'column_count': total_results['column_count'],
                            'full_success': total_results['error_count'] == 0,
                            'processed_databases': total_results['databases_processed']
                        }
                    })
                    
                process_logger.info(f"Metadata collection completed with {total_results['success_count']} successful and {total_results['error_count']} failed databases")
            
        except Exception as e:
            error_message = str(e)
            process_logger.error(f"Metadata collection process {process_id} failed: {error_message}")
            
            self._update_cache_status(process_id, {
                'status': 'error',
                'message': error_message,
                'timestamp': datetime.now().isoformat()
            })
    
    # Keep the original connect_and_process method for backward compatibility
    @action(detail=False, methods=['post'], url_path='connect-and-process')
    def connect_and_process(self, request):
        """Single API endpoint to establish Snowflake connection and process data in parallel (legacy)"""
        try:
            # First connect to Snowflake
            connect_response = self.connect(request).data
            
            if connect_response.get('status') != 'success':
                return Response(connect_response)
            
            # If connection successful, start metadata collection
            process_id = connect_response.get('process_id')
            connection_params = connect_response.get('connection_params')
            
            # Add back the password which would have been filtered out in the response
            connection_params['password'] = request.data.get('password')
            
            # Create a new request with the connection parameters
            metadata_request = type('obj', (object,), {
                'data': {
                    'process_id': process_id,
                    **connection_params
                }
            })
            
            # Call collect_metadata method
            return self.collect_metadata(metadata_request)
            
        except Exception as e:
            process_logger.error(f"Unexpected error in connect_and_process: {str(e)}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _update_cache_status(self, process_id, status_data, timeout=3600):
        """Update the status in cache with a timeout"""
        cache.set(f'process_status_{process_id}', status_data, timeout)

    @action(detail=False, methods=['get'], url_path='process-status/(?P<process_id>[^/.]+)')
    def get_process_status(self, request, process_id):
        """Get the status of a processing job"""
        status_data = cache.get(f'process_status_{process_id}')
        if not status_data:
            return Response({
                'status': 'error',
                'message': 'Process not found or expired'
            }, status=status.HTTP_404_NOT_FOUND)
        
        return Response(status_data) 