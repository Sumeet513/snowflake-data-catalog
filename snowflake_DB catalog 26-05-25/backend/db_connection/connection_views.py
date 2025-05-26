from django.shortcuts import render
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import api_view, action
from django.core.cache import cache
import uuid
import threading
import time
import json
from datetime import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .models import (
    SnowflakeConnection,
    AWSGlueConnection,
    Connection
)
from .serializers import ConnectionSerializer

# Standalone test connection function
@csrf_exempt
@api_view(['POST'])
def test_connection(request):
    """Test a connection with provided credentials"""
    try:
        data = request.data
        connection_type = data.get('connection_type')
        
        if not connection_type:
            return Response({
                'status': 'error',
                'message': 'Connection type is required'
            }, status=status.HTTP_400_BAD_REQUEST)
            
        if connection_type == 'snowflake':
            # Test Snowflake connection
            try:
                import snowflake.connector
                
                # Extract credentials
                account = data.get('account')
                username = data.get('username')
                password = data.get('password')
                warehouse = data.get('warehouse')
                database = data.get('database_name')
                schema = data.get('schema_name')
                role = data.get('role')
                
                # Validate required fields
                if not all([account, username, password, warehouse]):
                    return Response({
                        'status': 'error',
                        'message': 'Account, username, password, and warehouse are required'
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                # Set default name if not provided
                if not data.get('name'):
                    data['name'] = f"Snowflake: {username}@{account}"
                
                # Create connection
                conn = snowflake.connector.connect(
                    account=account,
                    user=username,
                    password=password,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    role=role
                )
                
                # Test with a simple query
                cursor = conn.cursor()
                cursor.execute("SELECT current_version()")
                version = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                return Response({
                    'status': 'success',
                    'message': 'Snowflake connection successful',
                    'details': {'version': version}
                })
                
            except Exception as e:
                return Response({
                    'status': 'error',
                    'message': f'Snowflake connection failed: {str(e)}'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        elif connection_type == 'aws_glue':
            # Test AWS Glue connection
            try:
                from .aws_glue_manager import AWSGlueManager
                
                # Extract credentials
                aws_region = data.get('aws_region')
                access_key = data.get('access_key')
                
                # Set default name if not provided
                if not data.get('name'):
                    data['name'] = f"AWS Glue: {access_key}@{aws_region}"
                
                # Create AWS Glue manager
                aws_manager = AWSGlueManager()
                
                # Test connection
                result = aws_manager.test_connection(data)
                
                if result.get('status') == 'success':
                    return Response({
                        'status': 'success',
                        'message': 'AWS Glue connection successful',
                        'details': result.get('details', {})
                    })
                else:
                    return Response({
                        'status': 'error',
                        'message': result.get('message', 'AWS Glue connection failed')
                    }, status=status.HTTP_400_BAD_REQUEST)
                    
            except Exception as e:
                return Response({
                    'status': 'error',
                    'message': f'AWS Glue connection failed: {str(e)}'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        else:
            return Response({
                'status': 'error',
                'message': f'Unsupported connection type: {connection_type}'
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ViewSet for Connection model
class ConnectionViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing database connections
    """
    serializer_class = ConnectionSerializer
    
    def get_queryset(self):
        return Connection.objects.all()
    
    def list(self, request):
        """List all connections"""
        try:
            connections = self.get_queryset()
            connection_list = []
            
            for conn in connections:
                connection_data = {
                    'id': conn.id,
                    'name': conn.name,
                    'connection_type': conn.connection_type,
                    'is_active': conn.is_active,
                    'created_at': conn.created_at,
                    'last_used': conn.last_used,
                    'connection_details': conn.connection_details
                }
                connection_list.append(connection_data)
                
            return Response(connection_list)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def create(self, request):
        """Create a new connection"""
        try:
            data = request.data
            connection_type = data.get('connection_type')
            
            if not connection_type:
                return Response({'error': 'Connection type is required'}, status=status.HTTP_400_BAD_REQUEST)
                
            if connection_type == 'snowflake':
                # Create Snowflake connection
                snowflake_conn = SnowflakeConnection.objects.create(
                    name=data.get('name', 'New Snowflake Connection'),
                    account=data.get('account'),
                    username=data.get('username'),
                    password=data.get('password'),
                    warehouse=data.get('warehouse'),
                    database_name=data.get('database_name'),
                    schema_name=data.get('schema_name'),
                    role=data.get('role'),
                    is_active=data.get('is_active', True)
                )
                
                # Create universal connection
                connection = Connection.objects.create(
                    name=data.get('name', 'New Snowflake Connection'),
                    connection_type='snowflake',
                    snowflake_connection=snowflake_conn,
                    is_active=data.get('is_active', True)
                )
                
            elif connection_type == 'aws_glue':
                # Create AWS Glue connection
                aws_conn = AWSGlueConnection.objects.create(
                    name=data.get('name', 'New AWS Glue Connection'),
                    aws_region=data.get('aws_region'),
                    access_key=data.get('access_key'),
                    secret_key=data.get('secret_key'),
                    session_token=data.get('session_token'),
                    role_arn=data.get('role_arn'),
                    is_active=data.get('is_active', True)
                )
                
                # Create universal connection
                connection = Connection.objects.create(
                    name=data.get('name', 'New AWS Glue Connection'),
                    connection_type='aws_glue',
                    aws_glue_connection=aws_conn,
                    is_active=data.get('is_active', True)
                )
                
            else:
                return Response({'error': f'Unsupported connection type: {connection_type}'}, 
                               status=status.HTTP_400_BAD_REQUEST)
            
            # Return the created connection
            return Response({
                'id': connection.id,
                'name': connection.name,
                'connection_type': connection.connection_type,
                'is_active': connection.is_active,
                'created_at': connection.created_at,
                'last_used': connection.last_used,
                'connection_details': connection.connection_details
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def retrieve(self, request, pk=None):
        """Get a specific connection"""
        try:
            connection = Connection.objects.get(id=pk)
            return Response({
                'id': connection.id,
                'name': connection.name,
                'connection_type': connection.connection_type,
                'is_active': connection.is_active,
                'created_at': connection.created_at,
                'last_used': connection.last_used,
                'connection_details': connection.connection_details
            })
        except Connection.DoesNotExist:
            return Response({'error': 'Connection not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def update(self, request, pk=None):
        """Update a connection"""
        try:
            connection = Connection.objects.get(id=pk)
            data = request.data
            
            # Update connection name and active status
            if 'name' in data:
                connection.name = data['name']
            if 'is_active' in data:
                connection.is_active = data['is_active']
                
            # Update specific connection details
            if connection.connection_type == 'snowflake' and connection.snowflake_connection:
                snowflake_conn = connection.snowflake_connection
                
                # Update Snowflake connection fields
                for field in ['account', 'username', 'warehouse', 'database_name', 'schema_name', 'role']:
                    if field in data:
                        setattr(snowflake_conn, field, data[field])
                        
                # Only update password if provided
                if 'password' in data and data['password']:
                    snowflake_conn.password = data['password']
                    
                snowflake_conn.save()
                
            elif connection.connection_type == 'aws_glue' and connection.aws_glue_connection:
                aws_conn = connection.aws_glue_connection
                
                # Update AWS Glue connection fields
                for field in ['aws_region', 'access_key', 'role_arn', 'session_token']:
                    if field in data:
                        setattr(aws_conn, field, data[field])
                        
                # Only update secret key if provided
                if 'secret_key' in data and data['secret_key']:
                    aws_conn.secret_key = data['secret_key']
                    
                aws_conn.save()
                
            connection.save()
            
            return Response({
                'id': connection.id,
                'name': connection.name,
                'connection_type': connection.connection_type,
                'is_active': connection.is_active,
                'created_at': connection.created_at,
                'last_used': connection.last_used,
                'connection_details': connection.connection_details
            })
            
        except Connection.DoesNotExist:
            return Response({'error': 'Connection not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def destroy(self, request, pk=None):
        """Delete a connection"""
        try:
            connection = Connection.objects.get(id=pk)
            
            # Delete the specific connection first
            if connection.connection_type == 'snowflake' and connection.snowflake_connection:
                connection.snowflake_connection.delete()
            elif connection.connection_type == 'aws_glue' and connection.aws_glue_connection:
                connection.aws_glue_connection.delete()
                
            # Delete the universal connection
            connection.delete()
            
            return Response({'message': 'Connection deleted successfully'})
            
        except Connection.DoesNotExist:
            return Response({'error': 'Connection not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'])
    def test_connection(self, request, pk=None):
        """Test a connection"""
        try:
            connection = Connection.objects.get(id=pk)
            connection_type = connection.connection_type
            
            if connection_type == 'snowflake':
                conn_details = connection.snowflake_connection
                data = {
                    'account': conn_details.account,
                    'username': conn_details.username,
                    'password': conn_details.password,
                    'warehouse': conn_details.warehouse,
                    'database': conn_details.database_name,
                    'schema': conn_details.schema_name,
                    'role': conn_details.role
                }
                
                # Test Snowflake connection
                try:
                    import snowflake.connector
                    conn = snowflake.connector.connect(
                        account=data['account'],
                        user=data['username'],
                        password=data['password'],
                        warehouse=data['warehouse'],
                        database=data['database'],
                        schema=data['schema'],
                        role=data['role']
                    )
                    
                    # Test the connection with a simple query
                    cursor = conn.cursor()
                    cursor.execute("SELECT current_version()")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    conn.close()
                    
                    return Response({
                        'status': 'success',
                        'message': 'Snowflake connection successful',
                        'details': {'version': version}
                    })
                    
                except Exception as e:
                    return Response({
                        'status': 'error',
                        'message': f'Snowflake connection failed: {str(e)}'
                    }, status=status.HTTP_400_BAD_REQUEST)
                    
            elif connection_type == 'aws_glue':
                conn_details = connection.aws_glue_connection
                data = {
                    'aws_region': conn_details.aws_region,
                    'access_key': conn_details.access_key,
                    'secret_key': conn_details.secret_key,
                    'session_token': conn_details.session_token,
                    'role_arn': conn_details.role_arn
                }
                
                # Test AWS Glue connection
                try:
                    import boto3
                    
                    # Create a session with the provided credentials
                    session = boto3.Session(
                        aws_access_key_id=data['access_key'],
                        aws_secret_access_key=data['secret_key'],
                        aws_session_token=data['session_token'],
                        region_name=data['aws_region']
                    )
                    
                    # Create a Glue client
                    glue_client = session.client('glue')
                    
                    # Test the connection by listing databases
                    response = glue_client.get_databases()
                    
                    return Response({
                        'status': 'success',
                        'message': 'AWS Glue connection successful',
                        'details': {
                            'database_count': len(response.get('DatabaseList', []))
                        }
                    })
                    
                except Exception as e:
                    return Response({
                        'status': 'error',
                        'message': f'AWS Glue connection failed: {str(e)}'
                    }, status=status.HTTP_400_BAD_REQUEST)
                    
            else:
                return Response({
                    'status': 'error',
                    'message': f'Unsupported connection type: {connection_type}'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except Connection.DoesNotExist:
            return Response({'error': 'Connection not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'])
    def collect_metadata(self, request, pk=None):
        """Collect metadata from a connection"""
        try:
            connection = Connection.objects.get(id=pk)
            
            # Generate a process ID for tracking
            process_id = str(uuid.uuid4())
            
            # Set initial status in cache
            self._update_cache_status(process_id, {
                'status': 'initiated',
                'progress': 0,
                'message': 'Metadata collection initiated...',
                'timestamp': datetime.now().isoformat()
            })
            
            # Start metadata collection in a separate thread
            if connection.connection_type == 'snowflake':
                threading.Thread(
                    target=self._collect_snowflake_metadata,
                    args=(process_id, connection),
                    daemon=True
                ).start()
            elif connection.connection_type == 'aws_glue':
                threading.Thread(
                    target=self._collect_aws_glue_metadata,
                    args=(process_id, connection),
                    daemon=True
                ).start()
            else:
                return Response({
                    'status': 'error',
                    'message': f'Metadata collection not supported for connection type: {connection.connection_type}'
                }, status=status.HTTP_400_BAD_REQUEST)
                
            return Response({
                'status': 'processing',
                'process_id': process_id,
                'message': f'Metadata collection started for {connection.name}'
            })
            
        except Connection.DoesNotExist:
            return Response({'error': 'Connection not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['get'])
    def metadata_status(self, request, pk=None, process_id=None):
        """Get the status of a metadata collection process"""
        try:
            process_id = request.query_params.get('process_id')
            if not process_id:
                return Response({
                    'status': 'error',
                    'message': 'Process ID is required'
                }, status=status.HTTP_400_BAD_REQUEST)
                
            process_status = cache.get(f'metadata_status_{process_id}')
            if not process_status:
                return Response({
                    'status': 'not_found',
                    'message': 'Process ID not found'
                }, status=status.HTTP_404_NOT_FOUND)
                
            return Response(process_status)
            
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _collect_snowflake_metadata(self, process_id, connection):
        """Collect metadata from a Snowflake connection"""
        try:
            snowflake_conn = connection.snowflake_connection
            
            # Update status
            self._update_cache_status(process_id, {
                'status': 'processing',
                'progress': 10,
                'message': 'Connecting to Snowflake...'
            })
            
            # Create connection parameters
            connection_params = {
                'account': snowflake_conn.account,
                'username': snowflake_conn.username,
                'password': snowflake_conn.password,
                'warehouse': snowflake_conn.warehouse,
                'database': snowflake_conn.database_name,
                'schema': snowflake_conn.schema_name,
                'role': snowflake_conn.role
            }
            
            # Collect metadata
            from .snowflake_manager import SnowflakeManager
            snowflake_manager = SnowflakeManager()
            result = snowflake_manager.collect_snowflake_metadata(connection_params)
            
            if result.get('status') == 'success':
                self._update_cache_status(process_id, {
                    'status': 'completed',
                    'progress': 100,
                    'message': 'Metadata collection completed successfully',
                    'details': result.get('details', {})
                })
            else:
                self._update_cache_status(process_id, {
                    'status': 'error',
                    'progress': 0,
                    'message': result.get('message', 'Metadata collection failed')
                })
                
        except Exception as e:
            self._update_cache_status(process_id, {
                'status': 'error',
                'progress': 0,
                'message': f'Metadata collection failed: {str(e)}'
            })
    
    def _collect_aws_glue_metadata(self, process_id, connection):
        """Collect metadata from an AWS Glue connection"""
        try:
            aws_conn = connection.aws_glue_connection
            
            # Update status
            self._update_cache_status(process_id, {
                'status': 'processing',
                'progress': 10,
                'message': 'Connecting to AWS Glue...'
            })
            
            # Create connection parameters
            connection_params = {
                'aws_region': aws_conn.aws_region,
                'access_key': aws_conn.access_key,
                'secret_key': aws_conn.secret_key,
                'session_token': aws_conn.session_token,
                'role_arn': aws_conn.role_arn
            }
            
            # Collect metadata
            from .aws_glue_manager import AWSGlueManager
            aws_manager = AWSGlueManager()
            result = aws_manager.collect_aws_glue_metadata(connection_params)
            
            if result.get('status') == 'success':
                self._update_cache_status(process_id, {
                    'status': 'completed',
                    'progress': 100,
                    'message': 'Metadata collection completed successfully',
                    'details': result.get('details', {})
                })
            else:
                self._update_cache_status(process_id, {
                    'status': 'error',
                    'progress': 0,
                    'message': result.get('message', 'Metadata collection failed')
                })
                
        except Exception as e:
            self._update_cache_status(process_id, {
                'status': 'error',
                'progress': 0,
                'message': f'Metadata collection failed: {str(e)}'
            })
    
    def _update_cache_status(self, process_id, status_data, timeout=3600):
        """Helper method for updating cache with error handling"""
        try:
            if 'timestamp' not in status_data:
                status_data['timestamp'] = datetime.now().isoformat()
                
            cache.set(f'metadata_status_{process_id}', status_data, timeout=timeout)
        except Exception as e:
            print(f"Cache update failed for process {process_id}: {str(e)}")