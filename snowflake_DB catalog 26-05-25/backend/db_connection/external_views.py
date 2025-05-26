from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.core.paginator import Paginator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from .models import (
    SnowflakeConnection, 
    SnowflakeDatabase, 
    SnowflakeSchema, 
    SnowflakeTable, 
    SnowflakeColumn
)
import json

@api_view(['GET'])
def list_saved_connections(request):
    """List all saved connections from the database"""
    try:
        connections = SnowflakeConnection.objects.all().values(
            'id', 'name', 'account', 'username', 'warehouse', 
            'database_name', 'schema_name', 'created_at', 'last_used'
        )
        
        # Convert to list and process datetime objects
        connection_list = []
        for conn in connections:
            # Convert datetime objects to ISO format strings
            conn['created_at'] = conn['created_at'].isoformat() if conn['created_at'] else None
            conn['last_used'] = conn['last_used'].isoformat() if conn['last_used'] else None
            connection_list.append(conn)
            
        return Response({
            'status': 'success',
            'connections': connection_list
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def list_saved_databases(request):
    """List all saved databases from the external database"""
    try:
        # Get pagination parameters
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 10))
        
        # Get all databases
        databases = SnowflakeDatabase.objects.all().order_by('database_name')
        
        # Paginate the results
        paginator = Paginator(databases, page_size)
        page_obj = paginator.get_page(page)
        
        # Convert to list of dictionaries
        database_list = []
        for db in page_obj:
            database_list.append({
                'id': db.id,
                'database_id': db.database_id,
                'database_name': db.database_name,
                'database_owner': db.database_owner,
                'database_description': db.database_description,
                'create_date': db.create_date.isoformat() if db.create_date else None,
                'last_altered_date': db.last_altered_date.isoformat() if db.last_altered_date else None,
                'comment': db.comment,
                'tags': db.tags,
                'collected_at': db.collected_at.isoformat()
            })
            
        return Response({
            'status': 'success',
            'databases': database_list,
            'total_count': paginator.count,
            'page': page,
            'page_size': page_size,
            'total_pages': paginator.num_pages
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def list_saved_schemas(request):
    """List all saved schemas for a database"""
    try:
        # Get database ID from query parameters
        database_id = request.GET.get('database_id')
        
        # Get pagination parameters
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 10))
        
        # Query schemas
        schemas_query = SnowflakeSchema.objects.select_related('database')
        
        # Filter by database if specified
        if database_id:
            try:
                database_id = int(database_id)
                schemas_query = schemas_query.filter(database_id=database_id)
            except ValueError:
                # If database_id is not a valid integer, try with database_id field
                schemas_query = schemas_query.filter(database__database_id=database_id)
        
        # Order the results
        schemas_query = schemas_query.order_by('database__database_name', 'schema_name')
        
        # Paginate the results
        paginator = Paginator(schemas_query, page_size)
        page_obj = paginator.get_page(page)
        
        # Convert to list of dictionaries
        schema_list = []
        for schema in page_obj:
            schema_list.append({
                'id': schema.id,
                'schema_id': schema.schema_id,
                'schema_name': schema.schema_name,
                'database_id': schema.database.id,
                'database_name': schema.database.database_name,
                'schema_owner': schema.schema_owner,
                'schema_description': schema.schema_description,
                'create_date': schema.create_date.isoformat() if schema.create_date else None,
                'last_altered_date': schema.last_altered_date.isoformat() if schema.last_altered_date else None,
                'comment': schema.comment,
                'tags': schema.tags,
                'collected_at': schema.collected_at.isoformat()
            })
            
        return Response({
            'status': 'success',
            'schemas': schema_list,
            'total_count': paginator.count,
            'page': page,
            'page_size': page_size,
            'total_pages': paginator.num_pages
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def list_saved_tables(request):
    """List all saved tables for a schema"""
    try:
        # Get schema ID from query parameters
        schema_id = request.GET.get('schema_id')
        database_id = request.GET.get('database_id')
        search_query = request.GET.get('search', '')
        
        # Get pagination parameters
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 10))
        
        # Query tables
        tables_query = SnowflakeTable.objects.select_related('schema__database')
        
        # Apply filters
        if schema_id:
            try:
                schema_id = int(schema_id)
                tables_query = tables_query.filter(schema_id=schema_id)
            except ValueError:
                # If schema_id is not a valid integer, try with schema_id field
                tables_query = tables_query.filter(schema__schema_id=schema_id)
        
        if database_id:
            try:
                database_id = int(database_id)
                tables_query = tables_query.filter(schema__database_id=database_id)
            except ValueError:
                # If database_id is not a valid integer, try with database_id field
                tables_query = tables_query.filter(schema__database__database_id=database_id)
        
        # Search by table name or description
        if search_query:
            tables_query = tables_query.filter(
                table_name__icontains=search_query
            ) | tables_query.filter(
                table_description__icontains=search_query
            )
        
        # Order the results
        tables_query = tables_query.order_by('schema__database__database_name', 'schema__schema_name', 'table_name')
        
        # Paginate the results
        paginator = Paginator(tables_query, page_size)
        page_obj = paginator.get_page(page)
        
        # Convert to list of dictionaries
        table_list = []
        for table in page_obj:
            table_list.append({
                'id': table.id,
                'table_id': table.table_id,
                'table_name': table.table_name,
                'schema_id': table.schema.id,
                'schema_name': table.schema.schema_name,
                'database_id': table.schema.database.id,
                'database_name': table.schema.database.database_name,
                'table_type': table.table_type,
                'table_owner': table.table_owner,
                'table_description': table.table_description,
                'row_count': table.row_count,
                'byte_size': table.byte_size,
                'create_date': table.create_date.isoformat() if table.create_date else None,
                'last_altered_date': table.last_altered_date.isoformat() if table.last_altered_date else None,
                'comment': table.comment,
                'tags': table.tags,
                'keywords': table.keywords,
                'collected_at': table.collected_at.isoformat()
            })
            
        return Response({
            'status': 'success',
            'tables': table_list,
            'total_count': paginator.count,
            'page': page,
            'page_size': page_size,
            'total_pages': paginator.num_pages
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def list_saved_columns(request):
    """List all saved columns for a table"""
    try:
        # Get table ID from query parameters
        table_id = request.GET.get('table_id')
        search_query = request.GET.get('search', '')
        
        # Get pagination parameters
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 20))
        
        # Query columns
        columns_query = SnowflakeColumn.objects.select_related('table__schema__database')
        
        # Filter by table if specified
        if table_id:
            try:
                table_id = int(table_id)
                columns_query = columns_query.filter(table_id=table_id)
            except ValueError:
                # If table_id is not a valid integer, try with table_id field
                columns_query = columns_query.filter(table__table_id=table_id)
        
        # Search by column name or description
        if search_query:
            columns_query = columns_query.filter(
                column_name__icontains=search_query
            ) | columns_query.filter(
                column_description__icontains=search_query
            )
        
        # Order the results
        columns_query = columns_query.order_by('table__table_name', 'ordinal_position')
        
        # Paginate the results
        paginator = Paginator(columns_query, page_size)
        page_obj = paginator.get_page(page)
        
        # Convert to list of dictionaries
        column_list = []
        for column in page_obj:
            column_list.append({
                'id': column.id,
                'column_id': column.column_id,
                'column_name': column.column_name,
                'table_id': column.table.id,
                'table_name': column.table.table_name,
                'schema_id': column.table.schema.id,
                'schema_name': column.table.schema.schema_name,
                'database_id': column.table.schema.database.id,
                'database_name': column.table.schema.database.database_name,
                'ordinal_position': column.ordinal_position,
                'data_type': column.data_type,
                'column_description': column.column_description,
                'comment': column.comment,
                'is_nullable': column.is_nullable,
                'is_primary_key': column.is_primary_key,
                'is_foreign_key': column.is_foreign_key,
                'is_pii': column.is_pii,
                'sensitivity_level': column.sensitivity_level,
                'tags': column.tags,
                'collected_at': column.collected_at.isoformat()
            })
            
        return Response({
            'status': 'success',
            'columns': column_list,
            'total_count': paginator.count,
            'page': page,
            'page_size': page_size,
            'total_pages': paginator.num_pages
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def get_table_details(request, table_id):
    """Get detailed information about a specific table"""
    try:
        # Try to get the table
        table = None
        try:
            # Try with primary key
            table_id_int = int(table_id)
            table = SnowflakeTable.objects.get(id=table_id_int)
        except ValueError:
            # Try with table_id field
            table = SnowflakeTable.objects.get(table_id=table_id)
        
        if not table:
            return Response({
                'status': 'error',
                'message': f'Table with ID {table_id} not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Get columns for this table
        columns = SnowflakeColumn.objects.filter(table=table).order_by('ordinal_position')
        
        # Convert to dictionary
        table_data = {
            'id': table.id,
            'table_id': table.table_id,
            'table_name': table.table_name,
            'schema_id': table.schema.id,
            'schema_name': table.schema.schema_name,
            'database_id': table.schema.database.id,
            'database_name': table.schema.database.database_name,
            'table_type': table.table_type,
            'table_owner': table.table_owner,
            'table_description': table.table_description,
            'row_count': table.row_count,
            'byte_size': table.byte_size,
            'create_date': table.create_date.isoformat() if table.create_date else None,
            'last_altered_date': table.last_altered_date.isoformat() if table.last_altered_date else None,
            'comment': table.comment,
            'tags': table.tags,
            'keywords': table.keywords,
            'business_glossary_terms': table.business_glossary_terms,
            'sensitivity_level': table.sensitivity_level,
            'data_domain': table.data_domain,
            'collected_at': table.collected_at.isoformat(),
            'columns': []
        }
        
        # Add columns to the table data
        for column in columns:
            table_data['columns'].append({
                'id': column.id,
                'column_id': column.column_id,
                'column_name': column.column_name,
                'ordinal_position': column.ordinal_position,
                'data_type': column.data_type,
                'character_maximum_length': column.character_maximum_length,
                'numeric_precision': column.numeric_precision,
                'numeric_scale': column.numeric_scale,
                'is_nullable': column.is_nullable,
                'column_default': column.column_default,
                'column_description': column.column_description,
                'comment': column.comment,
                'is_primary_key': column.is_primary_key,
                'is_foreign_key': column.is_foreign_key,
                'is_pii': column.is_pii,
                'sensitivity_level': column.sensitivity_level,
                'tags': column.tags,
                'distinct_values': column.distinct_values,
                'null_count': column.null_count
            })
            
        return Response({
            'status': 'success',
            'table': table_data
        })
    except SnowflakeTable.DoesNotExist:
        return Response({
            'status': 'error',
            'message': f'Table with ID {table_id} not found'
        }, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def search_by_keyword(request):
    """Search for tables or columns by keyword"""
    try:
        query = request.GET.get('q', '')
        if not query:
            return Response({
                'status': 'error',
                'message': 'Search query is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Search tables
        tables = SnowflakeTable.objects.filter(
            table_name__icontains=query
        ) | SnowflakeTable.objects.filter(
            table_description__icontains=query
        )
        
        # Search columns
        columns = SnowflakeColumn.objects.filter(
            column_name__icontains=query
        ) | SnowflakeColumn.objects.filter(
            column_description__icontains=query
        )
        
        # Convert tables to dictionaries
        table_results = []
        for table in tables[:10]:  # Limit to 10 results
            table_results.append({
                'id': table.id,
                'type': 'table',
                'name': table.table_name,
                'description': table.table_description,
                'database': table.schema.database.database_name,
                'schema': table.schema.schema_name,
                'database_id': table.schema.database.id,
                'schema_id': table.schema.id,
                'table_id': table.id,
                'full_path': f"{table.schema.database.database_name}.{table.schema.schema_name}.{table.table_name}"
            })
        
        # Convert columns to dictionaries
        column_results = []
        for column in columns[:10]:  # Limit to 10 results
            column_results.append({
                'id': column.id,
                'type': 'column',
                'name': column.column_name,
                'description': column.column_description,
                'table': column.table.table_name,
                'database': column.table.schema.database.database_name,
                'schema': column.table.schema.schema_name,
                'database_id': column.table.schema.database.id,
                'schema_id': column.table.schema.id,
                'table_id': column.table.id,
                'full_path': f"{column.table.schema.database.database_name}.{column.table.schema.schema_name}.{column.table.table_name}.{column.column_name}"
            })
        
        return Response({
            'status': 'success',
            'query': query,
            'tables': table_results,
            'columns': column_results,
            'total_tables': tables.count(),
            'total_columns': columns.count()
        })
    except Exception as e:
        return Response({
            'status': 'error',
            'message': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
def generate_metadata_with_ai(request):
    """Generate tags and business glossary terms for saved database objects using AI"""
    try:
        # Extract parameters
        connection_id = request.data.get('connection_id')
        batch_size = request.data.get('batch_size', 5)
        ai_api_key = request.data.get('ai_api_key')
        ai_provider = request.data.get('ai_provider', 'openai')
        
        # Validate required fields
        if not connection_id:
            return Response(
                {'message': 'Connection ID is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not ai_api_key:
            return Response(
                {'message': 'AI API key is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get connection details
        try:
            connection = SnowflakeConnection.objects.get(id=connection_id)
        except SnowflakeConnection.DoesNotExist:
            return Response(
                {'message': f'Connection with ID {connection_id} not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Prepare connection parameters
        connection_params = {
            'account': connection.account,
            'user': connection.username,
            'password': connection.password,
            'warehouse': connection.warehouse,
            'database': connection.database_name,
            'schema': connection.schema_name,
            'role': connection.role
        }
        
        # Initialize Snowflake Manager
        from .snowflake_manager import SnowflakeManager
        manager = SnowflakeManager(ai_api_key=ai_api_key, ai_provider=ai_provider)
        
        # Generate tags and glossary
        results = manager.generate_tags_and_glossary(connection_params, batch_size)
        
        return Response(results)
    except Exception as e:
        return Response(
            {'message': f'Error generating metadata with AI: {str(e)}'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@api_view(['GET', 'POST'])
def view_metadata_enrichment(request):
    """View the tags, business glossary terms and descriptions for database objects"""
    try:
        # Handle POST requests with filter criteria
        if request.method == 'POST':
            database_id = request.data.get('database_id')
            schema_id = request.data.get('schema_id')
            table_id = request.data.get('table_id')
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 10))
        else:
            # For GET requests
            database_id = request.GET.get('database_id')
            schema_id = request.GET.get('schema_id')
            table_id = request.GET.get('table_id')
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 10))
        
        # Get filtered tables with non-empty metadata
        tables_query = SnowflakeTable.objects.select_related('schema__database')
        
        # Apply filters if provided
        if database_id:
            try:
                database_id = int(database_id)
                tables_query = tables_query.filter(schema__database_id=database_id)
            except ValueError:
                tables_query = tables_query.filter(schema__database__database_id=database_id)
        
        if schema_id:
            try:
                schema_id = int(schema_id)
                tables_query = tables_query.filter(schema_id=schema_id)
            except ValueError:
                tables_query = tables_query.filter(schema__schema_id=schema_id)
        
        if table_id:
            try:
                table_id = int(table_id)
                tables_query = tables_query.filter(pk=table_id)
            except ValueError:
                tables_query = tables_query.filter(table_id=table_id)
        
        # Filter to tables that have at least some enriched metadata
        tables_with_metadata = tables_query.exclude(
            table_description__isnull=True, 
            tags={}, 
            business_glossary_terms=[]
        ).order_by('schema__database__database_name', 'schema__schema_name', 'table_name')
        
        # Prepare summary data
        metadata_results = []
        total_count = tables_with_metadata.count()
        
        # Paginate
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paged_tables = tables_with_metadata[start_idx:end_idx]
        
        # Convert to response format
        for table in paged_tables:
            metadata_results.append({
                'table_id': table.table_id,
                'table_name': table.table_name,
                'schema_name': table.schema.schema_name,
                'database_name': table.schema.database.database_name,
                'full_name': f"{table.schema.database.database_name}.{table.schema.schema_name}.{table.table_name}",
                'description': table.table_description,
                'tags': table.tags,
                'business_glossary_terms': table.business_glossary_terms,
                'data_domain': table.data_domain,
                'keywords': table.keywords,
                'sensitivity_level': table.sensitivity_level
            })
        
        # Also get database level metadata if requested
        databases_metadata = []
        if database_id and not table_id:
            db_query = SnowflakeDatabase.objects
            
            if isinstance(database_id, int):
                db = db_query.filter(pk=database_id).first()
            else:
                db = db_query.filter(database_id=database_id).first()
            
            if db and (db.database_description or db.tags):
                databases_metadata.append({
                    'database_id': db.database_id,
                    'database_name': db.database_name,
                    'description': db.database_description,
                    'tags': db.tags
                })
        
        # Get column metadata if a specific table is requested
        columns_metadata = []
        if table_id:
            columns = SnowflakeColumn.objects.filter(table__table_id=table_id)
            for column in columns:
                if column.column_description or column.tags:
                    columns_metadata.append({
                        'column_id': column.column_id,
                        'column_name': column.column_name,
                        'data_type': column.data_type,
                        'description': column.column_description,
                        'tags': column.tags,
                        'is_pii': column.is_pii,
                        'sensitivity_level': column.sensitivity_level
                    })
        
        return Response({
            'status': 'success',
            'tables_metadata': metadata_results,
            'databases_metadata': databases_metadata,
            'columns_metadata': columns_metadata,
            'total_count': total_count,
            'page': page,
            'page_size': page_size,
            'total_pages': (total_count + page_size - 1) // page_size
        })
    except Exception as e:
        return Response(
            {'status': 'error', 'message': f'Error retrieving metadata: {str(e)}'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) 