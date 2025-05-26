from django.shortcuts import render
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from .models import Tag, TaggedItem
from .serializers import TagSerializer, TaggedItemSerializer

from rest_framework.decorators import api_view
from .models import ColumnTag
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json

# Create your views here.

@api_view(["GET"])
def get_tag_suggestions(request):
    # Example implementation
    suggestions = ["PII", "Confidential", "Sensitive", "Public", "Internal"]
    return JsonResponse({"suggestions": suggestions})

class TagViewSet(viewsets.ModelViewSet):
    queryset = Tag.objects.all()
    serializer_class = TagSerializer
    
    @action(detail=False, methods=['GET'])
    def search(self, request):
        name = request.query_params.get('name', '')
        tags = Tag.objects.filter(name__icontains=name)
        serializer = self.get_serializer(tags, many=True)
        return Response(serializer.data)

class TaggedItemViewSet(viewsets.ModelViewSet):
    queryset = TaggedItem.objects.all()
    serializer_class = TaggedItemSerializer
    
    def get_queryset(self):
        queryset = TaggedItem.objects.all()
        
        # Filter by object type
        object_type = self.request.query_params.get('object_type')
        if object_type:
            queryset = queryset.filter(object_type=object_type)
            
        # Filter by database name
        database_name = self.request.query_params.get('database_name')
        if database_name:
            queryset = queryset.filter(database_name=database_name)
            
        # Filter by schema name
        schema_name = self.request.query_params.get('schema_name')
        if schema_name:
            queryset = queryset.filter(schema_name=schema_name)
            
        # Filter by table name
        table_name = self.request.query_params.get('table_name')
        if table_name:
            queryset = queryset.filter(table_name=table_name)
            
        # Filter by column name
        column_name = self.request.query_params.get('column_name')
        if column_name:
            queryset = queryset.filter(column_name=column_name)
            
        # Filter by tag ID
        tag_id = self.request.query_params.get('tag_id')
        if tag_id:
            queryset = queryset.filter(tag_id=tag_id)
            
        return queryset
        
    @action(detail=False, methods=['POST'])
    def bulk_create(self, request):
        """Create multiple tagged items at once"""
        serializer = self.get_serializer(data=request.data, many=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
    @action(detail=False, methods=['DELETE'])
    def remove_tags(self, request):
        """Remove tags from database objects"""
        object_type = request.data.get('object_type')
        database_name = request.data.get('database_name')
        schema_name = request.data.get('schema_name')
        table_name = request.data.get('table_name')
        column_name = request.data.get('column_name')
        tag_ids = request.data.get('tag_ids', [])
        
        if not object_type or not database_name:
            return Response(
                {"error": "object_type and database_name are required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
            
        filters = {
            'object_type': object_type,
            'database_name': database_name,
        }
        
        if schema_name:
            filters['schema_name'] = schema_name
        if table_name:
            filters['table_name'] = table_name
        if column_name:
            filters['column_name'] = column_name
            
        if tag_ids:
            filters['tag_id__in'] = tag_ids
            
        deleted_count, _ = TaggedItem.objects.filter(**filters).delete()
        
        return Response({
            "message": f"Successfully deleted {deleted_count} tagged items",
            "count": deleted_count
        })

@api_view(["POST"])
def add_tag_to_column(request):
    try:
        data = request.data
        # Create the tag
        ColumnTag.objects.create(
            database=data["database"],
            schema=data["schema"],
            table=data["table"],
            column_name=data["column_name"],
            tag=data["tag"]
        )
        return Response({"message": "Tag added successfully"})
    except KeyError as e:
        return Response({"error": f"Missing required field: {str(e)}"}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@csrf_exempt
@api_view(["POST"])
def get_column_tags(request):
    """Get all tags for columns in a table"""
    try:
        # Handle both JSON and form data
        if request.content_type == 'application/json':
            data = json.loads(request.body) if isinstance(request.body, bytes) else request.body
        else:
            data = request.data
            
        database = data.get('database')
        schema = data.get('schema')
        table = data.get('table')
        
        if not all([database, schema, table]):
            return JsonResponse({"error": "Database, schema, and table are required"}, status=400)
        
        # Query the database for tags
        tags = ColumnTag.objects.filter(
            database=database,
            schema=schema,
            table=table
        )
        
        # Group tags by column
        column_tags = {}
        for tag in tags:
            if tag.column_name not in column_tags:
                column_tags[tag.column_name] = []
            column_tags[tag.column_name].append(tag.tag)
        
        return JsonResponse({"column_tags": column_tags})
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
