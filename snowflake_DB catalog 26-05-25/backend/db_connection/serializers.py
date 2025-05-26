from rest_framework import serializers
from .models import (
    SnowflakeConnection, 
    AWSGlueConnection, 
    Connection,
    AWSGlueCatalog,
    AWSGlueDatabase,
    AWSGlueTable,
    AWSGlueColumn
)

class SnowflakeConnectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = SnowflakeConnection
        fields = ['id', 'name', 'account', 'username', 'warehouse', 'database_name', 'schema_name', 'role', 'is_active', 'created_at', 'last_used']
        
    # Don't return the password in responses
    extra_kwargs = {
        'password': {'write_only': True}
    }

class AWSGlueConnectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = AWSGlueConnection
        fields = ['id', 'name', 'aws_region', 'access_key', 'role_arn', 'is_active', 'created_at', 'last_used']
        
    # Don't return the secret keys in responses
    extra_kwargs = {
        'secret_key': {'write_only': True},
        'session_token': {'write_only': True}
    }

class ConnectionSerializer(serializers.ModelSerializer):
    connection_details = serializers.SerializerMethodField()
    
    class Meta:
        model = Connection
        fields = ['id', 'name', 'connection_type', 'is_active', 'created_at', 'last_used', 'connection_details']
    
    def get_connection_details(self, obj):
        """Return the specific connection details based on connection_type"""
        if obj.connection_type == 'snowflake':
            return SnowflakeConnectionSerializer(obj.snowflake_connection).data
        elif obj.connection_type == 'aws_glue':
            return AWSGlueConnectionSerializer(obj.aws_glue_connection).data
        return None

class AWSGlueCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = AWSGlueCatalog
        fields = ['id', 'catalog_id', 'catalog_name', 'catalog_description', 'created_at', 'collected_at']

class AWSGlueDatabaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = AWSGlueDatabase
        fields = ['id', 'database_id', 'catalog', 'database_name', 'database_description', 
                 'location_uri', 'parameters', 'create_date', 'collected_at']

class AWSGlueTableSerializer(serializers.ModelSerializer):
    class Meta:
        model = AWSGlueTable
        fields = ['id', 'table_id', 'database', 'table_name', 'table_type', 'table_description',
                 'owner', 'create_date', 'last_access_date', 'last_altered_date',
                 'storage_location', 'storage_format', 'parameters', 'partition_keys', 'collected_at']

class AWSGlueColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = AWSGlueColumn
        fields = ['id', 'column_id', 'table', 'column_name', 'ordinal_position', 'data_type',
                 'column_description', 'is_nullable', 'is_partition_key', 'parameters', 'collected_at']