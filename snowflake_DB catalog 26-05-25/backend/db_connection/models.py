from django.db import models

class SnowflakeConnection(models.Model):
    name = models.CharField(max_length=255, default="Default Snowflake Connection")
    account = models.CharField(max_length=255)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)
    warehouse = models.CharField(max_length=255)
    database_name = models.CharField(max_length=255, blank=True, null=True)
    schema_name = models.CharField(max_length=255, blank=True, null=True)
    role = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'snowflake_connections'
        unique_together = ('account', 'username')

    def __str__(self):
        return f"{self.name} ({self.account})"


class SnowflakeDatabase(models.Model):
    connection = models.ForeignKey(SnowflakeConnection, on_delete=models.CASCADE, related_name='databases')
    database_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'snowflake_databases'
        unique_together = ('connection', 'database_name')
    
    def __str__(self):
        return f"{self.database_name} ({self.connection.name})"


class SnowflakeSchema(models.Model):
    database = models.ForeignKey(SnowflakeDatabase, on_delete=models.CASCADE, related_name='schemas')
    schema_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'snowflake_schemas'
        unique_together = ('database', 'schema_name')
    
    def __str__(self):
        return f"{self.schema_name} ({self.database.database_name})"


class SnowflakeTable(models.Model):
    schema = models.ForeignKey(SnowflakeSchema, on_delete=models.CASCADE, related_name='tables')
    table_name = models.CharField(max_length=255)
    table_type = models.CharField(max_length=50, blank=True, null=True)
    row_count = models.BigIntegerField(blank=True, null=True)
    bytes = models.BigIntegerField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'snowflake_tables'
        unique_together = ('schema', 'table_name')
    
    def __str__(self):
        return f"{self.table_name} ({self.schema.schema_name})"


class SnowflakeColumn(models.Model):
    table = models.ForeignKey(SnowflakeTable, on_delete=models.CASCADE, related_name='columns')
    column_name = models.CharField(max_length=255)
    data_type = models.CharField(max_length=100)
    is_nullable = models.BooleanField(default=True)
    ordinal_position = models.IntegerField()
    character_maximum_length = models.IntegerField(blank=True, null=True)
    numeric_precision = models.IntegerField(blank=True, null=True)
    numeric_scale = models.IntegerField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'snowflake_columns'
        unique_together = ('table', 'column_name')
    
    def __str__(self):
        return f"{self.column_name} ({self.table.table_name})"

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'account': self.account,
            'username': self.username,
            'warehouse': self.warehouse,
            'database_name': self.database_name,
            'schema_name': self.schema_name,
            'role': self.role,
            'is_active': self.is_active,
            'created_at': self.created_at,
            'last_used': self.last_used
        }


class AWSGlueConnection(models.Model):
    name = models.CharField(max_length=255, default="Default AWS Glue Connection")
    aws_region = models.CharField(max_length=50)
    access_key = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    session_token = models.TextField(blank=True, null=True)
    role_arn = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'aws_glue_connections'
        unique_together = ('aws_region', 'access_key')

    def __str__(self):
        return f"{self.name} ({self.aws_region})"

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'aws_region': self.aws_region,
            'access_key': self.access_key,
            'role_arn': self.role_arn,
            'is_active': self.is_active,
            'created_at': self.created_at,
            'last_used': self.last_used
        }


class Connection(models.Model):
    CONNECTION_TYPES = (
        ('snowflake', 'Snowflake'),
        ('aws_glue', 'AWS Glue'),
    )
    
    name = models.CharField(max_length=255)
    connection_type = models.CharField(max_length=50, choices=CONNECTION_TYPES)
    snowflake_connection = models.ForeignKey(SnowflakeConnection, on_delete=models.CASCADE, null=True, blank=True)
    aws_glue_connection = models.ForeignKey(AWSGlueConnection, on_delete=models.CASCADE, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'connections'

    def __str__(self):
        return f"{self.name} ({self.connection_type})"

    @property
    def connection_details(self):
        if self.connection_type == 'snowflake' and self.snowflake_connection:
            return self.snowflake_connection.to_dict()
        elif self.connection_type == 'aws_glue' and self.aws_glue_connection:
            return self.aws_glue_connection.to_dict()
        return {}


class AWSGlueCatalog(models.Model):
    catalog_id = models.CharField(max_length=255, unique=True)
    catalog_name = models.CharField(max_length=255)
    catalog_description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    collected_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'aws_glue_catalogs'

    def __str__(self):
        return self.catalog_name


class AWSGlueDatabase(models.Model):
    catalog = models.ForeignKey(AWSGlueCatalog, on_delete=models.CASCADE, related_name='databases')
    database_id = models.CharField(max_length=255, unique=True)
    database_name = models.CharField(max_length=255)
    database_description = models.TextField(blank=True, null=True)
    location_uri = models.CharField(max_length=255, blank=True, null=True)
    parameters = models.JSONField(default=dict, blank=True)
    create_date = models.DateTimeField(blank=True, null=True)
    collected_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'aws_glue_databases'
        unique_together = ('catalog', 'database_name')

    def __str__(self):
        return f"{self.database_name} ({self.catalog.catalog_name})"


class AWSGlueTable(models.Model):
    database = models.ForeignKey(AWSGlueDatabase, on_delete=models.CASCADE, related_name='tables')
    table_id = models.CharField(max_length=255, unique=True)
    table_name = models.CharField(max_length=255)
    table_type = models.CharField(max_length=50, blank=True, null=True)
    table_description = models.TextField(blank=True, null=True)
    owner = models.CharField(max_length=255, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    last_access_date = models.DateTimeField(blank=True, null=True)
    last_altered_date = models.DateTimeField(blank=True, null=True)
    storage_location = models.CharField(max_length=255, blank=True, null=True)
    storage_format = models.CharField(max_length=50, blank=True, null=True)
    parameters = models.JSONField(default=dict, blank=True)
    partition_keys = models.JSONField(default=list, blank=True)
    collected_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'aws_glue_tables'
        unique_together = ('database', 'table_name')

    def __str__(self):
        return f"{self.table_name} ({self.database.database_name})"


class AWSGlueColumn(models.Model):
    table = models.ForeignKey(AWSGlueTable, on_delete=models.CASCADE, related_name='columns')
    column_id = models.CharField(max_length=255, unique=True)
    column_name = models.CharField(max_length=255)
    ordinal_position = models.IntegerField(blank=True, null=True)
    data_type = models.CharField(max_length=100, blank=True, null=True)
    column_description = models.TextField(blank=True, null=True)
    is_nullable = models.BooleanField(default=True)
    is_partition_key = models.BooleanField(default=False)
    parameters = models.JSONField(default=dict, blank=True)
    collected_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'aws_glue_columns'
        unique_together = ('table', 'column_name')

    def __str__(self):
        return f"{self.column_name} ({self.table.table_name})"