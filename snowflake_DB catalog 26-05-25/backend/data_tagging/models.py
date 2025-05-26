from django.db import models

# Create your models here.

class Tag(models.Model):
    name = models.CharField(max_length=100, unique=True)
    color = models.CharField(max_length=20, default="#3498db")  # Hex color code
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

class TaggedItem(models.Model):
    TAG_TYPES = (
        ('database', 'Database'),
        ('schema', 'Schema'),
        ('table', 'Table'),
        ('column', 'Column'),
    )
    
    tag = models.ForeignKey(Tag, on_delete=models.CASCADE, related_name='tagged_items')
    object_type = models.CharField(max_length=20, choices=TAG_TYPES)
    database_name = models.CharField(max_length=255)
    schema_name = models.CharField(max_length=255, blank=True, null=True)
    table_name = models.CharField(max_length=255, blank=True, null=True)
    column_name = models.CharField(max_length=255, blank=True, null=True)
    tagged_by = models.CharField(max_length=255, blank=True, null=True)  # User who created the tag
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ('tag', 'object_type', 'database_name', 'schema_name', 'table_name', 'column_name')
        indexes = [
            models.Index(fields=['object_type']),
            models.Index(fields=['database_name']),
            models.Index(fields=['schema_name']),
            models.Index(fields=['table_name']),
        ]
    
    def __str__(self):
        path = self.database_name
        if self.schema_name:
            path += f".{self.schema_name}"
        if self.table_name:
            path += f".{self.table_name}"
        if self.column_name:
            path += f".{self.column_name}"
        return f"{self.tag.name} -> {self.object_type}: {path}"


class ColumnTag(models.Model):
    database = models.CharField(max_length=200)
    schema = models.CharField(max_length=200)
    table = models.CharField(max_length=200)
    column_name = models.CharField(max_length=200)
    tag = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.table}.{self.column_name} - {self.tag}"
