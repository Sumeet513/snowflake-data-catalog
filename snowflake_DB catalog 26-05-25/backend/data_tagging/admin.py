from django.contrib import admin
from .models import Tag, TaggedItem

@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ('name', 'color', 'description', 'created_at', 'updated_at')
    search_fields = ('name', 'description')
    list_filter = ('created_at', 'updated_at')

@admin.register(TaggedItem)
class TaggedItemAdmin(admin.ModelAdmin):
    list_display = ('tag', 'object_type', 'database_name', 'schema_name', 'table_name', 'column_name', 'created_at')
    search_fields = ('tag__name', 'database_name', 'schema_name', 'table_name', 'column_name')
    list_filter = ('object_type', 'created_at')
    raw_id_fields = ('tag',)
