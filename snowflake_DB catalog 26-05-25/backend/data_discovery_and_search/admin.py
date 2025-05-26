from django.contrib import admin
from .models import SearchHistory

@admin.register(SearchHistory)
class SearchHistoryAdmin(admin.ModelAdmin):
    list_display = ('query', 'timestamp', 'successful', 'user_identifier')
    list_filter = ('successful', 'timestamp')
    search_fields = ('query', 'generated_sql', 'user_identifier')
    readonly_fields = ('timestamp',)
    fieldsets = (
        (None, {
            'fields': ('query', 'generated_sql', 'user_identifier', 'successful')
        }),
        ('Metadata', {
            'fields': ('timestamp',),
            'classes': ('collapse',)
        }),
    ) 