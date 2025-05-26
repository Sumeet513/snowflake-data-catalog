from django.db import models

class SearchHistory(models.Model):
    """Model to store natural language query search history"""
    query = models.TextField(help_text="Natural language query")
    generated_sql = models.TextField(help_text="Generated SQL query", null=True, blank=True)
    user_identifier = models.CharField(max_length=64, help_text="User identifier", null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    successful = models.BooleanField(default=True)
    
    class Meta:
        verbose_name_plural = "Search histories"
        ordering = ['-timestamp']
    
    def __str__(self):
        return f"{self.query[:50]}... ({self.timestamp.strftime('%Y-%m-%d %H:%M')})" 