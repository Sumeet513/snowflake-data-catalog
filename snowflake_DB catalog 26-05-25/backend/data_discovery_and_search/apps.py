from django.apps import AppConfig

class DataDiscoveryAndSearchConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'data_discovery_and_search'
    verbose_name = 'Data Discovery and Search'
    
    def ready(self):
        # Import signals or perform other initialization
        pass 