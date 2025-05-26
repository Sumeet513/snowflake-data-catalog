from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views
from . import external_views
from . import connection_views

# Create a router for the connection viewset
router = DefaultRouter()
router.register(r'connections', connection_views.ConnectionViewSet, basename='connection')

urlpatterns = [
    # Include the router URLs
    path('', include(router.urls)),
    
    # Original endpoints
    path('static/databases/', views.get_databases, name='get_databases'),
    path('static/databases/<str:database>/schemas/', views.get_schemas, name='get_schemas'),
    path('static/databases/<str:database>/schemas/<str:schema>/tables/', views.get_tables, name='get_tables'),
    path('static/databases/<str:database>/schemas/<str:schema>/tables/<str:table>/columns/',
         views.get_table_columns, name='get_table_columns'),
    path('test-connection/', views.test_connection, name='test_connection'),
    path('databases/', views.get_databases_dynamic, name='get_databases_dynamic'),
    path('schemas/', views.get_schemas_for_database, name='get_schemas_for_database'),
    path('tables/', views.get_tables_for_schema, name='get_tables_for_schema'),
    path('columns/', views.get_columns_dynamic, name='get_columns_dynamic'),
    path('table-constraints/', views.get_table_constraints, name='get_table_constraints'),
    path('search-tables/', views.search_tables, name='search_tables'),
    path('snowflake/search-tables/', views.search_tables, name='snowflake_search_tables'),
    path('generate-ai-tags-glossary/', views.generate_ai_tags_and_glossary, name='generate_ai_tags_and_glossary'),
    path('view-metadata-enrichment/', views.view_metadata_enrichment, name='view_metadata_enrichment'),
    path('ext/connections/', external_views.list_saved_connections, name='list_saved_connections'),
    path('ext/databases/', external_views.list_saved_databases, name='list_saved_databases'),
    path('ext/schemas/', external_views.list_saved_schemas, name='list_saved_schemas'),
    path('ext/tables/', external_views.list_saved_tables, name='list_saved_tables'),
    path('ext/columns/', external_views.list_saved_columns, name='list_saved_columns'),
    path('ext/tables/<str:table_id>/', external_views.get_table_details, name='get_table_details'),
    path('ext/search/', external_views.search_by_keyword, name='search_by_keyword'),
    path('ext/generate-ai-metadata/', external_views.generate_metadata_with_ai, name='generate_metadata_with_ai'),
    path('ext/view-metadata-enrichment/', external_views.view_metadata_enrichment, name='ext_view_metadata_enrichment'),
    
    # New endpoints for metadata collection
    path('snowflake/collect-metadata/', views.collect_metadata, name='collect_metadata'),
    path('snowflake/metadata-status/<str:process_id>/', views.get_metadata_status, name='get_metadata_status'),
    
    # Universal connection management endpoints
    # Most endpoints are handled by the ConnectionViewSet
    path('connection-test/', connection_views.test_connection, name='universal_test_connection'),
    path("profile-table/", views.get_table_profile, name="profile-table"),
]