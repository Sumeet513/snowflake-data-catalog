from django.urls import path
from . import natural_language_query, semantic_search

urlpatterns = [
    path('api/natural-language-query/', natural_language_query.natural_language_query_endpoint, name='natural_language_query'),
    path('api/db/natural-language-query/', natural_language_query.natural_language_query_endpoint, name='natural_language_query_old_path'),
    path('api/semantic-search/', semantic_search.semantic_search_endpoint, name='semantic_search'),
    path('api/db/semantic-search/', semantic_search.semantic_search_endpoint, name='semantic_search_alt_path'),
]