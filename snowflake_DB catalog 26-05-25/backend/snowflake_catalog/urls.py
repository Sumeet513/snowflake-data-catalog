"""
URL configuration for snowflake_catalog project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from db_connection.viewsets import SnowflakeViewSet

# Create a router and register our ViewSet
router = DefaultRouter()
router.register(r'snowflake', SnowflakeViewSet, basename='snowflake')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include(router.urls)),  # This includes the ViewSet URLs
    path('api/', include('db_connection.urls')),  # This includes the function-based view URLs
    path('api/tagging/', include('data_tagging.urls')),  # Include the data tagging URLs
    path('', include('data_discovery_and_search.urls')),  # Include the data discovery and search URLs
    path('db_connection/', include('db_connection.urls')),
    path('data_tagging/', include('data_tagging.urls')),
]
