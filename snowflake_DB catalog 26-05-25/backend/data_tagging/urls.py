from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import TagViewSet, TaggedItemViewSet
from . import views

router = DefaultRouter()
router.register(r'tags', TagViewSet)
router.register(r'tagged-items', TaggedItemViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path("add-tag/", views.add_tag_to_column, name="add-tag"),
    path("tag-suggestions/", views.get_tag_suggestions, name="tag-suggestions"),
] 