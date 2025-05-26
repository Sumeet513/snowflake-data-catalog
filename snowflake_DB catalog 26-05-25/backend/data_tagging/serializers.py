from rest_framework import serializers
from .models import Tag, TaggedItem

class TagSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tag
        fields = '__all__'

class TaggedItemSerializer(serializers.ModelSerializer):
    tag_name = serializers.CharField(source='tag.name', read_only=True)
    tag_color = serializers.CharField(source='tag.color', read_only=True)
    
    class Meta:
        model = TaggedItem
        fields = '__all__'
        
    def to_representation(self, instance):
        representation = super().to_representation(instance)
        # Add tag information directly to the representation
        representation['tag_info'] = {
            'id': instance.tag.id,
            'name': instance.tag.name,
            'color': instance.tag.color,
            'description': instance.tag.description
        }
        return representation 