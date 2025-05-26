# Data Tagging App

This Django app provides functionality for tagging database objects (databases, schemas, tables, and columns) in Snowflake. It allows metadata to be enriched with business context and classification.

## Features

- Create, update, and delete tags
- Apply tags to database objects (databases, schemas, tables, columns)
- Search and filter objects by tags
- Bulk tagging operations
- REST API for integration with the frontend

## Models

### Tag

Represents a tag that can be applied to any database object.

- `name`: The name of the tag (unique)
- `color`: Color code for the tag (e.g., #3498db)
- `description`: Optional description of the tag
- `created_at`: Timestamp when the tag was created
- `updated_at`: Timestamp when the tag was last updated

### TaggedItem

Represents a relationship between a tag and a database object.

- `tag`: Foreign key to the Tag model
- `object_type`: Type of database object (database, schema, table, column)
- `database_name`: Name of the database
- `schema_name`: Name of the schema (optional, depends on object_type)
- `table_name`: Name of the table (optional, depends on object_type)
- `column_name`: Name of the column (optional, depends on object_type)
- `tagged_by`: User who created the tag (optional)
- `created_at`: Timestamp when the tagged item was created

## API Endpoints

### Tags

- `GET /api/tagging/tags/` - List all tags
- `POST /api/tagging/tags/` - Create a new tag
- `GET /api/tagging/tags/{id}/` - Retrieve a specific tag
- `PUT /api/tagging/tags/{id}/` - Update a specific tag
- `DELETE /api/tagging/tags/{id}/` - Delete a specific tag
- `GET /api/tagging/tags/search/` - Search tags by name

### Tagged Items

- `GET /api/tagging/tagged-items/` - List all tagged items (with filtering options)
- `POST /api/tagging/tagged-items/` - Apply a tag to a database object
- `GET /api/tagging/tagged-items/{id}/` - Retrieve a specific tagged item
- `PUT /api/tagging/tagged-items/{id}/` - Update a specific tagged item
- `DELETE /api/tagging/tagged-items/{id}/` - Delete a specific tagged item
- `POST /api/tagging/tagged-items/bulk-create/` - Apply tags to multiple database objects
- `DELETE /api/tagging/tagged-items/remove-tags/` - Remove tags from database objects

## Usage Examples

### Creating a Tag

```python
# Python example
import requests

response = requests.post(
    "http://localhost:8000/api/tagging/tags/",
    json={
        "name": "PII",
        "color": "#FF5733",
        "description": "Personally Identifiable Information"
    }
)
print(response.json())
```

### Tagging a Table

```python
# Python example
import requests

response = requests.post(
    "http://localhost:8000/api/tagging/tagged-items/",
    json={
        "tag": 1,  # Tag ID
        "object_type": "table",
        "database_name": "SNOWFLAKE_SAMPLE_DATA",
        "schema_name": "TPCH_SF1",
        "table_name": "CUSTOMER",
        "tagged_by": "admin"
    }
)
print(response.json())
```

### Retrieving Tags for a Table

```python
# Python example
import requests

response = requests.get(
    "http://localhost:8000/api/tagging/tagged-items/",
    params={
        "object_type": "table",
        "database_name": "SNOWFLAKE_SAMPLE_DATA",
        "schema_name": "TPCH_SF1",
        "table_name": "CUSTOMER"
    }
)
print(response.json())
```

## Frontend Integration

This app can be integrated with the frontend to provide a UI for managing tags and applying them to database objects. The frontend can use the provided API endpoints to:

1. Display a list of available tags
2. Allow users to create, update, and delete tags
3. Show existing tags on database objects
4. Provide an interface for applying tags to database objects
5. Filter database objects by tags

## Installation

1. Add 'data_tagging' to INSTALLED_APPS in settings.py
2. Run migrations: `python manage.py makemigrations data_tagging`
3. Apply migrations: `python manage.py migrate`
4. Include URLs in the main URLconf:
   ```python
   path('api/tagging/', include('data_tagging.urls')),
   ``` 