# Code Structure Overview

## Project Organization

The backend codebase has been reorganized to separate concerns and improve maintainability:

### Main Components

1. **db_connection**
   - Core database connection functionality
   - Snowflake metadata management
   - General database utilities

2. **data_discovery_and_search**
   - Natural language query processing
   - AI-powered search capabilities
   - Search history tracking
   - Optimized connection management for search operations

3. **snowflake_catalog**
   - Main Django project configuration
   - URL routing
   - Settings management

## File Structure

```
backend/
├── db_connection/
│   ├── migrations/
│   ├── utils/
│   ├── services/
│   ├── viewsets.py            - REST API viewsets
│   ├── views.py               - Function-based views
│   ├── models.py              - Database models
│   ├── snowflake_connection.py - Core connection management
│   ├── snowflake_manager.py   - Snowflake operations
│   ├── snowflake_metadata.py  - Metadata handling
│   ├── snowflake_ai.py        - AI integration for Snowflake
│   └── urls.py                - URL routing
│
├── data_discovery_and_search/
│   ├── migrations/
│   ├── ai_utils.py            - OpenAI integration
│   ├── connection_manager.py  - Connection management for search
│   ├── models.py              - Search history model
│   ├── natural_language_query.py - Query processing
│   ├── urls.py                - URL routing
│   ├── apps.py                - Django app configuration
│   └── admin.py               - Admin interface
│
└── snowflake_catalog/         - Main project settings
    ├── settings.py
    ├── urls.py
    └── wsgi.py
```

## Key API Endpoints

- `/api/natural-language-query/` - Natural language to SQL query endpoint
- `/api/databases/` - List available databases
- `/api/schemas/` - List schemas in a database
- `/api/tables/` - List tables in a schema
- `/api/columns/` - List columns in a table

## Recent Changes

1. **Separation of Concerns**
   - Moved natural language query functionality to a dedicated app
   - Removed duplicated OpenAI utility code
   - Created specialized connection management for search operations

2. **Improved Code Organization**
   - Created proper Django app structure for data discovery
   - Added models for search history tracking
   - Organized URL routing

3. **Enhanced Maintainability**
   - Clear separation between core DB functionality and search/discovery
   - Dedicated models and endpoints for each concern
   - Better organized configuration 