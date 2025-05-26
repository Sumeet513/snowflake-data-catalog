# Data Discovery and Search

This Django app provides natural language search capabilities for Snowflake databases. It allows users to query Snowflake data using plain English questions, which are then translated into SQL queries using AI.

## Features

- Natural language to SQL translation using OpenAI
- Optimized Snowflake connection management
- Query history tracking
- Detailed schema information extraction for accurate query generation

## Usage

### API Endpoints

- `/api/natural-language-query/` - POST endpoint for executing natural language queries

### Example Request

```json
{
  "credentials": {
    "account": "your_snowflake_account",
    "username": "your_username",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
  },
  "query": "Show me the top 10 customers by order amount"
}
```

### Example Response

```json
{
  "columns": ["CUSTOMER_ID", "CUSTOMER_NAME", "TOTAL_ORDER_AMOUNT"],
  "rows": [
    {
      "CUSTOMER_ID": "C001",
      "CUSTOMER_NAME": "Acme Corp",
      "TOTAL_ORDER_AMOUNT": 50000
    },
    ...
  ],
  "sql": "SELECT CUSTOMER_ID, CUSTOMER_NAME, SUM(ORDER_AMOUNT) AS TOTAL_ORDER_AMOUNT FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID GROUP BY CUSTOMER_ID, CUSTOMER_NAME ORDER BY TOTAL_ORDER_AMOUNT DESC LIMIT 10",
  "execution_time": 0.521,
  "natural_language_query": "Show me the top 10 customers by order amount"
}
```

## Architecture

- `ai_utils.py` - OpenAI integration for natural language processing
- `connection_manager.py` - Snowflake connection management
- `natural_language_query.py` - Core query processing logic
- `models.py` - Django models for storing query history
- `urls.py` - URL routing

## Dependencies

- Django
- Snowflake Connector for Python
- OpenAI API 