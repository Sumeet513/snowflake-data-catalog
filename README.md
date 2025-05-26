# Snowflake Data Catalog

A comprehensive data cataloging solution for Snowflake databases with metadata management, data profiling, and search capabilities.

## Features

- **Data Discovery and Search**: Easily find and explore data assets across your Snowflake environment
- **Metadata Management**: Capture and manage metadata for databases, schemas, tables, and columns
- **Data Tagging**: Apply business context and categorization to your data assets
- **Data Profiling**: Automatically analyze and profile your data to understand its structure and quality
- **Natural Language Search**: Query your data using natural language
- **Semantic Search**: Find related data assets based on semantic similarity

## Architecture

The application consists of:

- **Backend**: Django REST API for data management and integration with Snowflake
- **Frontend**: React TypeScript application for user interface
- **Metadata Manager**: Python modules for metadata extraction and profiling

## Getting Started

### Prerequisites

- Python 3.8+
- Node.js 14+
- Snowflake account with appropriate permissions

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/Sumeet513/snowflake-data-catalog.git
   cd snowflake-data-catalog
   ```

2. Install backend dependencies:
   ```
   cd backend
   pip install -r requirements.txt
   ```

3. Install frontend dependencies:
   ```
   cd ../frontend
   npm install
   ```

4. Configure your Snowflake connection in `backend/.env`

5. Run the application:
   ```
   # In one terminal
   cd backend
   python manage.py runserver
   
   # In another terminal
   cd frontend
   npm start
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
