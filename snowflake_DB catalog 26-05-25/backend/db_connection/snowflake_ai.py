import requests
import json
from django.core.cache import cache
from datetime import datetime
import time
from .snowflake_connection import SnowflakeConnection

class SnowflakeAI:
    """
    Handles AI-powered enhancement of Snowflake metadata, including:
    - Generating table descriptions
    - Detecting sensitive data
    - Enhancing search capabilities
    """
    def __init__(self, ai_api_key=None, ai_provider="openai"):
        self.connection = SnowflakeConnection()
        self.ai_api_key = ai_api_key
        self.ai_provider = ai_provider

    def set_ai_api_key(self, api_key, provider="openai"):
        """Set the API key for AI services"""
        self.ai_api_key = api_key
        self.ai_provider = provider

    def _generate_ai_description(self, table_name, columns_info):
        """Generate AI-powered descriptions for tables"""
        if not self.ai_api_key:
            return {
                'description': 'AI description not available (no API key provided)',
                'keywords': []
            }
            
        # Format column info as text for the AI
        column_text = "\n".join([
            f"- {col['name']} ({col['type']}): {col.get('comment', 'No description')}"
            for col in columns_info
        ])
        
        # Generate description based on provider
        if self.ai_provider.lower() == "openai":
            return self._generate_openai_description(table_name, column_text)
        elif self.ai_provider.lower() == "anthropic":
            return self._generate_anthropic_description(table_name, column_text)
        else:
            return {
                'description': f'AI description not available (unsupported provider: {self.ai_provider})',
                'keywords': []
            }
    
    def _generate_openai_description(self, table_name, column_text):
        """Generate table description using OpenAI API"""
        try:
            prompt = f"""
            I have a database table named '{table_name}' with the following columns:
            
            {column_text}
            
            Based on this information, please:
            1. Write a concise description of what this table likely contains and its purpose (2-3 sentences).
            2. Provide 5-10 relevant keywords that would help categorize this table.
            
            Format your response as valid JSON with two fields:
            - 'description': the table description
            - 'keywords': array of keywords
            """
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.ai_api_key}"
            }
            
            data = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data['choices'][0]['message']['content']
                try:
                    result = json.loads(content)
                    return {
                        'description': result.get('description', 'No description generated'),
                        'keywords': result.get('keywords', [])
                    }
                except json.JSONDecodeError:
                    return {
                        'description': 'Error parsing AI response as JSON',
                        'keywords': []
                    }
            else:
                error_message = f"OpenAI API error: {response.status_code}, {response.text}"
                print(error_message)
                return {
                    'description': f'Error generating description: {response.status_code}',
                    'keywords': []
                }
                
        except Exception as e:
            print(f"Error generating OpenAI description: {str(e)}")
            return {
                'description': f'Error generating description: {str(e)}',
                'keywords': []
            }
    
    def _generate_anthropic_description(self, table_name, column_text):
        """Generate table description using Anthropic API"""
        try:
            prompt = f"""
            I have a database table named '{table_name}' with the following columns:
            
            {column_text}
            
            Based on this information, please:
            1. Write a concise description of what this table likely contains and its purpose (2-3 sentences).
            2. Provide 5-10 relevant keywords that would help categorize this table.
            
            Format your response as valid JSON with two fields:
            - 'description': the table description
            - 'keywords': array of keywords
            """
            
            headers = {
                "Content-Type": "application/json",
                "X-API-Key": self.ai_api_key,
                "anthropic-version": "2023-06-01"
            }
            
            data = {
                "model": "claude-2",
                "max_tokens_to_sample": 500,
                "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
                "temperature": 0.3
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/complete",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data.get('completion', '')
                
                # Extract JSON part from Claude's response
                try:
                    # Find JSON-like parts in the response
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_content = content[json_start:json_end]
                        result = json.loads(json_content)
                        return {
                            'description': result.get('description', 'No description generated'),
                            'keywords': result.get('keywords', [])
                        }
                    else:
                        return {
                            'description': content[:300] + "...",  # Use the text response as a fallback
                            'keywords': []
                        }
                except json.JSONDecodeError:
                    return {
                        'description': content[:300] + "...",  # Use the text response as a fallback
                        'keywords': []
                    }
            else:
                error_message = f"Anthropic API error: {response.status_code}, {response.text}"
                print(error_message)
                return {
                    'description': f'Error generating description: {response.status_code}',
                    'keywords': []
                }
                
        except Exception as e:
            print(f"Error generating Anthropic description: {str(e)}")
            return {
                'description': f'Error generating description: {str(e)}',
                'keywords': []
            }
    
    def generate_table_descriptions(self, connection_params, batch_size=5):
        """
        Generate AI descriptions for a batch of tables and store in standard description fields
        
        Args:
            connection_params: Dictionary with connection parameters
            batch_size: Number of tables to process in this batch
            
        Returns:
            Dictionary with batch processing results
        """
        results = {
            'status': 'processing',
            'processed_count': 0,
            'success_count': 0,
            'error_count': 0,
            'start_time': datetime.now().isoformat(),
            'errors': [],
        }
        
        try:
            with self.connection.get_connection(connection_params) as conn:
                cur = conn.cursor()
                
                # Get tables without descriptions
                cur.execute("""
                SELECT TABLE_ID, TABLE_NAME, SCHEMA_ID 
                FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_TABLES 
                WHERE TABLE_DESCRIPTION IS NULL OR TRIM(TABLE_DESCRIPTION) = ''
                LIMIT %s
                """, (batch_size,))
                
                tables = cur.fetchall()
                results['total_count'] = len(tables)
                
                for table_row in tables:
                    table_id = table_row[0]
                    table_name = table_row[1]
                    schema_id = table_row[2]
                    
                    try:
                        # Split table_id into components
                        db_name, schema_name, tbl_name = table_id.split('.')
                        
                        # Get columns for this table
                        columns = []
                        try:
                            # Get column info from our catalog
                            cur.execute("""
                            SELECT COLUMN_NAME, DATA_TYPE, COMMENT 
                            FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_COLUMNS 
                            WHERE TABLE_ID = %s
                            ORDER BY ORDINAL_POSITION
                            """, (table_id,))
                            
                            for col in cur.fetchall():
                                columns.append({
                                    'name': col[0],
                                    'type': col[1],
                                    'comment': col[2]
                                })
                        except Exception:
                            # Fallback: get column info directly from Snowflake
                            cur.execute(f"DESCRIBE TABLE {table_id}")
                            for col in cur.fetchall():
                                columns.append({
                                    'name': col[0],
                                    'type': col[1],
                                    'comment': col[8] if len(col) > 8 else None
                                })
                        
                        # Generate AI description
                        if columns:
                            ai_result = self._generate_ai_description(table_name, columns)
                            description = ai_result.get('description', '')
                            keywords = ai_result.get('keywords', [])
                            
                            # Update the table with AI-generated content in standard columns
                            cur.execute("""
                            UPDATE SNOWFLAKE_CATALOG.METADATA.CATALOG_TABLES 
                            SET 
                                TABLE_DESCRIPTION = %s,
                                KEYWORDS = %s
                            WHERE TABLE_ID = %s
                            """, (
                                description,
                                json.dumps(keywords) if keywords else None,
                                table_id
                            ))
                            
                            conn.commit()
                            results['success_count'] += 1
                        else:
                            results['errors'].append(f"No columns found for table {table_id}")
                            results['error_count'] += 1
                            
                    except Exception as e:
                        results['errors'].append(f"Error processing table {table_id}: {str(e)}")
                        results['error_count'] += 1
                    
                    results['processed_count'] += 1
                
                results['status'] = 'success'
                results['end_time'] = datetime.now().isoformat()
                results['duration_seconds'] = (datetime.now() - datetime.fromisoformat(results['start_time'])).total_seconds()
                
        except Exception as e:
            results['status'] = 'error'
            results['message'] = str(e)
        
        return results 

    def generate_tags_and_glossary(self, connection_params, batch_size=5):
        """
        Generate AI-powered tags and business glossary terms for tables and databases
        
        Args:
            connection_params: Dictionary with connection parameters
            batch_size: Number of tables to process in this batch
            
        Returns:
            Dictionary with batch processing results
        """
        results = {
            'status': 'processing',
            'processed_count': 0,
            'success_count': 0,
            'error_count': 0,
            'start_time': datetime.now().isoformat(),
            'errors': [],
        }
        
        try:
            with self.connection.get_connection(connection_params) as conn:
                cur = conn.cursor()
                
                # Get tables without tags or business glossary terms
                cur.execute("""
                SELECT TABLE_ID, TABLE_NAME, SCHEMA_ID, TABLE_DESCRIPTION
                FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_TABLES 
                WHERE (TAGS IS NULL OR TAGS = '{}' OR 
                      BUSINESS_GLOSSARY_TERMS IS NULL OR BUSINESS_GLOSSARY_TERMS = '[]')
                LIMIT %s
                """, (batch_size,))
                
                tables = cur.fetchall()
                results['total_count'] = len(tables)
                
                for table_row in tables:
                    table_id = table_row[0]
                    table_name = table_row[1]
                    schema_id = table_row[2]
                    table_description = table_row[3] or ""
                    
                    try:
                        # Get columns for this table
                        columns = []
                        cur.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DESCRIPTION, COMMENT 
                        FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_COLUMNS 
                        WHERE TABLE_ID = %s
                        ORDER BY ORDINAL_POSITION
                        """, (table_id,))
                        
                        for col in cur.fetchall():
                            columns.append({
                                'name': col[0],
                                'type': col[1],
                                'description': col[2] or "",
                                'comment': col[3] or ""
                            })
                        
                        # Generate tags and glossary terms with AI
                        ai_result = self._generate_tags_and_glossary(table_name, table_description, columns)
                        
                        # Update the table with generated tags and terms
                        cur.execute("""
                        UPDATE SNOWFLAKE_CATALOG.METADATA.CATALOG_TABLES
                        SET TAGS = %s, BUSINESS_GLOSSARY_TERMS = %s
                        WHERE TABLE_ID = %s
                        """, (
                            json.dumps(ai_result.get('tags', {})),
                            json.dumps(ai_result.get('business_glossary_terms', [])),
                            table_id
                        ))
                        
                        results['success_count'] += 1
                        
                    except Exception as e:
                        error_message = f"Error processing table {table_id}: {str(e)}"
                        print(error_message)
                        results['errors'].append(error_message)
                        results['error_count'] += 1
                    
                    results['processed_count'] += 1
                    
                # Also process databases that need tags or descriptions
                cur.execute("""
                SELECT DATABASE_ID, DATABASE_NAME, DATABASE_DESCRIPTION
                FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_DATABASES
                WHERE (TAGS IS NULL OR TAGS = '{}' OR DATABASE_DESCRIPTION IS NULL OR DATABASE_DESCRIPTION = '')
                LIMIT %s
                """, (batch_size,))
                
                databases = cur.fetchall()
                
                for db_row in databases:
                    database_id = db_row[0]
                    database_name = db_row[1]
                    database_description = db_row[2] or ""
                    
                    try:
                        # Get schemas in this database for context
                        schemas = []
                        cur.execute("""
                        SELECT SCHEMA_NAME, SCHEMA_DESCRIPTION
                        FROM SNOWFLAKE_CATALOG.METADATA.CATALOG_SCHEMAS
                        WHERE DATABASE_ID = %s
                        """, (database_id,))
                        
                        for schema in cur.fetchall():
                            schemas.append({
                                'name': schema[0],
                                'description': schema[1] or ""
                            })
                        
                        # Generate tags and description for the database
                        ai_result = self._generate_database_metadata(database_name, schemas)
                        
                        # Update the database record
                        cur.execute("""
                        UPDATE SNOWFLAKE_CATALOG.METADATA.CATALOG_DATABASES
                        SET TAGS = %s, DATABASE_DESCRIPTION = COALESCE(NULLIF(DATABASE_DESCRIPTION, ''), %s)
                        WHERE DATABASE_ID = %s
                        """, (
                            json.dumps(ai_result.get('tags', {})),
                            ai_result.get('description', ''),
                            database_id
                        ))
                        
                        results['success_count'] += 1
                        
                    except Exception as e:
                        error_message = f"Error processing database {database_id}: {str(e)}"
                        print(error_message)
                        results['errors'].append(error_message)
                        results['error_count'] += 1
                    
                    results['processed_count'] += 1
                    
        except Exception as e:
            error_message = f"Error in generate_tags_and_glossary: {str(e)}"
            print(error_message)
            results['status'] = 'error'
            results['error_message'] = error_message
            return results
        
        results['status'] = 'completed'
        results['end_time'] = datetime.now().isoformat()
        
        return results

    def _generate_tags_and_glossary(self, table_name, table_description, columns_info):
        """Generate AI-powered tags and business glossary terms for a table"""
        if not self.ai_api_key:
            return {
                'tags': {},
                'business_glossary_terms': []
            }
        
        # Format column info as text for the AI
        column_text = "\n".join([
            f"- {col['name']} ({col['type']}): {col.get('description', '') or col.get('comment', 'No description')}"
            for col in columns_info
        ])
        
        # Generate based on provider
        if self.ai_provider.lower() == "openai":
            return self._generate_openai_tags_and_glossary(table_name, table_description, column_text)
        elif self.ai_provider.lower() == "anthropic":
            return self._generate_anthropic_tags_and_glossary(table_name, table_description, column_text)
        else:
            return {
                'tags': {},
                'business_glossary_terms': []
            }

    def _generate_openai_tags_and_glossary(self, table_name, table_description, column_text):
        """Generate tags and business glossary terms using OpenAI API"""
        try:
            prompt = f"""
            I have a database table named '{table_name}' with the following description:
            "{table_description}"
            
            And these columns:
            {column_text}
            
            Based on this information, please provide:
            
            1. A set of tags as key-value pairs to categorize this table (5-8 tags)
            2. A list of business glossary terms that would apply to this table (3-6 terms)
            
            Format your response as valid JSON with these fields:
            - 'tags': object with key-value pairs (e.g. {{"domain": "finance", "data_type": "transactional"}})
            - 'business_glossary_terms': array of terms (e.g. ["customer data", "sales information"])
            """
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.ai_api_key}"
            }
            
            data = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data['choices'][0]['message']['content']
                try:
                    result = json.loads(content)
                    return {
                        'tags': result.get('tags', {}),
                        'business_glossary_terms': result.get('business_glossary_terms', [])
                    }
                except json.JSONDecodeError:
                    return {
                        'tags': {},
                        'business_glossary_terms': []
                    }
            else:
                print(f"OpenAI API error: {response.status_code}, {response.text}")
                return {
                    'tags': {},
                    'business_glossary_terms': []
                }
                
        except Exception as e:
            print(f"Error generating OpenAI tags and glossary: {str(e)}")
            return {
                'tags': {},
                'business_glossary_terms': []
            }

    def _generate_anthropic_tags_and_glossary(self, table_name, table_description, column_text):
        """Generate tags and business glossary terms using Anthropic API"""
        try:
            prompt = f"""
            I have a database table named '{table_name}' with the following description:
            "{table_description}"
            
            And these columns:
            {column_text}
            
            Based on this information, please provide:
            
            1. A set of tags as key-value pairs to categorize this table (5-8 tags)
            2. A list of business glossary terms that would apply to this table (3-6 terms)
            
            Format your response as valid JSON with these fields:
            - 'tags': object with key-value pairs (e.g. {{"domain": "finance", "data_type": "transactional"}})
            - 'business_glossary_terms': array of terms (e.g. ["customer data", "sales information"])
            """
            
            headers = {
                "Content-Type": "application/json",
                "X-API-Key": self.ai_api_key,
                "anthropic-version": "2023-06-01"
            }
            
            data = {
                "model": "claude-2",
                "max_tokens_to_sample": 500,
                "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
                "temperature": 0.3
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/complete",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data.get('completion', '')
                
                try:
                    # Find JSON-like parts in the response
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_content = content[json_start:json_end]
                        result = json.loads(json_content)
                        return {
                            'tags': result.get('tags', {}),
                            'business_glossary_terms': result.get('business_glossary_terms', [])
                        }
                    else:
                        return {
                            'tags': {},
                            'business_glossary_terms': []
                        }
                except json.JSONDecodeError:
                    return {
                        'tags': {},
                        'business_glossary_terms': []
                    }
            else:
                print(f"Anthropic API error: {response.status_code}, {response.text}")
                return {
                    'tags': {},
                    'business_glossary_terms': []
                }
                
        except Exception as e:
            print(f"Error generating Anthropic tags and glossary: {str(e)}")
            return {
                'tags': {},
                'business_glossary_terms': []
            }

    def _generate_database_metadata(self, database_name, schemas_info):
        """Generate AI-powered tags and description for a database"""
        if not self.ai_api_key:
            return {
                'tags': {},
                'description': f"Database {database_name}"
            }
        
        # Format schema info as text for the AI
        schema_text = "\n".join([
            f"- Schema: {schema['name']} - {schema.get('description', 'No description')}"
            for schema in schemas_info
        ])
        
        # Generate based on provider
        if self.ai_provider.lower() == "openai":
            return self._generate_openai_database_metadata(database_name, schema_text)
        elif self.ai_provider.lower() == "anthropic":
            return self._generate_anthropic_database_metadata(database_name, schema_text)
        else:
            return {
                'tags': {},
                'description': f"Database {database_name}"
            }

    def _generate_openai_database_metadata(self, database_name, schema_text):
        """Generate database metadata using OpenAI API"""
        try:
            prompt = f"""
            I have a Snowflake database named '{database_name}' with the following schemas:
            
            {schema_text}
            
            Based on this information, please:
            1. Write a concise description of what this database likely contains and its purpose (2-3 sentences).
            2. Provide 3-5 relevant key-value pairs as tags that would help categorize this database.
            
            Format your response as valid JSON with these fields:
            - 'description': the database description
            - 'tags': object with key-value pairs (e.g. {{"purpose": "analytics", "environment": "production"}})
            """
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.ai_api_key}"
            }
            
            data = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data['choices'][0]['message']['content']
                try:
                    result = json.loads(content)
                    return {
                        'description': result.get('description', f"Database {database_name}"),
                        'tags': result.get('tags', {})
                    }
                except json.JSONDecodeError:
                    return {
                        'description': f"Database {database_name}",
                        'tags': {}
                    }
            else:
                print(f"OpenAI API error: {response.status_code}, {response.text}")
                return {
                    'description': f"Database {database_name}",
                    'tags': {}
                }
                
        except Exception as e:
            print(f"Error generating OpenAI database metadata: {str(e)}")
            return {
                'description': f"Database {database_name}",
                'tags': {}
            }

    def _generate_anthropic_database_metadata(self, database_name, schema_text):
        """Generate database metadata using Anthropic API"""
        try:
            prompt = f"""
            I have a Snowflake database named '{database_name}' with the following schemas:
            
            {schema_text}
            
            Based on this information, please:
            1. Write a concise description of what this database likely contains and its purpose (2-3 sentences).
            2. Provide 3-5 relevant key-value pairs as tags that would help categorize this database.
            
            Format your response as valid JSON with these fields:
            - 'description': the database description
            - 'tags': object with key-value pairs (e.g. {{"purpose": "analytics", "environment": "production"}})
            """
            
            headers = {
                "Content-Type": "application/json",
                "X-API-Key": self.ai_api_key,
                "anthropic-version": "2023-06-01"
            }
            
            data = {
                "model": "claude-2",
                "max_tokens_to_sample": 500,
                "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
                "temperature": 0.3
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/complete",
                headers=headers,
                data=json.dumps(data)
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data.get('completion', '')
                
                try:
                    # Find JSON-like parts in the response
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_content = content[json_start:json_end]
                        result = json.loads(json_content)
                        return {
                            'description': result.get('description', f"Database {database_name}"),
                            'tags': result.get('tags', {})
                        }
                    else:
                        return {
                            'description': f"Database {database_name}",
                            'tags': {}
                        }
                except json.JSONDecodeError:
                    return {
                        'description': f"Database {database_name}",
                        'tags': {}
                    }
            else:
                print(f"Anthropic API error: {response.status_code}, {response.text}")
                return {
                    'description': f"Database {database_name}",
                    'tags': {}
                }
                
        except Exception as e:
            print(f"Error generating Anthropic database metadata: {str(e)}")
            return {
                'description': f"Database {database_name}",
                'tags': {}
            } 