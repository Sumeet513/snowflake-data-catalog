
from django.conf import settings

def set_snowflake_connection(account, username, password, warehouse, database, schema):
    settings.DATABASES['snowflake'] = {
        'ENGINE': 'django_snowflake',
        'NAME': database,
        'SCHEMA': schema,
        'USER': username,
        'PASSWORD': password,
        'ACCOUNT': account,
        'WAREHOUSE': warehouse,
    }