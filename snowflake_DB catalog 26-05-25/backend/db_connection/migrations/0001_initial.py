# Generated manually

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='SnowflakeConnection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(default='Default Connection', max_length=255)),
                ('account', models.CharField(max_length=255)),
                ('username', models.CharField(max_length=255)),
                ('password', models.CharField(max_length=255)),
                ('warehouse', models.CharField(max_length=255)),
                ('database_name', models.CharField(blank=True, max_length=255, null=True)),
                ('schema_name', models.CharField(blank=True, max_length=255, null=True)),
                ('role', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('last_used', models.DateTimeField(auto_now=True)),
                ('is_active', models.BooleanField(default=True)),
            ],
            options={
                'db_table': 'snowflake_connections',
                'unique_together': {('account', 'username')},
            },
        ),
        migrations.CreateModel(
            name='SnowflakeDatabase',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('database_id', models.CharField(max_length=255, unique=True)),
                ('database_name', models.CharField(max_length=255)),
                ('database_owner', models.CharField(blank=True, max_length=255, null=True)),
                ('database_description', models.TextField(blank=True, null=True)),
                ('create_date', models.DateTimeField(blank=True, null=True)),
                ('last_altered_date', models.DateTimeField(blank=True, null=True)),
                ('comment', models.TextField(blank=True, null=True)),
                ('tags', models.JSONField(blank=True, default=dict)),
                ('collected_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'snowflake_databases',
            },
        ),
        migrations.CreateModel(
            name='SnowflakeSchema',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('schema_id', models.CharField(max_length=255, unique=True)),
                ('schema_name', models.CharField(max_length=255)),
                ('schema_owner', models.CharField(blank=True, max_length=255, null=True)),
                ('schema_description', models.TextField(blank=True, null=True)),
                ('create_date', models.DateTimeField(blank=True, null=True)),
                ('last_altered_date', models.DateTimeField(blank=True, null=True)),
                ('comment', models.TextField(blank=True, null=True)),
                ('tags', models.JSONField(blank=True, default=dict)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('database', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='schemas', to='db_connection.snowflakedatabase')),
            ],
            options={
                'db_table': 'snowflake_schemas',
                'unique_together': {('database', 'schema_name')},
            },
        ),
        migrations.CreateModel(
            name='SnowflakeTable',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('table_id', models.CharField(max_length=255, unique=True)),
                ('table_name', models.CharField(max_length=255)),
                ('table_type', models.CharField(blank=True, max_length=50, null=True)),
                ('table_owner', models.CharField(blank=True, max_length=255, null=True)),
                ('table_description', models.TextField(blank=True, null=True)),
                ('row_count', models.IntegerField(blank=True, null=True)),
                ('byte_size', models.BigIntegerField(blank=True, null=True)),
                ('create_date', models.DateTimeField(blank=True, null=True)),
                ('last_altered_date', models.DateTimeField(blank=True, null=True)),
                ('comment', models.TextField(blank=True, null=True)),
                ('tags', models.JSONField(blank=True, default=dict)),
                ('sensitivity_level', models.CharField(blank=True, max_length=50, null=True)),
                ('data_domain', models.CharField(blank=True, max_length=100, null=True)),
                ('keywords', models.JSONField(blank=True, default=list)),
                ('business_glossary_terms', models.JSONField(blank=True, default=list)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('schema', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tables', to='db_connection.snowflakeschema')),
            ],
            options={
                'db_table': 'snowflake_tables',
                'unique_together': {('schema', 'table_name')},
            },
        ),
        migrations.CreateModel(
            name='SnowflakeColumn',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('column_id', models.CharField(max_length=255, unique=True)),
                ('column_name', models.CharField(max_length=255)),
                ('ordinal_position', models.IntegerField(blank=True, null=True)),
                ('data_type', models.CharField(blank=True, max_length=100, null=True)),
                ('character_maximum_length', models.IntegerField(blank=True, null=True)),
                ('numeric_precision', models.IntegerField(blank=True, null=True)),
                ('numeric_scale', models.IntegerField(blank=True, null=True)),
                ('is_nullable', models.BooleanField(default=True)),
                ('column_default', models.TextField(blank=True, null=True)),
                ('column_description', models.TextField(blank=True, null=True)),
                ('comment', models.TextField(blank=True, null=True)),
                ('tags', models.JSONField(blank=True, default=dict)),
                ('sensitivity_level', models.CharField(blank=True, max_length=50, null=True)),
                ('is_pii', models.BooleanField(default=False)),
                ('is_primary_key', models.BooleanField(default=False)),
                ('is_foreign_key', models.BooleanField(default=False)),
                ('referenced_table_id', models.CharField(blank=True, max_length=255, null=True)),
                ('referenced_column_id', models.CharField(blank=True, max_length=255, null=True)),
                ('min_value', models.CharField(blank=True, max_length=255, null=True)),
                ('max_value', models.CharField(blank=True, max_length=255, null=True)),
                ('distinct_values', models.IntegerField(blank=True, null=True)),
                ('null_count', models.IntegerField(blank=True, null=True)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='columns', to='db_connection.snowflaketable')),
            ],
            options={
                'db_table': 'snowflake_columns',
                'unique_together': {('table', 'column_name')},
            },
        ),
    ] 