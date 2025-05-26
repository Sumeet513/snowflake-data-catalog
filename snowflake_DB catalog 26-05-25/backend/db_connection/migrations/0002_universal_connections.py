# Generated manually for universal connections

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('db_connection', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='AWSGlueConnection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(default='Default AWS Glue Connection', max_length=255)),
                ('aws_region', models.CharField(max_length=50)),
                ('access_key', models.CharField(max_length=255)),
                ('secret_key', models.CharField(max_length=255)),
                ('session_token', models.TextField(blank=True, null=True)),
                ('role_arn', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('last_used', models.DateTimeField(auto_now=True)),
                ('is_active', models.BooleanField(default=True)),
            ],
            options={
                'db_table': 'aws_glue_connections',
                'unique_together': {('aws_region', 'access_key')},
            },
        ),
        migrations.CreateModel(
            name='Connection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
                ('connection_type', models.CharField(choices=[('snowflake', 'Snowflake'), ('aws_glue', 'AWS Glue')], max_length=50)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('last_used', models.DateTimeField(auto_now=True)),
                ('is_active', models.BooleanField(default=True)),
                ('aws_glue_connection', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='db_connection.awsglueconnection')),
                ('snowflake_connection', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='db_connection.snowflakeconnection')),
            ],
            options={
                'db_table': 'connections',
            },
        ),
        migrations.CreateModel(
            name='AWSGlueCatalog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('catalog_id', models.CharField(max_length=255, unique=True)),
                ('catalog_name', models.CharField(max_length=255)),
                ('catalog_description', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('collected_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'aws_glue_catalogs',
            },
        ),
        migrations.CreateModel(
            name='AWSGlueDatabase',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('database_id', models.CharField(max_length=255, unique=True)),
                ('database_name', models.CharField(max_length=255)),
                ('database_description', models.TextField(blank=True, null=True)),
                ('location_uri', models.CharField(blank=True, max_length=255, null=True)),
                ('parameters', models.JSONField(blank=True, default=dict)),
                ('create_date', models.DateTimeField(blank=True, null=True)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('catalog', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='databases', to='db_connection.awsgluecatalog')),
            ],
            options={
                'db_table': 'aws_glue_databases',
                'unique_together': {('catalog', 'database_name')},
            },
        ),
        migrations.CreateModel(
            name='AWSGlueTable',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('table_id', models.CharField(max_length=255, unique=True)),
                ('table_name', models.CharField(max_length=255)),
                ('table_type', models.CharField(blank=True, max_length=50, null=True)),
                ('table_description', models.TextField(blank=True, null=True)),
                ('owner', models.CharField(blank=True, max_length=255, null=True)),
                ('create_date', models.DateTimeField(blank=True, null=True)),
                ('last_access_date', models.DateTimeField(blank=True, null=True)),
                ('last_altered_date', models.DateTimeField(blank=True, null=True)),
                ('storage_location', models.CharField(blank=True, max_length=255, null=True)),
                ('storage_format', models.CharField(blank=True, max_length=50, null=True)),
                ('parameters', models.JSONField(blank=True, default=dict)),
                ('partition_keys', models.JSONField(blank=True, default=list)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('database', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tables', to='db_connection.awsgluedatabase')),
            ],
            options={
                'db_table': 'aws_glue_tables',
                'unique_together': {('database', 'table_name')},
            },
        ),
        migrations.CreateModel(
            name='AWSGlueColumn',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('column_id', models.CharField(max_length=255, unique=True)),
                ('column_name', models.CharField(max_length=255)),
                ('ordinal_position', models.IntegerField(blank=True, null=True)),
                ('data_type', models.CharField(blank=True, max_length=100, null=True)),
                ('column_description', models.TextField(blank=True, null=True)),
                ('is_nullable', models.BooleanField(default=True)),
                ('is_partition_key', models.BooleanField(default=False)),
                ('parameters', models.JSONField(blank=True, default=dict)),
                ('collected_at', models.DateTimeField(auto_now=True)),
                ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='columns', to='db_connection.awsgluetable')),
            ],
            options={
                'db_table': 'aws_glue_columns',
                'unique_together': {('table', 'column_name')},
            },
        ),
    ]