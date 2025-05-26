# Generated manually

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Tag',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
                ('color', models.CharField(default='#3498db', max_length=20)),
                ('description', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name='ColumnTag',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('database', models.CharField(max_length=200)),
                ('schema', models.CharField(max_length=200)),
                ('table', models.CharField(max_length=200)),
                ('column_name', models.CharField(max_length=200)),
                ('tag', models.CharField(max_length=100)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.CreateModel(
            name='TaggedItem',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('object_type', models.CharField(choices=[('database', 'Database'), ('schema', 'Schema'), ('table', 'Table'), ('column', 'Column')], max_length=20)),
                ('database_name', models.CharField(max_length=255)),
                ('schema_name', models.CharField(blank=True, max_length=255, null=True)),
                ('table_name', models.CharField(blank=True, max_length=255, null=True)),
                ('column_name', models.CharField(blank=True, max_length=255, null=True)),
                ('tagged_by', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('tag', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tagged_items', to='data_tagging.tag')),
            ],
            options={
                'unique_together': {('tag', 'object_type', 'database_name', 'schema_name', 'table_name', 'column_name')},
            },
        ),
        migrations.AddIndex(
            model_name='taggeditem',
            index=models.Index(fields=['object_type'], name='data_taggin_object__e765ab_idx'),
        ),
        migrations.AddIndex(
            model_name='taggeditem',
            index=models.Index(fields=['database_name'], name='data_taggin_databas_eb0de3_idx'),
        ),
        migrations.AddIndex(
            model_name='taggeditem',
            index=models.Index(fields=['schema_name'], name='data_taggin_schema__3eeaff_idx'),
        ),
        migrations.AddIndex(
            model_name='taggeditem',
            index=models.Index(fields=['table_name'], name='data_taggin_table_n_f85ca7_idx'),
        ),
    ]
