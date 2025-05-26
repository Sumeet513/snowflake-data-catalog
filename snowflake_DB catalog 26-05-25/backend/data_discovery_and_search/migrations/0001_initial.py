from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='SearchHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('query', models.TextField(help_text='Natural language query')),
                ('generated_sql', models.TextField(blank=True, help_text='Generated SQL query', null=True)),
                ('user_identifier', models.CharField(blank=True, help_text='User identifier', max_length=64, null=True)),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('successful', models.BooleanField(default=True)),
            ],
            options={
                'verbose_name_plural': 'Search histories',
                'ordering': ['-timestamp'],
            },
        ),
    ] 