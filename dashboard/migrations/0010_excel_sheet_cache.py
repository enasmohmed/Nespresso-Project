# Generated manually for ExcelSheetCache

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0009_alter_meetingpoint_assigned_to'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExcelSheetCache',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sheet_name', models.CharField(db_index=True, max_length=255, unique=True)),
                ('data', models.JSONField(default=list)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'Excel Sheet Cache',
                'verbose_name_plural': 'Excel Sheet Caches',
            },
        ),
    ]
