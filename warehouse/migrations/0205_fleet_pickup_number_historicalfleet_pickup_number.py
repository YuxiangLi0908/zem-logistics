# Generated by Django 4.2.7 on 2025-07-11 06:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0204_merge_20250625_1707'),
    ]

    operations = [
        migrations.AddField(
            model_name='fleet',
            name='pickup_number',
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
        migrations.AddField(
            model_name='historicalfleet',
            name='pickup_number',
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
    ]
