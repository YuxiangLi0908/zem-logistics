# Generated by Django 5.1.1 on 2025-02-28 09:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0161_historicalshipment_express_number_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='invoicepreport',
            name='surcharge_notes',
            field=models.JSONField(default=dict),
        ),
        migrations.AddField(
            model_name='invoicepreport',
            name='surcharges',
            field=models.JSONField(default=dict),
        ),
    ]
