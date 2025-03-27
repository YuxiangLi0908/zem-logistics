# Generated by Django 4.2.7 on 2025-03-25 09:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0167_historicalinvoicedelivery_surcharge_notes_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='historicalinvoicepreport',
            name='other_fees',
            field=models.JSONField(default=dict),
        ),
        migrations.AddField(
            model_name='invoicepreport',
            name='other_fees',
            field=models.JSONField(default=dict),
        ),
    ]
