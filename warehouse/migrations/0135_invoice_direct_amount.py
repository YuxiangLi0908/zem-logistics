# Generated by Django 5.1.1 on 2024-12-13 08:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0134_rename_amazon_amount_invoice_delivery_amount_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='invoice',
            name='direct_amount',
            field=models.FloatField(blank=True, null=True),
        ),
    ]

