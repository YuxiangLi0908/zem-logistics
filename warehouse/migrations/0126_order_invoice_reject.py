# Generated by Django 5.1.1 on 2024-12-02 01:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0125_order_invoice_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='invoice_reject',
            field=models.BooleanField(default=False),
        ),
    ]
