# Generated by Django 5.1.1 on 2024-12-10 06:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0131_alter_order_invoice_reject_reason_invoicewarehouse'),
    ]

    operations = [
        migrations.AddField(
            model_name='pallet',
            name='delivery_type',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
