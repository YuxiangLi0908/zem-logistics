# Generated by Django 4.2.7 on 2025-01-08 06:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0138_alter_invoicepreport_chassis_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='invoicedelivery',
            name='total_pallet',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
