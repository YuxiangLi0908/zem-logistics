# Generated by Django 4.2.7 on 2024-04-29 16:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0043_shipment_third_party_address'),
    ]

    operations = [
        migrations.AlterField(
            model_name='shipment',
            name='address',
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
    ]