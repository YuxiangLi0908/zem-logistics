# Generated by Django 4.2.7 on 2023-12-29 02:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0003_packinglist_is_shipment_schduled'),
    ]

    operations = [
        migrations.AddField(
            model_name='packinglist',
            name='shipment_appointmend',
            field=models.DateField(null=True),
        ),
    ]