# Generated by Django 5.1.1 on 2024-12-11 07:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0131_pallet_delivery_type'),
    ]

    operations = [
        migrations.AddField(
            model_name='pallet',
            name='price_quote',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]

