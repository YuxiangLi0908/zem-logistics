# Generated by Django 4.2.7 on 2025-07-15 06:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0208_historicalpallet_expense_pallet_expense_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='historicalpallet',
            name='expense',
        ),
        migrations.RemoveField(
            model_name='pallet',
            name='expense',
        ),
        migrations.AddField(
            model_name='fleetshipmentpallet',
            name='expense',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='historicalfleetshipmentpallet',
            name='expense',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
