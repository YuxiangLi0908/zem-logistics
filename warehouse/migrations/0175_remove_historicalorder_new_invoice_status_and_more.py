# Generated by Django 4.2.7 on 2025-04-09 03:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0174_historicalorder_new_invoice_status_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="historicalorder",
            name="new_invoice_status",
        ),
        migrations.RemoveField(
            model_name="order",
            name="new_invoice_status",
        ),
        migrations.AddField(
            model_name="historicalorder",
            name="sub_status",
            field=models.CharField(max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="order",
            name="sub_status",
            field=models.CharField(max_length=255, null=True),
        ),
    ]
