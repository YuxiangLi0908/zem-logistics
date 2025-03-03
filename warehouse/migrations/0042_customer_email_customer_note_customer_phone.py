# Generated by Django 4.2.7 on 2024-04-24 04:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0041_rename_destination_retrieval_destination_port_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="customer",
            name="email",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="customer",
            name="note",
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
        migrations.AddField(
            model_name="customer",
            name="phone",
            field=models.CharField(blank=True, max_length=30, null=True),
        ),
    ]
