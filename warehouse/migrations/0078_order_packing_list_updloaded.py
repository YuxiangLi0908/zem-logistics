# Generated by Django 4.2.7 on 2024-08-09 03:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0077_retrieval_temp_t49_available_for_pickup_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="order",
            name="packing_list_updloaded",
            field=models.BooleanField(default=False),
        ),
    ]