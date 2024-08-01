# Generated by Django 4.2.7 on 2024-07-31 04:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0071_remove_retrieval_retrieval_location_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="container",
            name="is_special_container",
            field=models.BooleanField(blank=True, default=False, null=True),
        ),
        migrations.AddField(
            model_name="container",
            name="note",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
