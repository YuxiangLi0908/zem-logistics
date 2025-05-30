# Generated by Django 4.2.7 on 2024-01-29 03:17

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0009_clearance_offload_retrieval_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="clearance",
            name="is_clearance_required",
            field=models.BooleanField(default=True),
        ),
        migrations.AlterField(
            model_name="retrieval",
            name="shipping_line",
            field=models.CharField(max_length=255, null=True),
        ),
    ]
