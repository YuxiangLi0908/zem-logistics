# Generated by Django 4.2.7 on 2024-11-04 19:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0119_shipment_priority"),
    ]

    operations = [
        migrations.AddField(
            model_name="fleet",
            name="pod_uploaded_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="pod_uploaded_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
