# Generated by Django 4.2.7 on 2024-04-22 19:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0039_customer_zem_code"),
    ]

    operations = [
        migrations.AddField(
            model_name="shipment",
            name="pod_link",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
    ]
