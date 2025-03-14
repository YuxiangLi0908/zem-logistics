# Generated by Django 4.2.7 on 2024-03-09 02:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0017_alter_retrieval_retrieval_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="shipment",
            name="appointment_id",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name="container",
            name="weight_lbs",
            field=models.FloatField(blank=True, null=True),
        ),
    ]
