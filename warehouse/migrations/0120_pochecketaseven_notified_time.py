# Generated by Django 5.1.1 on 2024-11-02 02:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0119_shipment_priority'),
    ]

    operations = [
        migrations.AddField(
            model_name='pochecketaseven',
            name='notified_time',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]