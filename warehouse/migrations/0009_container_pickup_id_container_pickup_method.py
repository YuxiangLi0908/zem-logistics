# Generated by Django 4.2.7 on 2023-12-13 18:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0008_alter_packinglist_destination'),
    ]

    operations = [
        migrations.AddField(
            model_name='container',
            name='pickup_id',
            field=models.CharField(max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='container',
            name='pickup_method',
            field=models.CharField(max_length=255, null=True),
        ),
    ]
