# Generated by Django 4.2.7 on 2024-08-10 01:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0079_retrieval_arrive_at_retrieval_arrive_at_destination"),
    ]

    operations = [
        migrations.AddField(
            model_name="retrieval",
            name="empty_returned",
            field=models.BooleanField(blank=True, default=False),
        ),
        migrations.AddField(
            model_name="retrieval",
            name="empty_returned_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
