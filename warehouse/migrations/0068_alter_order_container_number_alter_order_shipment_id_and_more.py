# Generated by Django 4.2.7 on 2024-07-22 21:46

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0067_alter_packinglist_shipment_batch_number"),
    ]

    operations = [
        migrations.AlterField(
            model_name="order",
            name="container_number",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="order",
                to="warehouse.container",
            ),
        ),
        migrations.AlterField(
            model_name="order",
            name="shipment_id",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="order",
                to="warehouse.shipment",
            ),
        ),
        migrations.AlterField(
            model_name="order",
            name="warehouse",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="order",
                to="warehouse.zemwarehouse",
            ),
        ),
    ]