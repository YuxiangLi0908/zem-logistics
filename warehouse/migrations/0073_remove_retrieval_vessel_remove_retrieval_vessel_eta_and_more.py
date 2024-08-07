# Generated by Django 4.2.7 on 2024-07-31 04:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0072_container_is_special_container_container_note"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="retrieval",
            name="vessel",
        ),
        migrations.RemoveField(
            model_name="retrieval",
            name="vessel_eta",
        ),
        migrations.RemoveField(
            model_name="retrieval",
            name="vessel_etd",
        ),
        migrations.CreateModel(
            name="Vessel",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("vessel_id", models.CharField(max_length=255, null=True)),
                (
                    "master_bill_of_lading",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "origin_port",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "destination_port",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "shipping_line",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                ("vessel", models.CharField(blank=True, max_length=100, null=True)),
                ("voyage", models.CharField(blank=True, max_length=100, null=True)),
                ("vessel_etd", models.DateField(blank=True, null=True)),
                ("vessel_eta", models.DateField(blank=True, null=True)),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["vessel_id"], name="warehouse_v_vessel__7a4ca8_idx"
                    ),
                    models.Index(
                        fields=["master_bill_of_lading"],
                        name="warehouse_v_master__fd3aab_idx",
                    ),
                    models.Index(
                        fields=["vessel"], name="warehouse_v_vessel_5b5591_idx"
                    ),
                    models.Index(
                        fields=["voyage"], name="warehouse_v_voyage_91b60a_idx"
                    ),
                ],
            },
        ),
    ]
