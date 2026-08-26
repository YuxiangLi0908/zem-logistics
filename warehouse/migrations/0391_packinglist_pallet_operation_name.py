from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0390_packinglist_pallet_operation_log"),
    ]

    operations = [
        migrations.AddField(
            model_name="packinglistpalletoperationlog",
            name="operation_name",
            field=models.CharField(
                blank=True,
                db_index=True,
                max_length=255,
                null=True,
                verbose_name="操作名称",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["operation_name"],
                name="warehouse_p_operati_d7a2c2_idx",
            ),
        ),
    ]
