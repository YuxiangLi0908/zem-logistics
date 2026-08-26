from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("warehouse", "0389_remove_historicalpallet_pickup_images_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="PackingListPalletOperationLog",
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
                (
                    "target_type",
                    models.CharField(
                        choices=[
                            ("packing_list", "PackingList"),
                            ("pallet", "Pallet"),
                        ],
                        db_index=True,
                        max_length=20,
                        verbose_name="对象类型",
                    ),
                ),
                (
                    "target_id",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=100,
                        null=True,
                        verbose_name="对象ID",
                    ),
                ),
                (
                    "target_display",
                    models.CharField(
                        blank=True,
                        max_length=500,
                        null=True,
                        verbose_name="对象显示信息",
                    ),
                ),
                (
                    "container_number",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=255,
                        null=True,
                        verbose_name="柜号",
                    ),
                ),
                (
                    "po_id",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=500,
                        null=True,
                        verbose_name="PO_ID",
                    ),
                ),
                (
                    "fba_id",
                    models.CharField(
                        blank=True,
                        max_length=500,
                        null=True,
                        verbose_name="FBA",
                    ),
                ),
                (
                    "ref_id",
                    models.CharField(
                        blank=True,
                        max_length=500,
                        null=True,
                        verbose_name="REF",
                    ),
                ),
                (
                    "shipping_mark",
                    models.CharField(
                        blank=True,
                        max_length=1000,
                        null=True,
                        verbose_name="唛头",
                    ),
                ),
                (
                    "destination",
                    models.CharField(
                        blank=True,
                        max_length=1000,
                        null=True,
                        verbose_name="仓点",
                    ),
                ),
                (
                    "warehouse",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=100,
                        null=True,
                        verbose_name="操作仓库/区域",
                    ),
                ),
                (
                    "operation_location",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=255,
                        null=True,
                        verbose_name="操作位置",
                    ),
                ),
                (
                    "action_type",
                    models.CharField(
                        choices=[
                            ("create", "新增"),
                            ("update", "修改"),
                            ("delete", "删除"),
                            ("bind", "绑定"),
                            ("unbind", "解绑"),
                            ("schedule", "排约"),
                            ("transfer", "转仓/转干线"),
                            ("merge", "合板"),
                            ("split", "拆分"),
                            ("status", "状态变更"),
                            ("export", "导出"),
                            ("other", "其他"),
                        ],
                        db_index=True,
                        default="other",
                        max_length=20,
                        verbose_name="操作类型",
                    ),
                ),
                (
                    "action_detail",
                    models.TextField(blank=True, null=True, verbose_name="操作内容"),
                ),
                (
                    "operator_username",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        max_length=150,
                        null=True,
                        verbose_name="操作人用户名",
                    ),
                ),
                (
                    "request_path",
                    models.CharField(
                        blank=True,
                        max_length=500,
                        null=True,
                        verbose_name="请求路径",
                    ),
                ),
                (
                    "operation_time_utc",
                    models.DateTimeField(
                        auto_now_add=True,
                        db_index=True,
                        verbose_name="操作时间(UTC)",
                    ),
                ),
                (
                    "operation_time_beijing",
                    models.DateTimeField(
                        db_index=True,
                        default=django.utils.timezone.now,
                        verbose_name="国内操作时间",
                    ),
                ),
                (
                    "metadata",
                    models.JSONField(blank=True, default=dict, verbose_name="额外信息"),
                ),
                (
                    "operator",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="packinglist_pallet_operation_logs",
                        to=settings.AUTH_USER_MODEL,
                        verbose_name="操作人",
                    ),
                ),
            ],
            options={
                "verbose_name": "板子/packinglist修改记录",
                "verbose_name_plural": "板子/packinglist修改记录",
                "db_table": "warehouse_packinglist_pallet_operation_log",
                "ordering": ["-operation_time_utc"],
            },
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["target_type", "target_id"],
                name="warehouse_p_target__644f84_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["operator_username"],
                name="warehouse_p_operato_a04715_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["operation_time_beijing"],
                name="warehouse_p_operati_425f0e_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["action_type"],
                name="warehouse_p_action__d309b5_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["container_number"],
                name="warehouse_p_contain_e7181b_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(fields=["po_id"], name="warehouse_p_po_id_7e4c39_idx"),
        ),
        migrations.AddIndex(
            model_name="packinglistpalletoperationlog",
            index=models.Index(
                fields=["warehouse"], name="warehouse_p_warehou_119c01_idx"
            ),
        ),
    ]
