import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("warehouse", "0391_packinglist_pallet_operation_name"),
    ]

    operations = [
        migrations.CreateModel(
            name="MultiCarrierQuoteHistory",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("origin_warehouse", models.CharField(db_index=True, max_length=100, verbose_name="发货仓库")),
                ("destination_warehouse", models.CharField(db_index=True, max_length=100, verbose_name="收货仓点")),
                ("pickup_date", models.DateField(db_index=True, verbose_name="取件日期")),
                ("quote_type", models.CharField(db_index=True, max_length=20, verbose_name="运输类型")),
                ("ftl_car_type", models.CharField(blank=True, default="", max_length=100, verbose_name="FTL车型")),
                ("freight_class", models.CharField(blank=True, default="", max_length=20, verbose_name="Freight Class")),
                ("declared_value", models.DecimalField(decimal_places=2, max_digits=12, verbose_name="申报价值")),
                ("pallet_items", models.JSONField(default=list, verbose_name="板子明细")),
                ("maersk_quotes", models.JSONField(blank=True, default=dict, verbose_name="马士基报价")),
                ("kakas_quotes", models.JSONField(blank=True, default=dict, verbose_name="卡卡省报价")),
                ("abf_quotes", models.JSONField(blank=True, default=dict, verbose_name="ABF报价")),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True, verbose_name="询价时间")),
                ("operator", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="multi_carrier_quote_histories", to=settings.AUTH_USER_MODEL, verbose_name="询价人")),
            ],
            options={
                "verbose_name": "三方询价历史",
                "verbose_name_plural": "三方询价历史",
                "db_table": "warehouse_multi_carrier_quote_history",
                "ordering": ["id"],
            },
        ),
    ]
