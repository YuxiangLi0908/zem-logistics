# Generated by Django 4.2.7 on 2025-01-08 06:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0137_alter_pallet_sequence_number"),
    ]

    operations = [
        migrations.AlterField(
            model_name="invoicepreport",
            name="chassis",
            field=models.FloatField(blank=True, null=True, verbose_name="托架费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="chassis_split",
            field=models.FloatField(blank=True, null=True, verbose_name="托架提取费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="congestion_fee",
            field=models.FloatField(blank=True, null=True, verbose_name="港口拥堵费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="demurrage",
            field=models.FloatField(blank=True, null=True, verbose_name="港内滞期费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="dry_run",
            field=models.FloatField(blank=True, null=True, verbose_name="空跑费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="exam_fee",
            field=models.FloatField(blank=True, null=True, verbose_name="查验费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="handling_fee",
            field=models.FloatField(blank=True, null=True, verbose_name="操作处理费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="hanging_crane",
            field=models.FloatField(blank=True, null=True, verbose_name="吊柜费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="hazmat",
            field=models.FloatField(blank=True, null=True, verbose_name="危险品"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="other_serive",
            field=models.FloatField(blank=True, null=True, verbose_name="其他服务"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="over_weight",
            field=models.FloatField(blank=True, null=True, verbose_name="超重费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="per_diem",
            field=models.FloatField(blank=True, null=True, verbose_name="港外滞期费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="pickup",
            field=models.FloatField(
                blank=True, null=True, verbose_name="提拆/打托缠膜"
            ),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="pier_pass",
            field=models.FloatField(blank=True, null=True, verbose_name="码头"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="prepull",
            field=models.FloatField(blank=True, null=True, verbose_name="预提费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="second_pickup",
            field=models.FloatField(blank=True, null=True, verbose_name="二次提货"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="urgent_fee",
            field=models.FloatField(blank=True, null=True, verbose_name="加急费"),
        ),
        migrations.AlterField(
            model_name="invoicepreport",
            name="yard_storage",
            field=models.FloatField(blank=True, null=True, verbose_name="货柜放置费"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="counting",
            field=models.FloatField(blank=True, null=True, verbose_name="货品清点费"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="destroy",
            field=models.FloatField(blank=True, null=True, verbose_name="销毁"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="handling",
            field=models.FloatField(blank=True, null=True, verbose_name=""),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="inner_outer_box",
            field=models.FloatField(blank=True, null=True, verbose_name="内外箱"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="inner_outer_box_label",
            field=models.FloatField(blank=True, null=True, verbose_name=""),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="intercept",
            field=models.FloatField(blank=True, null=True, verbose_name="拦截费"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="open_close_box",
            field=models.FloatField(blank=True, null=True, verbose_name="开封箱"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="pallet_label",
            field=models.FloatField(blank=True, null=True, verbose_name="托盘标签"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="po_activation",
            field=models.FloatField(blank=True, null=True, verbose_name="亚马逊PO激活"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="re_pallet",
            field=models.FloatField(blank=True, null=True, verbose_name="重新打板"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="repeated_operation_fee",
            field=models.FloatField(blank=True, null=True, verbose_name="重复操作费"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="self_pickup",
            field=models.FloatField(blank=True, null=True, verbose_name="客户自提"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="sorting",
            field=models.FloatField(blank=True, null=True, verbose_name="分拣费"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="specified_labeling",
            field=models.FloatField(blank=True, null=True, verbose_name="指定贴标"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="take_photo",
            field=models.FloatField(blank=True, null=True, verbose_name="拍照"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="take_video",
            field=models.FloatField(blank=True, null=True, verbose_name="拍视频"),
        ),
        migrations.AlterField(
            model_name="invoicewarehouse",
            name="warehouse_rent",
            field=models.FloatField(blank=True, null=True, verbose_name="仓租"),
        ),
    ]
