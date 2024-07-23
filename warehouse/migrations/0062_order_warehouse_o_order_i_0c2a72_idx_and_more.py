# Generated by Django 4.2.7 on 2024-07-22 18:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0061_quote_warehouse_q_quote_i_336eff_idx_and_more"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="order",
            index=models.Index(
                fields=["order_id"], name="warehouse_o_order_i_0c2a72_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="order",
            index=models.Index(fields=["eta"], name="warehouse_o_eta_7c4927_idx"),
        ),
        migrations.AddIndex(
            model_name="order",
            index=models.Index(
                fields=["created_at"], name="warehouse_o_created_627fb4_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="retrieval",
            index=models.Index(
                fields=["retrieval_id"], name="warehouse_r_retriev_490587_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="retrieval",
            index=models.Index(
                fields=["target_retrieval_timestamp"],
                name="warehouse_r_target__72ca8c_idx",
            ),
        ),
    ]