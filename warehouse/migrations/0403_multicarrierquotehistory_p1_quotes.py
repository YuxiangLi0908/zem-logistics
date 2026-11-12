from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0402_feedetail_hot_warehouse")]

    operations = [
        migrations.AddField(
            model_name="multicarrierquotehistory",
            name="p1_quotes",
            field=models.JSONField(default=dict, blank=True, verbose_name="P1报价"),
        ),
    ]
