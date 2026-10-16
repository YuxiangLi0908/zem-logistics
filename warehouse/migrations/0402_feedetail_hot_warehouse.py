from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0401_auto_quote_batch_started_at")]

    operations = [
        migrations.AddField(
            model_name="feedetail",
            name="hot_warehouse",
            field=models.JSONField(default=list, blank=True, verbose_name="热门仓点"),
        ),
    ]
