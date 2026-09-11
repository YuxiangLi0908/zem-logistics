from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("warehouse", "0393_multicarrierquotehistory_kakas_request_payload"),
    ]

    operations = [
        migrations.AddField(
            model_name="multicarrierquotehistory",
            name="kakas_response_payload",
            field=models.JSONField(blank=True, null=True, verbose_name="卡卡省返回体"),
        ),
    ]
