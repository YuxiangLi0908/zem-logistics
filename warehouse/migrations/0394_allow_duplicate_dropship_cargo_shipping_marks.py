from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("warehouse", "0393_multicarrierquotehistory_kakas_request_payload"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="dropshipcargo",
            unique_together=set(),
        ),
    ]
