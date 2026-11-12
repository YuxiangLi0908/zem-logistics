import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("warehouse", "0403_multicarrierquotehistory_p1_quotes"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="AutoQuoteCargoGroup",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=100)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("operator", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-updated_at", "-id"]},
        ),
        migrations.CreateModel(
            name="AutoQuoteCargoGroupMember",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("position", models.PositiveIntegerField()),
                ("group", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="members", to="warehouse.autoquotecargogroup")),
                ("profile", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="warehouse.autoquoteprofile")),
            ],
            options={"ordering": ["position", "id"]},
        ),
        migrations.AddField(
            model_name="autoquotecargogroup", name="profiles",
            field=models.ManyToManyField(through="warehouse.AutoQuoteCargoGroupMember", to="warehouse.autoquoteprofile"),
        ),
        migrations.AddField(
            model_name="autoquotebatch", name="cargo_group",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="warehouse.autoquotecargogroup"),
        ),
        migrations.AddField(model_name="autoquotebatch", name="cargo_group_name", field=models.CharField(blank=True, default="", max_length=100)),
        migrations.AddField(model_name="autoquotebatch", name="cargo_group_position", field=models.PositiveIntegerField(blank=True, null=True)),
        migrations.AddField(model_name="autoquotebatch", name="cargo_submission_id", field=models.UUIDField(blank=True, db_index=True, null=True)),
        migrations.AddConstraint(
            model_name="autoquotecargogroup",
            constraint=models.UniqueConstraint(fields=("operator", "name"), name="auto_quote_cargo_group_name_unique"),
        ),
        migrations.AddConstraint(
            model_name="autoquotecargogroupmember",
            constraint=models.UniqueConstraint(fields=("group", "profile"), name="auto_quote_cargo_member_unique"),
        ),
        migrations.AddConstraint(
            model_name="autoquotecargogroupmember",
            constraint=models.UniqueConstraint(fields=("group", "position"), name="auto_quote_cargo_position_unique"),
        ),
    ]
