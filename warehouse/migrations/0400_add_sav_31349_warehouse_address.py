import json

from django.db import migrations


def add_warehouse_address(apps, schema_editor):
    SystemParameter = apps.get_model("warehouse", "SystemParameter")
    parameters = SystemParameter.objects.using(schema_editor.connection.alias)
    address = json.dumps({
        "detailAddress": "774 King George Blvd",
        "city": "Savannah",
        "state": "GA",
        "postCode": "31419",
    })
    existing = parameters.filter(category="ZEM仓库地址", key="SAV-31349")
    if existing.exists():
        existing.update(value=address, is_active=True)
    else:
        parameters.create(
            category="ZEM仓库地址", key="SAV-31349", value=address, is_active=True
        )


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0399_auto_quote_worker_lifecycle")]

    operations = [
        migrations.RunPython(add_warehouse_address, migrations.RunPython.noop),
    ]
