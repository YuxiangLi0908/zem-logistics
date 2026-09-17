"""Offline migration regression: python warehouse/test_dropship_duplicate_marks.py.

Uses a minimal historical schema in an in-memory SQLite database, never project
database credentials. The migration operation is imported from the real file.
"""
import ast
import importlib.util
import random
import re
import string
import unittest
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import django
from django.conf import settings
from django.db import IntegrityError, connection, models, transaction
from django.db.migrations.state import ModelState, ProjectState


class DuplicateShippingMarkTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).parent / "migrations/0394_allow_duplicate_dropship_cargo_shipping_marks.py"
        spec = importlib.util.spec_from_file_location("duplicate_marks_migration", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.operation = module.Migration.operations[0]
        self.before = ProjectState()
        self.before.add_model(ModelState("warehouse", "DropshipCargo", [
            ("id", models.AutoField(primary_key=True)),
            ("shipping_mark", models.CharField(max_length=200)),
            ("order", models.IntegerField()),
        ], options={
            "unique_together": {("shipping_mark", "order")},
            "indexes": [models.Index(fields=["order", "shipping_mark"], name="cargo_mark_lookup")],
        }))
        self.model = self.before.apps.get_model("warehouse", "DropshipCargo")
        with connection.schema_editor() as editor:
            editor.create_model(self.model)
        self.model.objects.create(shipping_mark="SAME", order=11688)

    def tearDown(self):
        with connection.schema_editor() as editor:
            editor.delete_model(self.model)

    def migrate(self):
        after = self.before.clone()
        self.operation.state_forwards("warehouse", after)
        with connection.schema_editor() as editor:
            self.operation.database_forwards("warehouse", editor, self.before, after)
        self.model = after.apps.get_model("warehouse", "DropshipCargo")

    def test_old_constraint_rejects_duplicate(self):
        with self.assertRaises(IntegrityError), transaction.atomic():
            self.model.objects.create(shipping_mark="SAME", order=11688)

    def test_migration_allows_duplicate_and_preserves_rows_and_index(self):
        self.migrate()
        self.model.objects.create(shipping_mark="SAME", order=11688)
        self.assertEqual(self.model.objects.filter(shipping_mark="SAME", order=11688).count(), 2)
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(cursor, self.model._meta.db_table)
        self.assertIn("cargo_mark_lookup", constraints)
        self.assertFalse(any(c["unique"] and not c["primary_key"] for c in constraints.values()))

    def test_update_by_id_keeps_same_mark_rows_independent(self):
        self.migrate()
        second = self.model.objects.create(shipping_mark="OTHER", order=11688)
        self.model.objects.filter(pk=second.pk).update(shipping_mark="SAME")
        self.assertEqual(self.model.objects.filter(shipping_mark="SAME").count(), 2)
        self.model.objects.filter(pk=second.pk).update(shipping_mark="CHANGED")
        self.assertEqual(self.model.objects.filter(shipping_mark="SAME").count(), 1)

    def test_save_rolls_back_cargo_when_container_save_fails(self):
        self.migrate()
        # Execute the actual synchronous save helper without importing the app's
        # external-service dependencies; cargo writes use the temporary database.
        source = (Path(__file__).parent / "views/dropshipping.py").read_text(encoding="utf-8")
        view = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == "Dropshipping")
        helper = next(n for n in view.body if isinstance(n, ast.FunctionDef) and n.name == "_save_order_cargos")
        order = SimpleNamespace(warehouse_id=None)
        order_manager = Mock()
        order_manager.select_for_update.return_value.get.return_value = order
        order_manager.select_related.return_value.get.return_value = order
        container = SimpleNamespace(save=Mock(side_effect=RuntimeError("container save failed")))
        container_manager = Mock()
        container_manager.get.return_value = container
        cargo_manager = Mock()
        cargo_manager.filter.side_effect = lambda **kw: (
            self.model.objects.filter(id__in=kw["id__in"]) if "id__in" in kw
            else SimpleNamespace(aggregate=lambda **kw: {"total": 0})
        )
        def bulk_update(rows, model, fields):
            for row in rows:
                self.model.objects.filter(pk=row.pk).update(shipping_mark=row.shipping_mark)
        namespace = dict(transaction=transaction, Order=SimpleNamespace(objects=order_manager),
                         Container=SimpleNamespace(objects=container_manager),
                         DropshipCargo=SimpleNamespace(objects=cargo_manager),
                         bulk_update_with_history=bulk_update, Decimal=Decimal,
                         InvalidOperation=InvalidOperation, random=random, string=string,
                         re=re, Sum=models.Sum, DELIVERY_METHOD_CODE={})
        exec(compile(ast.Module(body=[helper], type_ignores=[]), "save-helper", "exec"), namespace)
        pk = self.model.objects.get().pk
        request = SimpleNamespace(POST={f"shipping_mark_{pk}": "CHANGED", f"delivery_method_{pk}": "pickup"})
        with self.assertRaisesRegex(RuntimeError, "container save failed"):
            namespace["_save_order_cargos"](SimpleNamespace(convert_delivery_method=lambda x: x), request, "TEST1234567", [pk])
        self.assertEqual(self.model.objects.get(pk=pk).shipping_mark, "SAME")


if __name__ == "__main__":
    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=[], SECRET_KEY="offline-test-only",
    )
    django.setup()
    unittest.main(verbosity=2)
