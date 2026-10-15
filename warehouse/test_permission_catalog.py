from pathlib import Path

from django.template import Context, Engine
from django.test import SimpleTestCase

from warehouse.utils.permission_catalog import display_permissions, get_permission_catalog


class PermissionCatalogTests(SimpleTestCase):
    def test_superuser_display_rule(self):
        self.assertEqual(display_permissions({"superuser"}), "superuser")
        self.assertEqual(display_permissions({"superuser", "shipment_bind_public"}), "shipment_bind_public")
        self.assertEqual(display_permissions({"is_staff"}), "is_staff")

    def test_inventory_covers_groups_templates_and_admin(self):
        rows = get_permission_catalog()
        self.assertGreater(len(rows), 100)
        permissions = " ".join(row["permission"] for row in rows)
        for group in ("create_order_dropshipping", "create_order", "shipment_bind_public", "shipment_bind_other",
                      "unpacking_personnel", "shipment_leader", "invoice_preport_leader", "accounting",
                      "warehouse_public", "warehouse_other", "mix_account", "NJ_mix_account", "four_major_whs"):
            self.assertIn(group, permissions)
        self.assertTrue(any(row["permission"] == "superuser" and row["source"] == "warehouse/admin.py" for row in rows))
        self.assertTrue(any(row["name"] == "取消预报" and row["kind"] == "页面/按钮显示" for row in rows))
        for row in rows:
            self.assertTrue((Path(__file__).resolve().parent.parent / row["source"]).is_file())
            self.assertGreater(row["line"], 0)

    def test_catalog_renders_without_parameter_edit_forms(self):
        root = Path(__file__).parent / "templates"
        engine = Engine(dirs=[str(root)], libraries={"static": "django.templatetags.static"},
                        loaders=[("django.template.loaders.locmem.Loader", {"base.html": "{% block content %}{% endblock %}"}),
                                 "django.template.loaders.filesystem.Loader"])
        template = engine.get_template("post_port/new_sop/leader_check/system_parameter_add.html")
        rows = get_permission_catalog()
        html = template.render(Context({"current_category": "权限管理", "categories": [{"category": "权限管理", "count": len(rows)}],
                                        "permission_rows": rows, "csrf_token": "test"}))
        self.assertIn('id="permission-catalog-rows"', html)
        self.assertIn("shipment_bind_public", html)
        self.assertNotIn('value="system_parameter_add_item"', html)
        self.assertNotIn('value="system_parameter_delete_item"', html)
