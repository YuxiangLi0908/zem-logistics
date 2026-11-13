import ast
import json
import uuid
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from asgiref.sync import async_to_sync
from django.template import Context, Engine
from django.contrib.auth.models import AnonymousUser, User
from django.test import RequestFactory, SimpleTestCase, TestCase
from bs4 import BeautifulSoup

from warehouse.models.container import Container
from warehouse.models.pallet import Pallet
from warehouse.models.packing_list import PackingList
from warehouse.models.shipment import Shipment
from warehouse.models.packinglist_pallet_operation_log import PackingListPalletOperationLog
from warehouse.views.post_port.warehouse.inventory import Inventory
from simple_history.utils import bulk_update_with_history


class InventoryPoIdentifiersTests(SimpleTestCase):
    def render_page(self, name, context):
        engine = Engine(
            dirs=[str(Path(__file__).parent / "templates")],
            loaders=[("django.template.loaders.locmem.Loader", {
                "base.html": "{% block content %}{% endblock %}",
            }), "django.template.loaders.filesystem.Loader"],
            libraries={"static": "django.templatetags.static"},
        )
        return engine.get_template(name).render(Context(context))

    def adjust(self, identifiers, count=1):
        """Run the actual adjustment function with database writes mocked."""
        source = (Path(__file__).parent / "views/post_port/warehouse/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        handler = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "handle_update_po_post")
        adjust = next(n for n in ast.walk(handler) if isinstance(n, ast.FunctionDef) and n.name == "adjust_pallets")
        adjust.decorator_list = []
        container = Container(id=1, container_number="TEST")
        old = Pallet(id=1, container_number=container, PO_ID="PO1", shipping_mark="PALLET-MARK",
                     fba_id="PALLET-FBA", ref_id="PALLET-REF")
        manager = MagicMock()
        manager.select_related.return_value.filter.return_value.order_by.return_value = [old]
        factory = MagicMock(side_effect=Pallet)
        factory.objects = manager
        namespace = dict(
            Pallet=factory, transaction=SimpleNamespace(atomic=nullcontext), uuid=uuid, seed=0,
            plt_ids=[1], n_pallet_new=count, pallet_identifiers=identifiers,
            destination_new="SBD1", address_new="", zipcode_new="", delivery_method_new="卡车派送",
            delivery_type_new="public", location_new="LA", note_new="",
            self=SimpleNamespace(_is_hold_delivery_method=lambda value: False),
            # PackingList form data must never supply card identifiers.
            shipping_mark_new=["OTHER-MARK"] * 8,
            fba_id_new=["OTHER-FBA"] * 8, ref_id_new=["OTHER-REF"] * 8,
        )
        exec(compile(ast.Module(body=[adjust], type_ignores=[]), "inventory-adjustment", "exec"), namespace)
        return namespace["adjust_pallets"]()

    def test_explicit_card_values_apply_to_kept_and_added_cards(self):
        rows = self.adjust({"shipping_mark": " CARD ", "fba_id": " FBA ", "ref_id": " REF "}, 2)
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id), ("CARD", "FBA", "REF"))

    def test_old_page_preserves_identifiers_even_when_packinglist_differs(self):
        for row in self.adjust(dict.fromkeys(("shipping_mark", "fba_id", "ref_id")), 2):
            self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id),
                             ("PALLET-MARK", "PALLET-FBA", "PALLET-REF"))

    def test_explicit_blank_clears_only_card_fields(self):
        row = self.adjust({"shipping_mark": "", "fba_id": " ", "ref_id": ""})[0]
        self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id), ("", "", ""))

    def test_page_has_separate_card_and_packinglist_inputs(self):
        html = self.render_page("post_port/inventory/02_inventory_po_update.html", {
            "user": AnonymousUser(),
            "pallet": {"shipping_mark": "CARD", "fba_id": "CARD-FBA", "ref_id": "CARD-REF"},
            "packing_list": [{"id": 2, "shipping_mark": "PL", "fba_id": "PL-FBA", "ref_id": None}],
        })
        soup = BeautifulSoup(html, "html.parser")
        for name, value in {"pallet_shipping_mark": "CARD", "pallet_fba_id": "CARD-FBA",
                            "pallet_ref_id": "CARD-REF", "shipping_mark_new": "PL",
                            "fba_id_new": "PL-FBA", "ref_id_new": ""}.items():
            self.assertEqual(soup.find("input", attrs={"name": name})["value"], value)

    def test_release_dialog_preserves_quoted_addresses_and_operation_layout(self):
        address = '123 "Main" St, O\'Brien <Unit 2> \\ FL'
        html = self.render_page("post_port/inventory/01_inventory_management_main.html", {
            "pallet": [{"plt_ids": "10,11", "container": "TEST", "address": address}],
            "pallet_json": '{"10,11": {"address": "quoted \\\"data\\\""}}',
            "active_section": "warehouse-special-process-sec",
        })
        soup = BeautifulSoup(html, "html.parser")
        section = soup.select_one("#warehouse-special-process-sec")
        self.assertIn("display: block", section["style"])
        self.assertEqual(section.select_one("thead th").text, "操作")
        operation = section.select_one("#warehouse-special tr td")
        self.assertEqual([button.text for button in operation.select("button")], ["分拣", "解扣", "更新PO"])
        release = operation.select_one("button[data-plt-ids]")
        self.assertEqual(release["data-plt-ids"], "10,11")
        self.assertEqual(release["data-address"], address)
        self.assertNotEqual(release.parent, operation.select_one("button[type=submit]").parent)
        fields = soup.select("#inventory-release-dialog input:not([type=hidden]), #inventory-release-dialog select")
        self.assertEqual([field["name"] for field in fields],
                         ["destination", "address", "zipcode", "delivery_method", "delivery_type_new"])


class InventoryReleaseTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="inventory-operator")
        self.view = Inventory()
        self.container = Container.objects.create(container_number="TEST-RELEASE", delivery_type="public")
        self.other_container = Container.objects.create(container_number="OTHER", delivery_type="public")
        self.created_at = datetime(2026, 10, 10, 8, tzinfo=timezone.utc)
        common = dict(container_number=self.container, PO_ID="PO1", destination="OLD", address="Old road",
                      zipcode=None, delivery_method="暂扣留仓(HOLD)", delivery_type="public")
        self.packing = [PackingList.objects.create(**common, shipping_mark=f"PL{i}", fba_id=f"FBA{i}",
                                                 ref_id=f"REF{i}", pcs=5, cbm=1.5) for i in range(2)]
        self.pallets = [Pallet.objects.create(**common, packing_list=self.packing[0], pallet_id=f"PLT{i}",
                                            shipping_mark="CARD", fba_id="CARD-FBA", ref_id="CARD-REF",
                                            weight_lbs=100 + i, pcs=10 + i, cbm=2 + i, note="Keep note",
                                            location="LA", created_at=self.created_at) for i in range(2)]
        self.unselected = Pallet.objects.create(**common, location="LA", pallet_id="UNCHANGED")
        self.other_po = PackingList.objects.create(**{**common, "PO_ID": "PO2"})
        self.other_pl = PackingList.objects.create(**{**common, "container_number": self.other_container})

    def request(self, **overrides):
        data = dict(step="release_pallet", warehouse="LA", plt_ids=",".join(str(p.pk) for p in self.pallets),
                    destination=" MCO2 ", address='123 "Main" St', zipcode="33811",
                    delivery_method="卡车派送", delivery_type_new="public")
        data.update(overrides)
        request = RequestFactory().post("/inventory/", data)
        request.user = self.user
        return request

    def test_post_updates_selected_cards_and_same_container_po_packing_lists_only(self):
        preserved_fields = ("id", "pallet_id", "packing_list_id", "pcs", "cbm", "weight_lbs", "shipping_mark",
                            "fba_id", "ref_id", "location", "note", "shipment_batch_number_id")
        before = list(Pallet.objects.order_by("id").values(*preserved_fields))
        response = async_to_sync(self.view.post)(self.request())
        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.content)["success"])
        for item in self.pallets + self.packing:
            item.refresh_from_db()
            self.assertEqual((item.destination, item.address, item.zipcode, item.delivery_method, item.delivery_type),
                             ("MCO2", '123 "Main" St', "33811", "卡车派送", "public"))
            self.assertEqual(item.history.first().history_user_id, self.user.pk)
        for item in (self.unselected, self.other_po, self.other_pl):
            item.refresh_from_db()
            self.assertEqual(item.destination, "OLD")
        self.assertEqual(before, list(Pallet.objects.order_by("id").values(*preserved_fields)))
        self.assertEqual(self.pallets[0].released_at, self.created_at)
        self.assertEqual(PackingListPalletOperationLog.objects.filter(operation_name="解扣").count(), 2)

    def test_self_pickup_forces_other_and_recalculates_container_type(self):
        self.view._update_pallet_delivery_info(self.request(delivery_method="客户自提", delivery_type_new="public"))
        for item in self.pallets + self.packing:
            item.refresh_from_db()
            self.assertEqual(item.delivery_type, "other")
        self.container.refresh_from_db()
        self.assertEqual(self.container.delivery_type, "mixed")

    def test_failure_after_packing_list_write_rolls_back_both_tables_and_history(self):
        history_counts = (Pallet.history.count(), PackingList.history.count())
        def fail_after_write(rows, model, **kwargs):
            bulk_update_with_history(rows, model, **kwargs)
            if model is PackingList:
                raise RuntimeError("packing list write failed")
        with patch("warehouse.views.post_port.warehouse.inventory.bulk_update_with_history", side_effect=fail_after_write):
            with self.assertLogs("warehouse.views.post_port.warehouse.inventory", level="ERROR"):
                response = async_to_sync(self.view.post)(self.request())
        self.assertEqual(response.status_code, 500)
        self.assertIn("packing list write failed", response.content.decode())
        for item in self.pallets + self.packing:
            item.refresh_from_db()
            self.assertEqual(item.destination, "OLD")
        self.assertEqual((Pallet.history.count(), PackingList.history.count()), history_counts)
        self.assertFalse(PackingListPalletOperationLog.objects.exists())

    def test_stale_or_shipped_selection_is_rejected_without_writing(self):
        for overrides in ({"plt_ids": ""}, {"plt_ids": "1,bad"}, {"warehouse": "NJ"},
                          {"plt_ids": f"{self.pallets[0].pk},999999"}, {"destination": ""}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.view._update_pallet_delivery_info(self.request(**overrides))
        shipment = Shipment.objects.create(shipment_batch_number="SHIPPED", is_shipped=True)
        Pallet.objects.filter(pk=self.pallets[0].pk).update(shipment_batch_number=shipment)
        with self.assertRaisesMessage(ValueError, "已发货"):
            self.view._update_pallet_delivery_info(self.request())
        self.pallets[0].refresh_from_db()
        self.assertEqual(self.pallets[0].destination, "OLD")

    def test_editing_address_alone_preserves_release_timestamp(self):
        release = datetime(2026, 10, 20, 8, tzinfo=timezone.utc)
        Pallet.objects.filter(pk__in=[p.pk for p in self.pallets]).update(delivery_method="卡车派送", released_at=release)
        self.view._update_pallet_delivery_info(self.request())
        self.pallets[0].refresh_from_db()
        self.assertEqual(self.pallets[0].released_at, release)
