import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from asgiref.sync import async_to_sync
from bs4 import BeautifulSoup
from django.contrib.auth.models import AnonymousUser, User
from django.db import connection
from django.db.models import Aggregate
from django.template import Context, Engine
from django.test import RequestFactory, TestCase
from simple_history.utils import bulk_update_with_history

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.views.post_port.warehouse.shipped_inventory import ShippedInventory


class SQLiteIds(Aggregate):
    function = "GROUP_CONCAT"
    allow_distinct = True


class ShippedInventoryTests(TestCase):
    def setUp(self):
        self.view = ShippedInventory()
        self.user = User.objects.create_user(username="shipped-operator")
        self.date = datetime(2026, 11, 20, 8, 30, 10, tzinfo=timezone.utc)
        self.customer = Customer.objects.create(zem_name="Original customer")
        self.other_customer = Customer.objects.create(zem_name="Other customer")
        self.container = Container.objects.create(container_number="SHIPPED-TEST", delivery_type="public")
        self.offload = Offload.objects.create(offload_id="OFFLOAD", offload_at=self.date)
        self.order = Order.objects.create(container_number=self.container, customer_name=self.customer,
                                          created_at=self.date, offload_id=self.offload)
        self.shipment = Shipment.objects.create(shipment_batch_number="BATCH-1", appointment_id="ISA-1", is_shipped=False, shipped_at=self.date)
        self.packing = PackingList.objects.create(container_number=self.container, PO_ID="PO1", destination="ORIGINAL", pcs=10)
        self.pallets = [self.card(pallet_id=f"CARD-{i}", pcs=5 + i, weight_lbs=100 + i, cbm=1 + i) for i in range(2)]
        self.other = self.card(pallet_id="UNSELECTED", destination="OTHER", PO_ID="PO2", weight_lbs=33, pcs=2, cbm=0.5)
        if connection.vendor == "sqlite":
            self.sqlite_ids = patch("warehouse.views.post_port.warehouse.shipped_inventory.StringAgg",
                                    side_effect=lambda expression, **kwargs: SQLiteIds(expression, distinct=True))
            self.sqlite_ids.start()
            self.addCleanup(self.sqlite_ids.stop)

    def card(self, **overrides):
        values = dict(container_number=self.container, shipment_batch_number=self.shipment, location="LA", PO_ID="PO1",
                      destination="ORIGINAL", delivery_method="卡车派送", delivery_type="public", shipping_mark="MARK",
                      fba_id="FBA", ref_id="REF", note="Note", packing_list=self.packing, created_at=self.date)
        values.update(overrides)
        return Pallet.objects.create(**values)

    def payload(self, **overrides):
        selection = {"warehouse": "LA", "plt_ids": ",".join(str(p.pk) for p in self.pallets), "order_id": str(self.order.pk)}
        data = self.view.row_data(selection)
        values = {**data, "warehouse": "LA", "step": "save"}
        values.update(overrides)
        return values

    def request(self, data):
        request = RequestFactory().post("/shipped_inventory/", data)
        request.user = self.user
        return request

    def test_query_includes_date_only_and_flag_only_and_filters_warehouse_type(self):
        flag_only = Shipment.objects.create(shipment_batch_number="FLAG", is_shipped=True)
        pending = Shipment.objects.create(shipment_batch_number="PENDING", is_shipped=False)
        included = self.card(shipment_batch_number=flag_only)
        excluded = self.card(shipment_batch_number=pending)
        no_shipment = self.card(shipment_batch_number=None)
        other_warehouse = self.card(location="NJ")
        other_type = self.card(delivery_type="other")
        rows = list(self.view.rows_query("LA", "public"))
        ids = {int(value) for row in rows for value in row["plt_ids"].split(",")}
        self.assertEqual(ids, {p.pk for p in self.pallets} | {included.pk, self.other.pk})
        self.assertTrue({excluded.pk, no_shipment.pk, other_warehouse.pk, other_type.pk}.isdisjoint(ids))
        self.assertEqual(len([row for row in rows if row["PO_ID"] == "PO1"]), 2)
        original = next(row for row in rows if row["shipment_batch_number_id"] == self.shipment.pk and row["PO_ID"] == "PO1")
        self.assertEqual((original["n_pallet"], original["weight"], original["pcs"], original["cbm"]), (2, 201, 11, 3))

    def test_edit_matches_inventory_scope_and_preserves_unselected_cards(self):
        request = self.request(self.payload(customer_id=str(self.other_customer.pk), container="CORRECTED",
                                            destination="MCO2", address="New address", zipcode="33811",
                                            shipping_mark="NEW-MARK", fba_id="NEW-FBA", ref_id="NEW-REF",
                                            offload_at="2026-11-19T09:10:11", shipped_at="2026-11-20T10:11:12",
                                            weight="301.0001", pcs="14", cbm="4.01", n_pallet="3", note="NEW-NOTE",
                                            shipment="CORRECTED-BATCH", appointment_id="CORRECTED-ISA"))
        response = async_to_sync(self.view.post)(request)
        self.assertEqual(response.status_code, 200, response.content)
        self.assertTrue(json.loads(response.content)["success"])
        cards = list(Pallet.objects.filter(PO_ID="PO1"))
        self.assertEqual(len(cards), 3)
        self.assertTrue({p.pk for p in self.pallets}.issubset({p.pk for p in cards}))
        self.assertAlmostEqual(sum(p.weight_lbs for p in cards), 301.0001)
        self.assertEqual(sum(p.pcs for p in cards), 14)
        self.assertAlmostEqual(sum(p.cbm for p in cards), 4.01)
        for card in cards:
            self.assertEqual((card.destination, card.shipping_mark, card.fba_id, card.ref_id, card.note),
                             ("MCO2", "NEW-MARK", "NEW-FBA", "NEW-REF", "NEW-NOTE"))
            self.assertEqual(card.shipment_batch_number_id, self.shipment.pk)
            self.assertEqual(card.packing_list_id, self.packing.pk)
            self.assertEqual(card.history.first().history_user_id, self.user.pk)
        self.order.refresh_from_db(); self.container.refresh_from_db(); self.offload.refresh_from_db(); self.shipment.refresh_from_db()
        self.assertEqual(self.order.customer_name_id, self.customer.pk)
        self.assertEqual(self.container.container_number, "SHIPPED-TEST")
        self.assertEqual(self.offload.offload_at, self.date)
        self.assertEqual(self.shipment.shipped_at, self.date)
        self.assertEqual((self.shipment.shipment_batch_number, self.shipment.appointment_id), ("BATCH-1", "ISA-1"))
        self.other.refresh_from_db(); self.packing.refresh_from_db()
        self.assertEqual((self.other.destination, self.other.weight_lbs), ("OTHER", 33))
        self.assertEqual((self.packing.destination, self.packing.address, self.packing.zipcode), ("MCO2", "New address", "33811"))
        self.assertIsNone(self.packing.fba_id)

    def test_count_reduction_and_old_revision_cannot_repeat_save(self):
        data = self.payload(n_pallet="1")
        result = self.view.update_row(self.request(data))
        self.assertEqual(result["ids"], [self.pallets[0].pk])
        remaining = Pallet.objects.get(pk=self.pallets[0].pk)
        self.assertEqual((remaining.weight_lbs, remaining.pcs, remaining.cbm), (201, 11, 3))
        with self.assertRaises(ValueError):
            self.view.update_row(self.request(data))

    def test_changed_or_already_saved_data_requires_refresh(self):
        data = self.payload(n_pallet="3")
        self.view.update_row(self.request(data))
        with self.assertRaisesMessage(ValueError, "刷新"):
            self.view.update_row(self.request(data))
        self.assertEqual(Pallet.objects.filter(PO_ID="PO1").count(), 3)

    def test_late_failure_rolls_back_cards_packing_lists_and_history(self):
        before = (Pallet.history.count(), PackingList.history.count())
        request = self.request(self.payload(n_pallet="3", customer_id=str(self.other_customer.pk),
                                            destination="CHANGED"))
        def fail_after_write(rows, model, **kwargs):
            bulk_update_with_history(rows, model, **kwargs)
            if model is PackingList:
                raise RuntimeError("packing list write failed")
        with patch("warehouse.views.post_port.warehouse.shipped_inventory.bulk_update_with_history", side_effect=fail_after_write):
            with self.assertLogs("warehouse.views.post_port.warehouse.shipped_inventory", level="ERROR"):
                response = async_to_sync(self.view.post)(request)
        self.assertEqual(response.status_code, 500)
        self.assertIn("packing list write failed", response.content.decode())
        self.assertEqual(Pallet.objects.filter(PO_ID="PO1").count(), 2)
        self.order.refresh_from_db(); self.offload.refresh_from_db(); self.packing.refresh_from_db()
        self.assertEqual(self.order.customer_name_id, self.customer.pk)
        self.assertEqual(self.offload.offload_at, self.date)
        self.assertEqual(self.packing.destination, "ORIGINAL")
        self.assertEqual((Pallet.history.count(), PackingList.history.count()), before)

    def test_invalid_values_reject_before_writes(self):
        for override in ({"n_pallet": "0"}, {"weight": "NaN"}, {"pcs": "1.5"}, {"zipcode": "X" * 201},
                         {"destination": ""}, {"delivery_type": "INVALID"}, {"location": "X" * 101}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                self.view.update_row(self.request(self.payload(**override)))
        self.assertEqual(Pallet.objects.count(), 3)

    def test_missing_offload_does_not_prevent_card_updates_or_create_shared_records(self):
        self.order.offload_id = None
        self.order.save()
        self.view.update_row(self.request(self.payload(note="Updated card")))
        self.order.refresh_from_db()
        self.assertIsNone(self.order.offload_id_id)
        self.pallets[0].refresh_from_db()
        self.assertEqual(self.pallets[0].note, "Updated card")
        self.assertEqual(Order.objects.count(), 1)

    def test_note_only_edit_does_not_redistribute_unchanged_totals(self):
        original = [(p.pk, p.weight_lbs, p.pcs, p.cbm) for p in self.pallets]
        self.view.update_row(self.request(self.payload(note="ONLY NOTE")))
        after = list(Pallet.objects.filter(pk__in=[p.pk for p in self.pallets]).order_by("pk").values_list("pk", "weight_lbs", "pcs", "cbm"))
        self.assertEqual(after, original)

    def test_unauthenticated_save_is_rejected(self):
        request = self.request({"step": "save"})
        request.user = AnonymousUser()
        self.assertEqual(async_to_sync(self.view.post)(request).status_code, 401)

    def test_template_columns_in_requested_order_and_all_edit_fields(self):
        engine = Engine(dirs=[str(Path(__file__).parent / "templates")],
                        loaders=[("django.template.loaders.locmem.Loader", {"base.html": "{% block content %}{% endblock %}"}), "django.template.loaders.filesystem.Loader"],
                        libraries={"static": "django.templatetags.static", "tz": "django.templatetags.tz"})
        html = engine.get_template(self.view.template_name).render(Context({
            "pallet": list(self.view.rows_query("LA")), "warehouse": "LA", "searched": True,
        }))
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual([th.text for th in soup.select("#shipped-table thead tr:first-child th")],
                         ["操作", "客户", "柜号", "目的地", "派送方式", "拆柜时间", "出库时间", "唛头", "FBA", "REF", "weight(lbs)", "pcs", "cbm", "卡板数", "备注", "预约批次", "预约号"])
        self.assertEqual(len(soup.select("#shipped-table tbody tr:first-child td")), 17)
        for name in ("customer_name", "container", "destination", "delivery_method", "offload_at", "shipped_at",
                     "shipping_mark", "fba_id", "ref_id", "weight", "pcs", "cbm", "n_pallet", "note", "shipment", "appointment_id"):
            self.assertIsNotNone(soup.select_one(f'#shipped-edit-form [name="{name}"]'))
        for name in ("customer_name", "container", "offload_at", "shipped_at", "shipment", "appointment_id"):
            self.assertTrue(soup.select_one(f'[name="{name}"]').has_attr("readonly"))
