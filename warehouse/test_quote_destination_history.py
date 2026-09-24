import json
import uuid
from datetime import timedelta

from django.contrib.auth.models import User
from django.test import RequestFactory, TransactionTestCase
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem
from warehouse.test_auto_quote import parameters
from warehouse.utils.quote_analysis_storage import get_profile, index_item
from warehouse.utils.quote_destination_history import destination_history
from warehouse.utils.quote_identity_v1 import address_key
from warehouse.views.post_port.auto_quote import auto_quote_get, visible_batches


class DestinationHistoryTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("operator", first_name="Test", last_name="User")
        self.other = User.objects.create_user("private")
        self.address = dict(city="Rahway", state="NJ", zipcode="07065", address="1 Main Street")
        self.now = timezone.now().replace(microsecond=0)

    def item(self, *, rates=None, address=None, user=None, value=100, lead=2, minute=0):
        now = self.now + timedelta(minutes=minute)
        form = parameters()
        form.update(declaredValue=value, pickupDate=(timezone.localdate(now) + timedelta(days=lead)).isoformat())
        batch = AutoQuoteBatch.objects.create(group="NJ", parameters=form, profile=get_profile(form),
                                             operator=user or self.user, submission_id=uuid.uuid4())
        item = AutoQuoteItem.objects.create(batch=batch, address_snapshot=address or self.address, status="success",
                                           started_at=now, finished_at=now + timedelta(seconds=10),
                                           result={"results": {"kakas": {"status": "success", "data": {"rates": rates or []}}}})
        index_item(item.pk)
        return item

    def history(self, **params):
        return destination_history(visible_batches(self.user), {"q": "Rahway 07065", **params})

    def test_cross_batch_history_keeps_each_inquiry_and_caps_only_chart_prices(self):
        rates = [{"carrierName": f"Carrier {i}", "totalPrice": i, "currency": "USD"} for i in range(200, 0, -1)]
        a = self.item(rates=rates)
        b = self.item(rates=[{"carrierName": "Other", "totalPrice": 12}], minute=1440)
        missing = self.item(minute=2880)
        data = self.history()
        self.assertEqual(data["total"], 3)
        self.assertEqual([p["id"] for p in data["chart"]], [a.pk, b.pk, missing.pk])
        self.assertEqual([p["price"] for p in data["chart"][0]["prices"]], list(range(1, 11)))
        self.assertEqual(data["chart"][2]["prices"], [])
        row = next(row for row in data["rows"] if row["id"] == a.pk)
        self.assertEqual(len(row["result"]["results"]["kakas"]["data"]["rates"]), 200)
        self.assertEqual(row["operator"], "Test User")

    def test_multiple_matches_require_exact_destination_for_chart(self):
        a = self.item()
        self.item(address={**self.address, "address": "2 Main Street"})
        self.item(address={**self.address, "id": 123, "group": "LA"})
        data = self.history()
        self.assertEqual(len(data["addresses"]), 2)
        self.assertEqual(data["chart"], [])
        data = self.history(address=address_key(a.address_snapshot))
        self.assertEqual(data["total"], 2)
        with self.assertRaises(ValueError):
            self.history(address="wrong")

    def test_pickup_chart_deduplicates_same_date_but_keeps_all_history(self):
        a = self.item(rates=[{"carrierName": "A", "totalPrice": 100}])
        b = self.item(rates=[{"carrierName": "B", "totalPrice": 150}], minute=1)
        pickup = a.batch.parameters["pickupDate"]
        data = self.history(start=pickup, end=pickup)
        self.assertEqual(data["total"], 2)
        self.assertEqual(len(data["chart"]), 1)
        self.assertEqual(data["chart"][0]["id"], b.pk)
        self.assertEqual(data["chart"][0]["time"], pickup)
        self.assertEqual(data["chart"][0]["prices"][0]["price"], 150)

    def test_chart_never_mixes_profile_currency_or_lead_but_table_keeps_all(self):
        a = self.item(rates=[{"carrierName": "A", "totalPrice": 99, "currency": "USD"},
                             {"carrierName": "B", "totalPrice": 1, "currency": "CAD"}])
        self.item(value=500)
        self.item(lead=5)
        data = self.history(profile=str(a.batch.profile_id), lead="2", currency="USD")
        self.assertEqual(data["total"], 3)
        self.assertEqual(len(data["chart"]), 1)
        self.assertEqual(data["chart"][0]["prices"][0]["price"], 99)
        self.assertEqual(data["currencies"], ["CAD", "USD"])

    def test_history_and_address_choices_are_permission_scoped_and_paginated(self):
        a = self.item()
        AutoQuoteItem.objects.bulk_create([AutoQuoteItem(batch=a.batch, address_snapshot=self.address) for _ in range(31)])
        self.item(user=self.other, address={**self.address, "address": "Secret Street"})
        data = self.history()
        self.assertEqual(data["total"], 32)
        self.assertEqual(len(data["rows"]), 30)
        self.assertEqual(len(data["addresses"]), 1)
        self.assertEqual(len(self.history(page="2")["rows"]), 2)
        self.user.is_staff = True
        self.assertEqual(self.history()["total"], 33)

    def test_endpoint_serialization_errors_and_operator_on_task_summary(self):
        self.item(rates=[{"carrierName": "A", "totalPrice": 10}])
        factory = RequestFactory()
        request = factory.get("/post_nsop/", {"kind": "destination_history", "q": "rahway"})
        request.user = self.user
        data = json.loads(auto_quote_get(request).content)
        self.assertTrue(data["success"])
        self.assertEqual(data["chart"][0]["prices"][0]["price"], "10.00")
        request = factory.get("/post_nsop/", {"kind": "destination_history", "q": "rahway", "start": "bad"})
        request.user = self.user
        self.assertEqual(auto_quote_get(request).status_code, 400)
        request = factory.get("/post_nsop/")
        request.user = self.user
        self.assertEqual(json.loads(auto_quote_get(request).content)["batches"][0]["operator"], "Test User")

    def test_task_date_filter_without_address_includes_both_endpoints(self):
        a = self.item()
        old = self.item(lead=5)
        AutoQuoteBatch.objects.filter(pk=old.batch_id).update(created_at=self.now - timedelta(days=5))
        factory = RequestFactory()
        day = a.batch.parameters["pickupDate"]
        request = factory.get("/post_nsop/", {"start": day, "end": day})
        request.user = self.user
        result = json.loads(auto_quote_get(request).content)
        self.assertEqual([batch["id"] for batch in result["batches"]], [a.batch_id])
        request = factory.get("/post_nsop/", {"start": "2026-10-01", "end": "2026-09-01"})
        request.user = self.user
        self.assertEqual(auto_quote_get(request).status_code, 400)
