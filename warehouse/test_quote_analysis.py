import copy
import importlib
import io
import json
import uuid
from pathlib import Path
from datetime import timedelta
from types import SimpleNamespace

from django.apps import apps
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db import connection
from django.test import RequestFactory, SimpleTestCase, TransactionTestCase
from django.template import Context, Engine
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem, AutoQuotePrice, AutoQuoteProfile
from warehouse.test_auto_quote import parameters
from warehouse.utils.quote_analysis import build_analysis, series_statistics
from warehouse.utils.quote_analysis_storage import get_profile, index_item
from warehouse.utils.quote_identity_v1 import address_key, normalize_prices, profile_configuration
from warehouse.views.post_port.auto_quote import auto_quote_get, visible_batches


class AnalysisMathTests(SimpleTestCase):
    def test_analysis_template_compiles_and_renders(self):
        engine = Engine(dirs=[str(Path(__file__).parent / "templates")], libraries={"static": "django.templatetags.static"},
                        loaders=[("django.template.loaders.locmem.Loader", {"base.html": "{% block content %}{% endblock %}"}), "django.template.loaders.filesystem.Loader"])
        rendered = engine.get_template("post_port/new_sop/leader_check/auto_quote_analysis.html").render(Context({}))
        self.assertIn('id="qa-form"', rendered)
        self.assertIn('auto_quote_analysis.js', rendered)

    def test_statistics_known_series(self):
        result = series_statistics([{"date": str(i), "price": value} for i, value in enumerate([100, 110, 90])])
        self.assertAlmostEqual(result["volatility_pct"], 8.165, places=3)
        self.assertEqual(result["mean"], 100)
        self.assertEqual(result["latest_change"], -20)
        self.assertAlmostEqual(result["latest_change_pct"], -18.1818)
        self.assertEqual(result["period_change_pct"], -10)

    def test_missing_last_quote_is_not_zero_or_stale_latest(self):
        result = series_statistics([{"date": str(i), "price": value} for i, value in enumerate([100, 120, None])])
        self.assertIsNone(result["latest"])
        self.assertIsNone(result["latest_change_pct"])
        self.assertEqual(result["last_available"], 120)
        self.assertEqual(result["samples"], 2)
        self.assertIsNone(result["volatility_pct"])
        self.assertAlmostEqual(result["coverage_pct"], 66.6667)

    def test_zero_baseline_and_insufficient_samples_do_not_create_infinite_or_false_stability(self):
        result = series_statistics([{"date": str(i), "price": 0} for i in range(3)])
        self.assertIsNone(result["volatility_pct"])
        self.assertIsNone(result["range_pct"])
        self.assertIsNone(result["latest_change_pct"])

    def test_profile_ignores_pickup_date_and_row_order_but_not_price_conditions(self):
        original = parameters()
        original["items"].append({**original["items"][0], "weight": 800})
        variant = copy.deepcopy(original)
        variant["pickupDate"] = "2030-01-01"
        variant["items"].reverse()
        variant["declaredValue"] = "100.00"
        variant["originWarehouse"] = "Same physical origin, renamed"
        self.assertEqual(profile_configuration(original), profile_configuration(variant))
        for key, value in (("declaredValue", 101), ("needLiftgate", True), ("originDetailAddress", "Another street"), ("destinationType", 2)):
            changed = copy.deepcopy(original)
            changed[key] = value
            self.assertNotEqual(profile_configuration(original), profile_configuration(changed))

    def test_normalization_preserves_multiple_services_and_rejects_bad_prices(self):
        result = {"results": {"kakas": {"status": "success", "data": {"rates": [
            {"carrierName": "Carrier A", "carrierCode": "AA", "serviceCode": "STD", "totalPrice": "100.00"},
            {"carrierName": "Carrier A", "carrierCode": "AA", "serviceCode": "EXP", "totalPrice": 150, "currency": "CAD"},
            {"carrierName": "Carrier A", "totalPrice": "NaN"},
            {"carrierName": "Carrier A", "totalPrice": -1},
            {"totalPrice": 100},
        ]}}}}
        rows = normalize_prices(result)
        self.assertEqual(len(rows), 3)
        self.assertNotEqual(rows[0]["series_key"], rows[1]["series_key"])
        self.assertTrue(rows[0]["currency_assumed"])
        self.assertEqual(rows[1]["currency"], "CAD")
        self.assertFalse(rows[2]["comparable"])


class AnalysisDataTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("analyst")
        self.other = User.objects.create_user("other")
        self.base = (timezone.now() - timedelta(days=10)).replace(hour=12, minute=0, second=0, microsecond=0)
        self.address = {"city": "Newark", "state": "NJ", "zipcode": "07101", "address": "1 Test Street", "group": "NJ", "id": 1}
        self.profile = get_profile(parameters())

    def item(self, day, rates, *, address=None, lead=2, warning=False, user=None, hour=0, indexed=True):
        now = self.base + timedelta(days=day, hours=hour)
        form = parameters()
        form["pickupDate"] = (timezone.localdate(now) + timedelta(days=lead)).isoformat()
        batch = AutoQuoteBatch.objects.create(group="NJ", parameters=form, profile=self.profile, operator=user or self.user,
                                             submission_id=uuid.uuid4(), status="completed", finished_at=now)
        result = {"results": {"kakas": {"status": "success", "data": {"finish": not warning, "rates": rates}}}}
        if warning:
            result["results"]["kakas"]["warning"] = "timeout"
        item = AutoQuoteItem.objects.create(batch=batch, address_snapshot=address or self.address, started_at=now,
                                           finished_at=now + timedelta(minutes=1), status="partial" if warning else "success", result=result)
        if indexed:
            index_item(item.pk)
        return item

    def rate(self, carrier, price, service="STD", currency="USD"):
        return {"carrierName": carrier, "carrierCode": carrier, "serviceCode": service, "totalPrice": price, "currency": currency}

    def analyze(self, **kwargs):
        params = {"profile": str(self.profile.pk), "start": timezone.localdate(self.base).isoformat(),
                  "end": timezone.localdate(self.base + timedelta(days=7)).isoformat(), "lead": "2", **kwargs}
        return build_analysis(visible_batches(self.user), params)

    def test_zipcode_and_carrier_keyword_filters(self):
        self.item(0, [self.rate("Alpha", 100), self.rate("Beta", 200)])
        self.item(0, [self.rate("Alpha", 300)], address={**self.address, "zipcode": "07001", "address": "071 Road"})
        report = self.analyze(zipcode="071", carrier_query="aLPH")
        self.assertEqual(len(report["rows"]), 1)
        self.assertEqual(report["rows"][0]["latest"], 100)
        self.assertEqual(report["selected_address"], address_key(self.address))
        self.assertEqual(self.analyze(zipcode="Newark")["rows"], [])
        self.assertEqual(len(self.analyze(zipcode="07", carrier_query="std")["rows"]), 3)
        self.assertEqual(self.analyze(carrier_query="not found")["rows"], [])

    def test_profile_reused_and_indexing_is_idempotent(self):
        self.assertEqual(get_profile(parameters()).pk, self.profile.pk)
        item = self.item(0, [self.rate("A", 100), self.rate("B", 200)])
        index_item(item.pk)
        self.assertEqual(item.prices.count(), 2)
        self.assertEqual(AutoQuoteProfile.objects.count(), 1)

    def test_analysis_filters_and_groups_by_pickup_instead_of_query_day(self):
        first = self.item(0, [self.rate("A", 100)])
        last = self.item(1, [self.rate("A", 130)])
        pickup = first.batch.parameters["pickupDate"]
        last.batch.parameters["pickupDate"] = pickup
        last.batch.save(update_fields=["parameters"])
        data = self.analyze(start=pickup, end=pickup, lead="all", address=address_key(self.address))
        self.assertEqual(data["summary"]["daily_samples"], 1)
        self.assertEqual(data["rows"][0]["latest_date"], pickup)
        self.assertEqual(data["rows"][0]["latest"], 130)
        self.assertEqual(data["chart"][0]["points"][0]["batch_id"], last.batch_id)
        query_day = timezone.localdate(first.started_at).isoformat()
        self.assertEqual(self.analyze(start=query_day, end=query_day, lead="all")["rows"], [])

    def test_address_rows_group_all_services_before_pagination(self):
        for i in range(21):
            self.item(0, [self.rate("A", 100), self.rate("B", 200)],
                      address={**self.address, "address": f"{i:02d} Street"})
        data = self.analyze(group_by="address")
        self.assertEqual(data["total"], 21)
        self.assertEqual(data["pages"], 2)
        self.assertEqual(len(data["route_rows"]), 20)
        self.assertTrue(all(len(row["services"]) == 2 for row in data["route_rows"]))
        self.assertEqual(len(self.analyze(group_by="address", page="2")["route_rows"]), 1)

    def test_address_statistics_use_daily_minima_and_preserve_missing_latest(self):
        self.item(0, [self.rate("A", 100), self.rate("B", 200)])
        self.item(1, [self.rate("A", 150), self.rate("B", 80)])
        row = self.analyze(group_by="address")["route_rows"][0]
        self.assertEqual(row["latest"], 80)
        self.assertEqual(row["latest_change"], -20)
        self.assertEqual(row["mean"], 90)
        self.assertEqual(len(row["services"]), 2)
        self.item(2, [])
        row = self.analyze(group_by="address")["route_rows"][0]
        self.assertIsNone(row["latest"])
        self.assertEqual(row["winners"], [])

    def test_rankings_and_fixed_panel_index(self):
        for day, price in enumerate([100, 200, 100]):
            self.item(day, [self.rate("A", price), self.rate("B", 100)])
        data = self.analyze(address=address_key(self.address))
        self.assertEqual(data["summary"]["most_volatile"]["carrier"], "A")
        self.assertEqual(data["summary"]["most_stable"]["carrier"], "B")
        self.assertEqual(data["market_index"][1]["value"], 150)
        self.assertEqual(data["summary"]["common_routes"], 1)
        self.assertEqual(len(data["chart"]), 2)
        self.assertEqual(data["rows"][0]["samples"], 3)

    def test_same_day_retry_uses_latest_even_if_no_quotes(self):
        self.item(0, [self.rate("A", 100)])
        self.item(1, [self.rate("A", 200)])
        self.item(1, [], hour=1)
        data = self.analyze(address=address_key(self.address))
        self.assertEqual(data["summary"]["daily_samples"], 2)
        self.assertIsNone(data["rows"][0]["latest"])
        self.assertEqual(data["rows"][0]["samples"], 1)
        self.assertEqual(data["chart"][0]["points"][-1]["price"], None)

    def test_same_service_duplicates_take_minimum_but_all_original_rows_remain(self):
        item = self.item(0, [self.rate("A", 120), self.rate("A", 100), self.rate("A", 150, "EXP")])
        data = self.analyze()
        self.assertEqual(item.prices.count(), 3)
        self.assertEqual(len(data["rows"]), 2)
        std = next(row for row in data["rows"] if row["service_code"] == "STD")
        self.assertEqual(std["latest"], 100)
        self.assertEqual(data["diagnostics"]["duplicates_collapsed"], 1)

    def test_pickup_dates_include_all_leads_but_respect_currency_and_permissions(self):
        self.item(0, [self.rate("A", 100), self.rate("A", 999, currency="CAD")])
        self.item(1, [self.rate("A", 200)], warning=True)
        self.item(2, [self.rate("A", 888)], lead=3)
        self.item(3, [self.rate("A", 777)], user=self.other)
        data = self.analyze()
        self.assertEqual(data["rows"][0]["samples"], 2)
        self.assertEqual(data["rows"][0]["maximum"], 888)
        self.assertEqual(self.analyze(lead="2")["rows"], data["rows"])
        data = self.analyze(include_partial="1")
        self.assertEqual(data["rows"][0]["samples"], 3)
        self.assertEqual(data["rows"][0]["maximum"], 888)
        self.assertEqual(self.analyze(currency="CAD")["rows"][0]["maximum"], 999)

    def test_no_shared_routes_does_not_declare_best_and_worst(self):
        other_address = {**self.address, "address": "2 Different Street"}
        for day in range(3):
            self.item(day, [self.rate("A", 100 + day)])
            self.item(day, [self.rate("B", 1000 + day)], address=other_address)
        data = self.analyze()
        self.assertEqual(len(data["rankings"]), 2)
        self.assertEqual(data["summary"]["common_routes"], 0)
        self.assertIsNone(data["summary"]["most_volatile"])
        self.assertIsNone(data["summary"]["most_stable"])

    def test_rankings_require_shared_dates_not_just_shared_route(self):
        for day in range(6):
            self.item(day, [self.rate("A" if day < 3 else "B", 100 + day)])
        data = self.analyze()
        self.assertEqual(len(data["rankings"]), 2)
        self.assertEqual(data["summary"]["common_routes"], 0)
        self.assertIsNone(data["summary"]["most_volatile"])

    def test_latest_winners_and_gaps(self):
        self.item(0, [self.rate("A", 100), self.rate("B", 110)])
        self.item(1, [self.rate("A", 120), self.rate("B", 100)])
        self.item(2, [])
        data = self.analyze(address=address_key(self.address))
        self.assertEqual(data["summary"]["winner_changes"], 1)
        self.assertIsNone(data["minimum_history"][-1]["price"])
        self.assertEqual(data["market_index"], [])

    def test_identical_stability_is_reported_as_tie(self):
        for day in range(3):
            self.item(day, [self.rate("A", 100), self.rate("B", 200)])
        summary = self.analyze()["summary"]
        self.assertEqual(summary["most_stable_ties"], 2)
        self.assertEqual(summary["most_volatile_ties"], 2)

    def test_export_has_all_rows_and_preserves_filter_context(self):
        self.item(0, [self.rate(f"Carrier{i}", 100+i) for i in range(55)])
        self.assertEqual(len(self.analyze()["rows"]), 50)
        request = RequestFactory().get("/post_nsop/", {"kind": "analysis_export", "profile": self.profile.pk,
            "start": timezone.localdate(self.base).isoformat(), "end": timezone.localdate(self.base + timedelta(days=7)).isoformat(), "lead": "2"})
        request.user = self.user
        response = auto_quote_get(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.content.decode("utf-8-sig").splitlines()), 56)
        self.assertIn(self.profile.code, response.content.decode("utf-8-sig"))

    def test_api_rejects_invisible_profile(self):
        self.item(0, [self.rate("A", 100)], user=self.other)
        request = RequestFactory().get("/post_nsop/", {"kind": "analysis", "profile": self.profile.pk})
        request.user = self.user
        self.assertEqual(auto_quote_get(request).status_code, 400)
        request = RequestFactory().get("/post_nsop/", {"kind": "analysis_options"})
        request.user = self.user
        self.assertEqual(json.loads(auto_quote_get(request).content)["profiles"], [])

    def test_historical_migration_backfill_and_command_are_repeatable(self):
        item = self.item(0, [self.rate("A", 100)], indexed=False)
        AutoQuoteBatch.objects.filter(pk=item.batch_id).update(profile=None)
        migration = importlib.import_module("warehouse.migrations.0398_backfill_auto_quote_analysis")
        migration.backfill(apps, SimpleNamespace(connection=connection))
        migration.backfill(apps, SimpleNamespace(connection=connection))
        call_command("backfill_auto_quote_analysis", stdout=io.StringIO())
        item.refresh_from_db()
        self.assertEqual(item.analysis_version, 1)
        self.assertEqual(item.prices.count(), 1)
        self.assertEqual(item.batch.profile_id, self.profile.pk)
