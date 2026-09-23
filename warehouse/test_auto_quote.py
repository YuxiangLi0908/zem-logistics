import asyncio
import io
import json
import uuid
from datetime import timedelta
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from django.contrib.auth.models import AnonymousUser, User
from django.core.exceptions import PermissionDenied
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import Http404
from django.template import Context, Engine
from pathlib import Path
from django.test import SimpleTestCase, TransactionTestCase, RequestFactory
from django.utils import timezone
from openpyxl import Workbook

from warehouse.models.auto_quote import AutoQuoteAddress, AutoQuoteBatch, AutoQuoteItem
from warehouse.utils.auto_quote import (
    address_payload, claim_item, classify_result, complete_item, create_batch,
    import_addresses, read_addresses, save_checkpoint, stop_batch,
)
from warehouse.utils.multi_carrier_quote import execute_quote, prepare_quote
from warehouse.views.post_port.auto_quote import auto_quote_get, auto_quote_post
from warehouse.management.commands.run_auto_quote_worker import process_item, RequestPacer


def parameters():
    return {"originWarehouse": "NJ test", "originCity": "Newark", "originState": "NJ", "originPostCode": "07101",
            "originDetailAddress": "1 Test Street", "pickupDate": (timezone.localdate() + timedelta(days=2)).isoformat(),
            "quoteType": 1, "carType": 0, "declaredValue": 100, "commodityUnit": 11, "palletType": 1,
            "destinationType": 1, "needLiftgate": False,
            "items": [{"description": "Pallet", "pieces": 2, "length": 48, "width": 40, "height": 50,
                       "weight": 500, "palletCount": 3}]}


def workbook(rows, headers=None):
    wb = Workbook()
    wb.active.append(headers or [None, "城市", "州", "邮编", "详细地址", "距仓库距离（miles）"])
    for row in rows:
        wb.active.append(row)
    stream = io.BytesIO()
    wb.save(stream)
    return SimpleUploadedFile("addresses.xlsx", stream.getvalue())


def successful_result():
    return {"results": {"maersk": {"status": "success", "data": {"quotes": [{"TotalQuote": 100}]}},
                        "kakas": {"status": "success", "data": {"finish": True, "rates": [{"totalPrice": 120}]}}}}


class AddressParsingTests(SimpleTestCase):
    def test_templates_render_without_live_database(self):
        base = Path(__file__).parent / "templates"
        engine = Engine(dirs=[str(base)], libraries={"static": "django.templatetags.static"},
                        loaders=[("django.template.loaders.locmem.Loader", {"base.html": "{% block content %}{% endblock %}"}),
                                 "django.template.loaders.filesystem.Loader"])
        for name in ("multi_carrier_quote.html", "auto_quote_history.html"):
            rendered = engine.get_template("post_port/new_sop/leader_check/" + name).render(Context({"zem_warehouse_addresses": [], "csrf_token": "test-token"}))
            self.assertIn('id="auto-quote-tasks"', rendered)
            self.assertIn("auto_quote.js", rendered)

    def test_b_to_f_and_numeric_zip_preserve_leading_zero(self):
        rows, errors = read_addresses(workbook([["ignored", " Newark ", "nj", 7101, "1 Test Street", 12.5],
                                               [None, "Boston", "MA", "02108-1234", "2 Test Street", None]]))
        self.assertEqual(errors, [])
        self.assertEqual(rows[0]["zipcode"], "07101")
        self.assertEqual(rows[0]["state"], "NJ")
        self.assertEqual(str(rows[0]["distance_miles"]), "12.50")
        self.assertEqual(rows[1]["zipcode"], "02108-1234")

    def test_invalid_rows_are_reported_with_excel_line_numbers(self):
        rows, errors = read_addresses(workbook([[None, "Newark", "New Jersey", "07101", "Street", 1],
                                               [None, "Newark", "NJ", "07101", "=A1", 1],
                                               [None, "Newark", "NJ", "07101", "Street", -1]]))
        self.assertEqual(rows, [])
        self.assertEqual(len(errors), 3)
        self.assertIn("第2行", errors[0])

    def test_header_alignment_is_required(self):
        with self.assertRaises(ValueError):
            read_addresses(workbook([], headers=["城市", "州", "邮编", "详细地址", "距仓库距离（miles）"]))

    def test_payload_conversion_keeps_manual_weight_semantics(self):
        form = address_payload(parameters(), {"city": "Boston", "state": "MA", "zipcode": "02108", "address": "1 Test"})
        payload, classes = prepare_quote(form)
        self.assertEqual(payload["carrierPayloads"]["kakas"]["commodityList"][0]["weight"], 1500)
        self.assertEqual(payload["carrierPayloads"]["maersk"]["lineItems"][0]["weight"], 500)
        self.assertEqual(len(classes), 1)

    def test_result_classification_separates_partial_failure_and_no_quote(self):
        result = successful_result()
        self.assertEqual(classify_result(result)[0], "success")
        result["results"]["kakas"]["warning"] = "timeout"
        self.assertEqual(classify_result(result)[0], "partial")
        result = {"results": {name: {"status": "success", "data": {"rates": []}} for name in ("maersk", "kakas")}}
        self.assertEqual(classify_result(result)[0], "no_quote")
        result["results"]["maersk"]["status"] = "error"
        self.assertEqual(classify_result(result)[0], "failed")


class QueueTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("quote-user")
        self.factory = RequestFactory()
        rows, _ = read_addresses(workbook([[None, "Newark", "NJ", "07101", f"{i} Test Street", 5] for i in range(5)]))
        self.rows = rows
        import_addresses("NJ", rows, self.user.pk)

    def batch(self):
        return create_batch("NJ", parameters(), self.user.pk, uuid.uuid4())

    def request(self, method, values):
        request = getattr(self.factory, method)("/post_nsop/", values)
        request.user = self.user
        return request

    def test_import_deduplicates_within_group_but_not_across_groups(self):
        result = import_addresses("NJ", self.rows * 2, self.user.pk)
        self.assertEqual(result["added"], 0)
        self.assertEqual(result["duplicates"], 10)
        self.assertEqual(import_addresses("LA", self.rows, self.user.pk)["added"], 5)

    def test_batch_snapshot_and_duplicate_submission(self):
        token = uuid.uuid4()
        batch = create_batch("NJ", parameters(), self.user.pk, token)
        self.assertEqual(create_batch("NJ", parameters(), self.user.pk, token).pk, batch.pk)
        self.assertEqual(create_batch("NJ", parameters(), self.user.pk, uuid.uuid4()).pk, batch.pk)
        AutoQuoteAddress.objects.update(address="Changed later")
        self.assertNotEqual(batch.items.first().address_snapshot["address"], "Changed later")
        self.assertEqual(batch.items.count(), 5)

    def test_five_hundred_address_batch_is_queued_and_paginated(self):
        AutoQuoteAddress.objects.all().delete()
        AutoQuoteAddress.objects.bulk_create([AutoQuoteAddress(
            group="NJ", city="Newark", state="NJ", zipcode="07101", address=f"{i} Street", fingerprint=str(i)) for i in range(523)])
        batch = self.batch()
        self.assertEqual(batch.items.filter(status="pending").count(), 523)
        body = json.loads(auto_quote_get(self.request("get", {"kind": "batch", "batch": batch.pk})).content)
        self.assertEqual(len(body["rows"]), 30)
        self.assertEqual(body["pages"], 18)
        self.assertEqual(body["batch"]["total"], 523)

    def test_expired_pickup_date_is_rejected_before_queue_creation(self):
        form = parameters()
        form["pickupDate"] = (timezone.localdate() - timedelta(days=1)).isoformat()
        with self.assertRaises(ValueError):
            create_batch("NJ", form, self.user.pk, uuid.uuid4())
        self.assertFalse(AutoQuoteBatch.objects.exists())

    def test_global_concurrency_limit_and_stale_lease_fencing(self):
        self.batch()
        items = [claim_item(3) for _ in range(3)]
        self.assertTrue(all(items))
        self.assertIsNone(claim_item(3))
        old = items[0]
        AutoQuoteItem.objects.filter(pk=old.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        replacement = claim_item(3)
        self.assertEqual(replacement.pk, old.pk)
        self.assertNotEqual(replacement.lease_token, old.lease_token)
        with self.assertRaises(RuntimeError):
            save_checkpoint(old, {}, "old", {})
        save_checkpoint(replacement, successful_result(), "new", {})
        self.assertEqual(AutoQuoteItem.objects.get(pk=old.pk).quote_uuid, "new")

    def test_stop_keeps_running_item_and_retry_creates_new_batch(self):
        batch = self.batch()
        item = claim_item()
        stop_batch(batch)
        self.assertEqual(batch.items.filter(status="cancelled").count(), 4)
        self.assertIsNone(claim_item())
        complete_item(item, "success")
        batch.refresh_from_db()
        self.assertEqual(batch.status, "stopped")
        retry = create_batch("NJ", batch.parameters, self.user.pk, uuid.uuid4(), parent=batch)
        self.assertEqual(retry.parent_id, batch.pk)
        self.assertEqual(retry.items.count(), 4)
        self.assertEqual(batch.items.filter(status="success").count(), 1)

    def test_worker_persists_results_without_manual_history(self):
        batch = self.batch()
        item = claim_item()

        async def fake_execute(form, **kwargs):
            result = successful_result()
            await kwargs["checkpoint"](result, "uuid-test", {"test": True})
            return result, {}

        with patch("warehouse.management.commands.run_auto_quote_worker.execute_quote", fake_execute):
            async_to_sync(process_item)(item, RequestPacer())
        item.refresh_from_db()
        self.assertEqual(item.status, "success")
        self.assertEqual(item.quote_uuid, "uuid-test")
        self.assertIsNotNone(item.finished_at)

    def test_failed_item_does_not_stop_remaining_queue(self):
        self.batch()
        item = claim_item()
        with patch("warehouse.management.commands.run_auto_quote_worker.execute_quote", AsyncMock(side_effect=ValueError("test failure"))), self.assertLogs("warehouse.management.commands.run_auto_quote_worker", level="ERROR"):
            async_to_sync(process_item)(item, RequestPacer())
        item.refresh_from_db()
        self.assertEqual(item.status, "failed")
        self.assertIsNotNone(claim_item())

    def test_import_endpoint_is_atomic_on_invalid_rows(self):
        request = self.request("post", {"step": "auto_quote_import", "group": "LA", "file": workbook([
            [None, "Newark", "NJ", "07101", "1 Test", 1], [None, "Bad", "XX", "bad", "2 Test", 1]])})
        self.assertEqual(auto_quote_post(request).status_code, 400)
        self.assertFalse(AutoQuoteAddress.objects.filter(group="LA").exists())

    def test_access_control_and_paginated_detail(self):
        batch = self.batch()
        request = self.request("get", {"kind": "batch", "batch": batch.pk})
        body = json.loads(auto_quote_get(request).content)
        self.assertEqual(len(body["rows"]), 5)
        request.user = User.objects.create_user("other-user")
        with self.assertRaises(Http404):
            auto_quote_get(request)
        request.user = AnonymousUser()
        with self.assertRaises(PermissionDenied):
            auto_quote_get(request)

    def test_copy_returns_full_batch_with_all_quotes_despite_page_and_status(self):
        batch = self.batch()
        rates = [{"carrierName": f"Carrier {i}", "totalPrice": i + 1} for i in range(200)]
        batch.items.update(result={"results": {"kakas": {"data": {"rates": rates}}}})
        AutoQuoteItem.objects.bulk_create([AutoQuoteItem(batch=batch, address_snapshot={"city": "Extra"}) for _ in range(31)])
        response = auto_quote_get(self.request("get", {"kind": "batch_copy", "batch": batch.pk, "page": 2, "status": "success"}))
        body = json.loads(response.content)
        self.assertEqual(len(body["rows"]), 36)
        self.assertEqual(len(body["rows"][0]["result"]["results"]["kakas"]["data"]["rates"]), 200)
        self.assertIn("address", body["rows"][0])
        self.assertNotIn("request_payload", body["rows"][0])
        request = self.request("get", {"kind": "batch_copy", "batch": batch.pk})
        request.user = User.objects.create_user("copy-other-user")
        with self.assertRaises(Http404):
            auto_quote_get(request)

    def test_start_resolves_origin_from_server_and_exports(self):
        origin = {"warehouse": "NJ test", "city": "Newark", "state": "NJ", "postCode": "07101", "detailAddress": "1 Test"}
        request = self.request("post", {"step": "auto_quote_start", "group": "NJ", "submission_id": str(uuid.uuid4()), "quote_payload": json.dumps(parameters())})
        with patch("warehouse.views.post_port.auto_quote.SystemParameter.get_zem_warehouse_addresses", return_value=[origin]), patch("warehouse.views.post_port.auto_quote.gateway_config"):
            response = auto_quote_post(request)
        self.assertEqual(response.status_code, 200, response.content)
        batch_id = json.loads(response.content)["batch"]["id"]
        response = auto_quote_get(self.request("get", {"step": "auto_quote_export", "batch": batch_id}))
        self.assertEqual(response.status_code, 200)
        self.assertIn("07101", response.content.decode("utf-8"))


class GatewayResumeTests(IsolatedAsyncioTestCase):
    async def test_recovery_polls_existing_uuid_without_resubmission(self):
        form = address_payload(parameters(), {"city": "Boston", "state": "MA", "zipcode": "02108", "address": "1 Test"})
        resume = {"result": successful_result(), "quote_uuid": "saved-id"}
        session = AsyncMock()
        checkpoint = AsyncMock()
        with patch("warehouse.utils.multi_carrier_quote.gateway_config", return_value=("https://example.invalid/rating", {})), \
             patch("warehouse.utils.multi_carrier_quote.aiohttp.ClientSession", return_value=session), \
             patch("warehouse.utils.multi_carrier_quote.wait_for_kakas_quotes", new_callable=AsyncMock) as poll:
            await execute_quote(form, resume=resume, checkpoint=checkpoint, automatic=True)
        session.__aenter__.return_value.post.assert_not_called()
        self.assertEqual(poll.call_args.args[3], "saved-id")
        self.assertEqual(poll.call_args.kwargs["max_seconds"], 90)
        self.assertEqual(checkpoint.await_count, 2)
