import ast
import copy
import json
import os
from datetime import datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from asgiref.sync import async_to_sync, sync_to_async
from django.contrib.auth.models import User
from django.http import JsonResponse
from django.test import RequestFactory, TransactionTestCase

from warehouse.models.multi_carrier_quote_history import MultiCarrierQuoteHistory
from warehouse.test_abf_quote import form
from warehouse.utils.multi_carrier_quote import execute_quote, prepare_quote
from warehouse.utils.p1_quote import parse_p1_response, prepare_p1_quote, request_p1_quote


def response_data():
    return {"id": 123456, "rateQuotes": [
        {"id": 21, "carrierName": "Estes", "carrierCode": "EXLA", "serviceLevel": "STANDARD",
         "transitDays": 4, "carrierQuoteNumber": "E123", "expirationDate": "2026-11-19T00:00:00",
         "rateQuoteDetail": {"total": 235.89, "charges": [{"code": "FSC", "amount": 39.46}]}},
        {"id": 22, "carrierName": "TForce", "carrierCode": "UPGF", "serviceLevel": "STANDARD",
         "transitDays": 2, "rateQuoteDetail": {"total": 200, "charges": []}},
    ], "invalidRateQuotes": [
        {"carrierCode": "ODFL", "carrierName": "Old Dominion", "errorMessages": [
            {"severity": "Error", "text": "No service for this lane", "source": "CAPACITY_PROVIDER"}]}],
        "messages": [{"severity": "Warning", "text": "Origin city normalized"}]}


def mock_session(data=None, status=200, raw=None):
    session = MagicMock()
    response = AsyncMock()
    response.status = status
    response.text.return_value = raw if raw is not None else json.dumps(data)
    session.post.return_value.__aenter__ = AsyncMock(return_value=response)
    session.post.return_value.__aexit__ = AsyncMock(return_value=False)
    return session


class P1MappingTests(TestCase):
    def test_shared_class_per_pallet_units_and_total_weight(self):
        data = form()
        data["items"][0].update(pieces=7, weight=500.1, length=48.1, freightClass="999")
        gateway, classes = prepare_quote(data)
        payload = prepare_p1_quote(data, classes)
        item = payload["items"][0]
        self.assertEqual(item["freightClass"], gateway["carrierPayloads"]["kakas"]["commodityList"][0]["freightClass"])
        self.assertNotEqual(item["freightClass"], "999")
        self.assertEqual((item["units"], item["pieces"], item["totalWeight"]), (3, 7, 1503))
        self.assertEqual((item["length"], item["width"], item["height"]), (49, 40, 60))
        self.assertEqual(item["packagingType"], "Pallet")
        self.assertEqual(payload["originZipCode"], "07001")
        self.assertEqual(payload["destinationZipCode"], "02108")
        self.assertEqual(payload["pickupDate"], "2026-11-01T00:00:00")
        self.assertEqual(payload["accessorialServices"], [{"code": "RESDEL"}, {"code": "LGPU"}, {"code": "LGDEL"}])
        self.assertNotIn("declaredValue", payload)
        self.assertNotIn("enhancedHandlingUnits", payload)

    def test_multiple_items_crates_and_no_unrequested_accessorials(self):
        data = form()
        data.update(palletType=2, commodityUnit=2, needLiftgate=False, destinationType=3)
        data["items"].append(dict(data["items"][0], palletCount=2, weight=100))
        _, classes = prepare_quote(data)
        payload = prepare_p1_quote(data, classes)
        self.assertEqual(len(payload["items"]), 2)
        self.assertEqual(payload["items"][1]["totalWeight"], 200)
        self.assertEqual(payload["items"][1]["packagingType"], "Crate")
        self.assertEqual(payload["accessorialServices"], [])

    def test_invalid_units_or_dimensions_fail_with_row_and_field(self):
        for field, value in (("weight", 0), ("length", float("nan")), ("palletCount", 1.5), ("pieces", -1)):
            with self.subTest(field=field):
                data = form()
                data["items"][0][field] = value
                with self.assertRaisesRegex(ValueError, field):
                    prepare_p1_quote(data, ["85"])

    def test_multiple_quotes_sorted_without_adding_charges_twice(self):
        result = parse_p1_response(response_data())
        self.assertEqual(result["status"], "success")
        rates = result["data"]["quotes"]
        self.assertEqual([r["carrierName"] for r in rates], ["TForce", "Estes"])
        self.assertEqual(rates[1]["TotalQuote"], "235.89")
        self.assertEqual(rates[1]["transitDays"], 4)
        self.assertEqual(rates[1]["carrierQuoteNumber"], "E123")
        self.assertEqual(result["data"]["quoteRequestId"], 123456)
        self.assertIn("Old Dominion", result["warning"])
        self.assertIn("No service for this lane", result["warning"])
        self.assertIn("Origin city normalized", result["warning"])

    def test_no_usable_rates_reports_the_carriers_error(self):
        data = response_data()
        data["rateQuotes"] = None
        with self.assertRaisesRegex(ValueError, "Old Dominion.*No service for this lane"):
            parse_p1_response(data)
        for amount in (None, "NaN", "Infinity", -1):
            data = response_data()
            data["rateQuotes"] = [dict(data["rateQuotes"][0], rateQuoteDetail={"total": amount})]
            with self.subTest(amount=amount), self.assertRaisesRegex(ValueError, "rateQuoteDetail.total"):
                parse_p1_response(data)
        data["rateQuotes"][0]["rateQuoteDetail"]["total"] = 0
        self.assertEqual(parse_p1_response(data)["data"]["quotes"][0]["TotalQuote"], "0")


class P1RequestTests(IsolatedAsyncioTestCase):
    async def test_official_post_header_only_key_and_redacted_echo(self):
        data = response_data()
        data["messages"] = [{"text": "Echo p1-secret-test"}]
        session = mock_session(data)
        with patch.dict(os.environ, {"P1_API_KEY": "p1-secret-test"}, clear=True):
            result, payload = await request_p1_quote(session, form(), ["85"])
        call = session.post.call_args
        self.assertEqual(call.args, ("https://api.priority1.com/v2/ltl/quotes/rates",))
        self.assertEqual(call.kwargs["headers"]["X-API-KEY"], "p1-secret-test")
        self.assertFalse(call.kwargs["allow_redirects"])
        self.assertNotIn("p1-secret-test", str(payload) + str(result))
        self.assertEqual(call.kwargs["json"], payload)

    async def test_missing_key_and_invalid_base_do_not_send(self):
        for env in ({}, {"P1_API_KEY": "test", "P1_API_BASE_URL": "https://other.example"}):
            session = mock_session()
            with patch.dict(os.environ, env, clear=True):
                result, _ = await request_p1_quote(session, form(), ["85"])
            self.assertEqual(result["status"], "error")
            session.post.assert_not_called()

    async def test_http_errors_preserve_invalid_field_and_reason(self):
        for status, data in ((400, {"errors": {"items[0].totalWeight": ["Must be greater than zero"]}}),
                             (401, {"text": "Invalid API key"}), (403, {"text": "Rating denied"}),
                             (429, {"text": "Rate limit exceeded"}), (500, [{"text": "Carrier unavailable"}])):
            session = mock_session(data, status)
            with self.subTest(status=status), patch.dict(os.environ, {"P1_API_KEY": "test-key"}, clear=True):
                result, _ = await request_p1_quote(session, form(), ["85"])
            self.assertEqual(result["status"], "error")
            self.assertIn(str(status), result["error"])
            if status == 400:
                self.assertIn("items[0].totalWeight", result["error"])
                self.assertIn("Must be greater than zero", result["error"])

    async def test_non_json_errors_and_network_failure(self):
        for status in (200, 401):
            with patch.dict(os.environ, {"P1_API_KEY": "test-key"}, clear=True):
                result, _ = await request_p1_quote(mock_session(status=status, raw="<html>Error</html>"), form(), ["85"])
            self.assertIn(str(status), result["error"])
        session = MagicMock()
        session.post.side_effect = aiohttp.ClientConnectionError("secret transport detail")
        with patch.dict(os.environ, {"P1_API_KEY": "test-key"}, clear=True):
            result, _ = await request_p1_quote(session, form(), ["85"])
        self.assertEqual(result["status"], "error")
        self.assertNotIn("secret transport detail", result["error"])

    async def test_ftl_is_not_sent_to_ltl_endpoint(self):
        session = MagicMock()
        result, _ = await request_p1_quote(session, dict(form(), quoteType=2), ["85"])
        self.assertEqual(result["status"], "skipped")
        session.post.assert_not_called()

    async def test_manual_integration_keeps_other_platforms_when_p1_fails(self):
        session = mock_session()
        gateway = {"results": {"maersk": {"status": "success", "data": {"quotes": []}},
                               "kakas": {"status": "error", "error": "No rates"}}}
        session.post.return_value.__aenter__.return_value.json.return_value = gateway
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=session)
        client.__aexit__ = AsyncMock(return_value=False)
        with patch("warehouse.utils.multi_carrier_quote.gateway_config", return_value=("https://example.invalid", {})), \
             patch("warehouse.utils.multi_carrier_quote.aiohttp.ClientSession", return_value=client), \
             patch("warehouse.utils.multi_carrier_quote.request_abf_quote", AsyncMock(return_value=({"status": "success"}, {}))), \
             patch("warehouse.utils.multi_carrier_quote.request_p1_quote", AsyncMock(return_value=({"status": "error", "error": "P1 401"}, {}))) as p1:
            result, _ = await execute_quote(form())
            self.assertEqual(result["results"]["p1"]["error"], "P1 401")
            self.assertEqual(result["results"]["maersk"]["status"], "success")
            self.assertEqual(result["results"]["abf"]["status"], "success")
            p1.assert_awaited_once()

    async def test_fresh_automatic_quote_does_not_call_p1(self):
        session = mock_session()
        gateway = {"results": {"maersk": {"status": "success"}, "kakas": {"status": "error"}}}
        session.post.return_value.__aenter__.return_value.json.return_value = gateway
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=session)
        client.__aexit__ = AsyncMock(return_value=False)
        with patch("warehouse.utils.multi_carrier_quote.gateway_config", return_value=("https://example.invalid", {})), \
             patch("warehouse.utils.multi_carrier_quote.aiohttp.ClientSession", return_value=client), \
             patch("warehouse.utils.multi_carrier_quote.request_abf_quote", AsyncMock(return_value=({"status": "success"}, {}))), \
             patch("warehouse.utils.multi_carrier_quote.request_p1_quote", AsyncMock()) as p1:
            result, payload = await execute_quote(form(), automatic=True)
        p1.assert_not_awaited()
        self.assertNotIn("p1", result["results"])
        self.assertNotIn("p1", payload["carrierPayloads"])

    async def test_automatic_resume_does_not_call_p1(self):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=MagicMock())
        client.__aexit__ = AsyncMock(return_value=False)
        with patch("warehouse.utils.multi_carrier_quote.gateway_config", return_value=("https://example.invalid", {})), \
             patch("warehouse.utils.multi_carrier_quote.aiohttp.ClientSession", return_value=client), \
             patch("warehouse.utils.multi_carrier_quote.request_p1_quote", AsyncMock()) as p1:
            await execute_quote(form(), automatic=True, resume={"result": {"results": {"abf": {"status": "success"}}}})
        p1.assert_not_awaited()


class P1HistoryTests(TransactionTestCase):
    def test_real_manual_handler_saves_p1_and_existing_booking_snapshot(self):
        # Load the actual handler without importing unrelated views that query on import.
        path = Path(__file__).parent / "views" / "post_port" / "post_nsop.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        view = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "PostNsop")
        handler = next(node for node in view.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_get_multi_carrier_quote")
        handler = copy.deepcopy(handler)
        handler.returns = None
        for arg in handler.args.args:
            arg.annotation = None
        namespace = dict(json=json, datetime=datetime, sync_to_async=sync_to_async,
                         MultiCarrierQuoteHistory=MultiCarrierQuoteHistory, JsonResponse=JsonResponse)
        exec(compile(ast.Module(body=[handler], type_ignores=[]), str(path), "exec"), namespace)
        user = User.objects.create_user("p1-history-user")
        request = RequestFactory().post("/post_nsop/", {"quote_payload": json.dumps(form())})
        request.user = user
        result = {"results": {"p1": parse_p1_response(response_data()), "abf": {"status": "success"}}, "freightClasses": ["85"]}
        with patch("warehouse.utils.multi_carrier_quote.execute_quote", AsyncMock(return_value=(result, {"carrierPayloads": {"kakas": {}}}))):
            response = async_to_sync(namespace[handler.name])(None, request)
        self.assertEqual(response.status_code, 200, response.content)
        history = MultiCarrierQuoteHistory.objects.get(pk=json.loads(response.content)["data"]["history_id"])
        self.assertEqual(history.p1_quotes["data"]["quoteRequestId"], 123456)
        self.assertEqual(len(history.p1_quotes["data"]["quotes"]), 2)
        self.assertEqual(history.abf_quotes["booking_source"]["form"]["originPostCode"], "07001")
