import os
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from warehouse.utils.abf_quote import prepare_abf_quote, parse_abf_response, request_abf_quote
from warehouse.utils.multi_carrier_quote import prepare_quote
from warehouse.utils.quote_identity_v1 import normalize_prices


def form():
    return {"originWarehouse": "NJ", "destinationWarehouse": "TEST", "pickupDate": "2026-11-01",
            "originCity": "Avenel", "originState": "NJ", "originPostCode": "07001",
            "destinationCity": "Boston", "destinationState": "MA", "destinationPostCode": "02108",
            "declaredValue": "100", "needLiftgate": True, "destinationType": 2,
            "items": [{"length": 48, "width": 40, "height": 60, "weight": 500, "palletCount": 3}]}


XML = '''<ABF><NUMERRORS>0</NUMERRORS><QUOTEID>TEST-Q1</QUOTEID><CHARGE>123.45</CHARGE>
<ADVERTISEDTRANSIT>2</ADVERTISEDTRANSIT><ITEMIZEDCHARGES>
<ITEM TYPE="CHARGE" AMOUNT="150"/><ITEM TYPE="DISCOUNT" AMOUNT="-26.55"/>
</ITEMIZEDCHARGES></ABF>'''


class ABFQuoteTests(TestCase):
    def test_mapping_uses_per_pallet_dimensions_total_weight_and_shared_class(self):
        data = form()
        gateway, classes = prepare_quote(data)
        params = prepare_abf_quote(data, classes)
        self.assertEqual(params["Class1"], gateway["carrierPayloads"]["kakas"]["commodityList"][0]["freightClass"])
        self.assertEqual(params["Wgt1"], "1500")
        self.assertEqual(params["UnitNo1"], "3")
        self.assertEqual(params["FrtLng1"], "48")
        self.assertEqual(params["ConsZip"], "02108")
        self.assertEqual(params["ShipAff"], "Y")
        self.assertEqual(params["Acc_RDEL"], "Y")
        self.assertEqual(params["Acc_GRD_DEL"], "Y")
        self.assertNotIn("ID", params)

    def test_unsupported_or_invalid_cargo_is_not_silently_quoted(self):
        data = form()
        data["items"] *= 16
        with self.assertRaises(ValueError):
            prepare_abf_quote(data, ["85"] * 16)
        data = form()
        data["items"][0]["weight"] = 0
        with self.assertRaises(ValueError):
            prepare_abf_quote(data, ["85"])

    def test_xml_amount_already_includes_itemized_charges(self):
        data = parse_abf_response(XML)
        self.assertEqual(data["quotes"][0]["TotalQuote"], "123.45")
        normalized = normalize_prices({"results": {"abf": {"status": "success", "data": data}}})
        self.assertEqual(normalized[0]["platform"], "abf")
        self.assertEqual(normalized[0]["carrier"], "ABF Freight")
        self.assertEqual(str(normalized[0]["price"]), "123.45")

    def test_xml_errors_and_invalid_prices_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "93: Invalid credentials"):
            parse_abf_response('<ABF><NUMERRORS>1</NUMERRORS><ERROR><ERRORCODE>93</ERRORCODE><ERRORMESSAGE>Invalid credentials</ERRORMESSAGE></ERROR></ABF>')
        for raw in ("<html/>", "not XML", XML.replace("123.45", "NaN"), XML.replace("<QUOTEID>TEST-Q1</QUOTEID>", "")):
            with self.assertRaises(ValueError):
                parse_abf_response(raw)


class ABFRequestTests(IsolatedAsyncioTestCase):
    async def test_post_credentials_not_in_saved_request(self):
        session = MagicMock()
        response = AsyncMock()
        response.status = 200
        response.text.return_value = XML
        session.post.return_value.__aenter__ = AsyncMock(return_value=response)
        session.post.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch.dict(os.environ, {"ABF_API_KEY": "secret-test-key"}, clear=True):
            result, payload = await request_abf_quote(session, form(), ["85"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(session.post.call_args.kwargs["data"]["ID"], "secret-test-key")
        self.assertEqual(session.post.call_args.kwargs["data"]["ShipPay"], "Y")
        self.assertNotIn("secret-test-key", str(payload))
        self.assertFalse(session.post.call_args.kwargs["allow_redirects"])

    async def test_configuration_failure_does_not_send_a_request(self):
        session = MagicMock()
        with patch.dict(os.environ, {"ABF_API_KEY": ""}):
            result, _ = await request_abf_quote(session, form(), ["85"])
        self.assertEqual(result["status"], "error")
        self.assertIn("ABF_API_KEY", result["error"])
        session.post.assert_not_called()

    async def test_ftl_not_sent_to_ltl_endpoint(self):
        session = MagicMock()
        data = form()
        data["quoteType"] = 2
        result, _ = await request_abf_quote(session, data, ["85"])
        self.assertEqual(result["status"], "skipped")
        session.post.assert_not_called()
