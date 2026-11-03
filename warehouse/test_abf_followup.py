import copy
import json
import os
from unittest.mock import patch

import requests
from django.contrib.auth.models import User, AnonymousUser
from django.http import Http404
from django.test import SimpleTestCase, TransactionTestCase, RequestFactory

from warehouse.models.multi_carrier_quote_history import MultiCarrierQuoteHistory
from warehouse.utils.abf_followup import (
    ABFQueryError, DOCUMENT_ENDPOINT, TRACKING_ENDPOINT, document_params, parse_documents,
    parse_tracking, reference_params, retrieve_documents, track_shipment,
)
from warehouse.views.post_port.abf_followup import abf_followup_post


TRACKING = '''<ABF><SHIPMENTS><SHIPMENT><PRONUMBER>011222333</PRONUMBER>
<PICKUP>11/03/2026</PICKUP><PICKUPTIME>10:30</PICKUPTIME><SHORTSTATUS2>IT</SHORTSTATUS2>
<SHORTSTATUS>obsolete</SHORTSTATUS><LONGSTATUS>In transit &amp; moving</LONGSTATUS>
<EXPECTEDDELIVERYDATE>11/05/2026</EXPECTEDDELIVERYDATE><OSDEXCEPTION>OS</OSDEXCEPTION>
<EXCEPTIONDESCRIPTION>Check pieces</EXCEPTIONDESCRIPTION><LATESTTTECODE>D</LATESTTTECODE>
<LATESTTTEDESCRIPTION>Weather</LATESTTTEDESCRIPTION><CONSIGNEEname>Receiver</CONSIGNEEname>
<BOLNUMBERS><BOLNUMBER>ZEM1</BOLNUMBER></BOLNUMBERS><PONUMBERS><PONUMBER>PO1</PONUMBER></PONUMBERS>
<CARRIERPROS><CARRIERPRO><PRO>123</PRO><CARRIER>Partner</CARRIER></CARRIERPRO></CARRIERPROS>
</SHIPMENT><SHIPMENT><PRONUMBER>011222334</PRONUMBER><SHORTSTATUS2>DL</SHORTSTATUS2>
<DELIVERYDATE>11/04/2026</DELIVERYDATE><DELIVSIGFIRSTNAME>Jane</DELIVSIGFIRSTNAME>
<DELIVSIGLASTNAME>Smith</DELIVSIGLASTNAME></SHIPMENT></SHIPMENTS><NUMERRORS>0</NUMERRORS></ABF>'''

DOCUMENTS = '''<ABF><DOCUMENTS><DOCUMENT><TYPE>BOL</TYPE><PAGES>
<PAGE>https://www.abfs.com/images/one.jpg</PAGE><PAGE>/images/two.jpg</PAGE>
</PAGES></DOCUMENT><DOCUMENT><TYPE>DR</TYPE><PAGES><PAGE>https://arcb.com/images/pod.tif</PAGE>
</PAGES></DOCUMENT></DOCUMENTS><NUMERRORS>0</NUMERRORS></ABF>'''


class FollowupAdapterTests(SimpleTestCase):
    def test_multiple_shipments_current_status_and_unconverted_times(self):
        result = parse_tracking(TRACKING)
        self.assertEqual(len(result["shipments"]), 2)
        first, second = result["shipments"]
        self.assertEqual(first["status_code"], "IT")
        self.assertEqual(first["status"], "In transit & moving")
        self.assertEqual(first["pickup_time"], "10:30")
        self.assertEqual(first["pickup_date"], "11/03/2026")
        self.assertEqual(first["bol_numbers"], ["ZEM1"])
        self.assertEqual(first["carrier_pros"][0]["carrier"], "Partner")
        self.assertEqual(first["consignee_name"], "Receiver")
        self.assertEqual(first["transit_exception_description"], "Weather")
        self.assertEqual(second["signature_first_name"], "Jane")

    def test_multi_page_documents_and_safe_urls(self):
        result = parse_documents(DOCUMENTS)
        self.assertEqual(len(result["documents"]), 2)
        pages = result["documents"][0]["pages"]
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[1], {"number": 2, "url": "https://www.abfs.com/images/two.jpg"})
        for url in ("javascript:alert(1)", "https://www.abfs.com.evil.example/file", "//evil.example/file"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                parse_documents(DOCUMENTS.replace('https://www.abfs.com/images/one.jpg', url))

    def test_grouped_errors_keep_all_codes_and_messages(self):
        xml = '<ABF><NUMERRORS>2</NUMERRORS><ERRORS><ERRORCODE>7</ERRORCODE><ERRORMESSAGE>No access</ERRORMESSAGE><ERRORCODE>9</ERRORCODE><ERRORMESSAGE>Not scanned</ERRORMESSAGE></ERRORS></ABF>'
        with self.assertRaises(ABFQueryError) as caught:
            parse_documents(xml)
        self.assertEqual([error["code"] for error in caught.exception.errors], ["7", "9"])
        self.assertIn("No access", str(caught.exception))
        with self.assertRaisesRegex(ABFQueryError, "9: No PRO"):
            parse_tracking('<ABF><NUMERRORS>1</NUMERRORS><ERRORS><ERRORCODE>9</ERRORCODE><ERRORMESSAGE>No PRO</ERRORMESSAGE></ERRORS></ABF>')

    def test_reference_and_document_rules_from_spec(self):
        self.assertEqual(reference_params("k", "WWW1234567")["RefType"], "K")
        for args in (("K", "WWW1234567", "ALL"), ("B", "ZEM1", "INV"), ("A", "011222333", "invalid")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                document_params(*args)
        self.assertEqual(document_params("A", "011222333", "STR", "T")["DocType"], "Str")
        self.assertEqual(document_params("A", "011222333", "INV")["DocType"], "INV")
        for number in ("", "WWW0000000", "a\nvalue"):
            with self.assertRaises(ValueError):
                reference_params("A", number)

    def test_malformed_xml_not_silently_shown_as_successful_empty_result(self):
        for xml in ("<html/>", "not XML", '<ABF><SHIPMENTS/></ABF>', '<ABF><NUMERRORS>0</NUMERRORS></ABF>', '<!DOCTYPE ABF><ABF/>'):
            with self.subTest(xml=xml), self.assertRaises(ValueError):
                parse_tracking(xml)
        self.assertEqual(parse_tracking('<ABF><NUMERRORS>0</NUMERRORS><SHIPMENTS/></ABF>')["shipments"], [])

    @patch("warehouse.utils.abf_followup.requests.post")
    def test_both_queries_use_api_key_and_never_booking_mode(self, post):
        post.return_value.status_code = 200
        for mode in (None, "test", "live", "invalid-booking-mode"):
            env = {"ABF_API_KEY": "followup-test-key"}
            if mode:
                env["ABF_BOOKING_MODE"] = mode
            with self.subTest(mode=mode), patch.dict(os.environ, env, clear=True):
                post.return_value.text = TRACKING
                track_shipment("K", "WWW1234567")
                self.assertEqual(post.call_args.args[0], TRACKING_ENDPOINT)
                params = post.call_args.kwargs["data"]
                self.assertEqual(params["ID"], "followup-test-key")
                self.assertNotIn("Test", params)
                self.assertEqual(params["RefType"], "K")
                post.return_value.text = DOCUMENTS
                retrieve_documents("A", "011222333", "DR", "H")
                self.assertEqual(post.call_args.args[0], DOCUMENT_ENDPOINT)
                self.assertEqual(post.call_args.kwargs["data"]["DocType"], "DR")
                self.assertEqual(post.call_args.kwargs["data"]["ImageType"], "H")
                self.assertFalse(post.call_args.kwargs["allow_redirects"])

    @patch("warehouse.utils.abf_followup.requests.post")
    def test_connection_errors_and_missing_key_are_clear(self, post):
        with patch.dict(os.environ, {"ABF_API_KEY": ""}), self.assertRaisesRegex(ValueError, "ABF_API_KEY"):
            track_shipment("A", "011222333")
        post.assert_not_called()
        post.side_effect = requests.Timeout("test")
        with patch.dict(os.environ, {"ABF_API_KEY": "key"}), self.assertRaises(ConnectionError):
            track_shipment("A", "011222333")


class FollowupViewTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("followup-user")
        self.history = MultiCarrierQuoteHistory.objects.create(origin_warehouse="NJ", destination_warehouse="TEST",
            pickup_date="2026-11-03", quote_type="LTL", declared_value=100, operator=self.user,
            abf_quotes={"status": "success", "booking": {"status": "success", "mode": "live",
                "confirmation": "WWW1234567", "pro_number": "011222333", "bol_number": "ZEM1"}})

    def request(self, step, linked=True, **fields):
        data = {"step": step, "ref_type": "A", "ref_num": "011222333", **fields}
        if linked:
            data["history_id"] = self.history.pk
        request = RequestFactory().post('/post_nsop/', data)
        request.user = self.user
        return request

    def test_preview_read_only_and_test_references_not_used(self):
        before = copy.deepcopy(self.history.abf_quotes)
        response = abf_followup_post(self.request("abf_shipment_preview"))
        self.assertEqual(json.loads(response.content)["references"][0], {"type": "A", "number": "011222333"})
        self.history.refresh_from_db()
        self.assertEqual(self.history.abf_quotes, before)
        self.history.abf_quotes["booking"].update(status="test_success", mode="test", confirmation="WWW0000000")
        self.history.save()
        response = abf_followup_post(self.request("abf_shipment_preview"))
        self.assertEqual(json.loads(response.content)["references"], [])

    @patch("warehouse.views.post_port.abf_followup.track_shipment", return_value=parse_tracking(TRACKING))
    @patch("warehouse.views.post_port.abf_followup.retrieve_documents", return_value=parse_documents(DOCUMENTS))
    def test_queries_merge_snapshots_without_overwriting_booking(self, documents, tracking):
        before = copy.deepcopy(self.history.abf_quotes["booking"])
        self.assertEqual(abf_followup_post(self.request("abf_tracking")).status_code, 200)
        self.assertEqual(abf_followup_post(self.request("abf_documents", doc_type="ALL")).status_code, 200)
        self.history.refresh_from_db()
        self.assertEqual(self.history.abf_quotes["booking"], before)
        self.assertIn("tracking_latest", self.history.abf_quotes)
        self.assertIn("documents_latest", self.history.abf_quotes)
        self.assertEqual(self.history.abf_quotes["documents_latest"]["ref_num"], "011222333")

    @patch("warehouse.views.post_port.abf_followup.track_shipment", return_value=parse_tracking(TRACKING))
    def test_standalone_lookup_works_without_booking_or_history(self, tracking):
        response = abf_followup_post(self.request("abf_tracking", linked=False))
        self.assertEqual(response.status_code, 200)
        self.history.refresh_from_db()
        self.assertNotIn("tracking_latest", self.history.abf_quotes)
        tracking.assert_called_once_with("A", "011222333")

    @patch("warehouse.views.post_port.abf_followup.track_shipment")
    def test_authentication_and_history_ownership(self, tracking):
        request = self.request("abf_tracking")
        request.user = AnonymousUser()
        self.assertEqual(abf_followup_post(request).status_code, 403)
        request.user = User.objects.create_user("stranger")
        with self.assertRaises(Http404):
            abf_followup_post(request)
        tracking.assert_not_called()

    @patch("warehouse.views.post_port.abf_followup.track_shipment", side_effect=ABFQueryError("物流查询", [{"code": "9", "message": "No PRO yet"}]))
    def test_not_billed_is_reported_without_creating_a_booking(self, tracking):
        response = abf_followup_post(self.request("abf_tracking"))
        self.assertEqual(response.status_code, 400)
        self.assertIn("9: No PRO yet", json.loads(response.content)["message"])
        self.history.refresh_from_db()
        self.assertNotIn("tracking_latest", self.history.abf_quotes)
        self.assertEqual(self.history.abf_quotes["booking"]["status"], "success")
