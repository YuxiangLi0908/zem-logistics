import copy
import json
import os
from datetime import timedelta
from unittest.mock import patch

import requests
from django.contrib.auth.models import User
from django.http import Http404
from django.test import TransactionTestCase, SimpleTestCase, RequestFactory
from django.utils import timezone

from warehouse.models.multi_carrier_quote_history import MultiCarrierQuoteHistory
from warehouse.test_abf_quote import form
from warehouse.utils.abf_booking import ABFRejected, prepare_booking, parse_booking_response, submit_booking
from warehouse.utils.multi_carrier_quote import prepare_quote
from warehouse.views.post_port.abf_booking import abf_booking_post


def source():
    data = form()
    data.update(originDetailAddress="1 Test Street", destinationDetailAddress="2 Test Street",
                pickupDate=(timezone.now().date() + timedelta(days=2)).isoformat())
    _, classes = prepare_quote(data)
    return {"form": data, "classes": classes}


def fields():
    return {"RequesterName": "Tester", "RequesterEmail": "test@example.com", "RequesterPhone": "(929) 555-1234",
            "ShipName": "ZEM", "ShipContact": "Origin", "ShipPhone": "9295551234",
            "ConsName": "Receiver", "ConsContact": "Destination", "ConsPhone": "9295555678",
            "OT": "09:00", "AT": "10:30", "CT": "17:00"}


XML = '<ABF><NUMERRORS>0</NUMERRORS><CONFIRMATION>WWW1234567</CONFIRMATION><DOCUMENT>https://www.abfs.com/test.pdf</DOCUMENT><PRONUMBER>123456789</PRONUMBER><BOLNUMBER>ZEMTEST</BOLNUMBER></ABF>'


class BookingAdapterTests(SimpleTestCase):
    def test_spec_mapping_and_quotation_link(self):
        with patch.dict(os.environ, {"ABF_BOOKING_MODE": "test", "ABF_PAYING_PARTY": "shipper"}):
            params, mode = prepare_booking(source(), "LP3CGP3026", fields(), 7)
        self.assertEqual(params["QuoteID"], "LP3CGP3026")
        self.assertEqual(params["RequesterType"], "1")
        self.assertEqual(params["PayTerms"], "P")
        self.assertEqual(params["WT1"], "1500")
        self.assertEqual(params["HN1"], "3")
        self.assertEqual(params["HT1"], "PLT")
        self.assertEqual(params["ShipPhone"], "9295551234")
        self.assertEqual(params["Bol"], "ZEMABF7")
        self.assertEqual(params["Test"], "Y")
        self.assertEqual(mode, "test")
        self.assertNotIn("ID", params)

    def test_invalid_contacts_time_or_dimensions_rejected_before_request(self):
        for changes in ({"ShipPhone": "123"}, {"AT": "10:15"}, {"CT": "09:00"}, {"RequesterEmail": "bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                prepare_booking(source(), "Q1", dict(fields(), **changes), 1)
        data = source()
        data["form"]["items"][0]["width"] = 100
        with self.assertRaises(ValueError):
            prepare_booking(data, "Q1", fields(), 1)

    def test_xml_success_test_errors_and_document_hosts(self):
        result = parse_booking_response(XML, "live")
        self.assertEqual(result["confirmation"], "WWW1234567")
        self.assertEqual(result["document"], "https://www.abfs.com/test.pdf")
        self.assertEqual(parse_booking_response(XML.replace('https://www.abfs.com/test.pdf', 'javascript:alert(1)'), "live")["document"], "")
        with self.assertRaises(ValueError):
            parse_booking_response(XML.replace("WWW1234567", "WWW0000000"), "live")
        with self.assertRaises(ValueError):
            parse_booking_response(XML.replace("<NUMERRORS>0</NUMERRORS>", ""), "live")
        with self.assertRaisesRegex(ABFRejected, "22: Bad requester"):
            parse_booking_response('<ABF><NUMERRORS>1</NUMERRORS><ERROR><ERRORCODE>22</ERRORCODE><ERRORMESSAGE>Bad requester</ERRORMESSAGE></ERROR></ABF>', "live")

    @patch("warehouse.utils.abf_booking.requests.post")
    def test_network_call_is_single_post_and_secret_is_not_saved(self, post):
        post.return_value.status_code = 200
        post.return_value.text = XML
        with patch.dict(os.environ, {"ABF_API_KEY": "test-secret-key"}):
            result = submit_booking({"QuoteID": "Q1"}, "live")
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["data"]["ID"], "test-secret-key")
        self.assertFalse(post.call_args.kwargs["allow_redirects"])
        self.assertNotIn("test-secret-key", str(result))


class BookingWorkflowTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("booking-user")
        snapshot = source()
        self.history = MultiCarrierQuoteHistory.objects.create(
            origin_warehouse="NJ", destination_warehouse="TEST", pickup_date=snapshot["form"]["pickupDate"],
            quote_type="LTL", declared_value=100, pallet_items=snapshot["form"]["items"], operator=self.user,
            abf_quotes={"status": "success", "booking_source": snapshot,
                        "data": {"quotes": [{"quoteId": "Q1", "TotalQuote": "123.45"}]}})
        self.env = patch.dict(os.environ, {"ABF_API_KEY": "test-secret-key", "ABF_BOOKING_MODE": "live", "ABF_PAYING_PARTY": "shipper"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def request(self, step="abf_booking_submit"):
        request = RequestFactory().post('/post_nsop/', {"step": step, "history_id": self.history.pk,
            "booking_fields": json.dumps(fields()), "confirmed": "yes"})
        request.user = self.user
        return request

    @patch("warehouse.views.post_port.abf_booking.submit_booking", return_value={"confirmation": "WWW1234567", "document": "https://www.abfs.com/test.pdf"})
    def test_success_is_durable_and_duplicate_click_does_not_book_again(self, submit):
        first = abf_booking_post(self.request())
        second = abf_booking_post(self.request())
        self.assertEqual(first.status_code, 200)
        self.assertEqual(json.loads(second.content)["booking"]["status"], "success")
        submit.assert_called_once()
        self.assertEqual(submit.call_args.args[0]["QuoteID"], "Q1")
        self.assertNotIn("test-secret-key", first.content.decode())
        self.assertNotIn("request", json.loads(second.content)["booking"])

    @patch("warehouse.views.post_port.abf_booking.submit_booking", side_effect=requests.Timeout("timed out"))
    def test_timeout_is_unknown_and_cannot_be_retried(self, submit):
        first = abf_booking_post(self.request())
        self.assertEqual(json.loads(first.content)["booking"]["status"], "unknown")
        abf_booking_post(self.request())
        submit.assert_called_once()

    @patch("warehouse.views.post_port.abf_booking.submit_booking", side_effect=ABFRejected("ABF 22: Bad requester"))
    def test_explicit_rejection_shows_reason_and_can_be_corrected(self, submit):
        result = json.loads(abf_booking_post(self.request()).content)
        self.assertEqual(result["booking"]["status"], "rejected")
        self.assertIn("22", result["booking"]["message"])
        abf_booking_post(self.request())
        self.assertEqual(submit.call_count, 2)

    @patch("warehouse.views.post_port.abf_booking.submit_booking")
    def test_cannot_book_another_users_history(self, submit):
        request = self.request()
        request.user = User.objects.create_user("stranger")
        with self.assertRaises(Http404):
            abf_booking_post(request)
        submit.assert_not_called()

    @patch("warehouse.views.post_port.abf_booking.submit_booking")
    def test_preview_is_read_only_and_invalid_time_does_not_reserve(self, submit):
        before = copy.deepcopy(self.history.abf_quotes)
        preview = abf_booking_post(self.request("abf_booking_preview"))
        self.assertEqual(preview.status_code, 200)
        request = self.request()
        request.POST = request.POST.copy()
        request.POST["booking_fields"] = json.dumps(dict(fields(), AT="10:15"))
        self.assertEqual(abf_booking_post(request).status_code, 400)
        self.history.refresh_from_db()
        self.assertEqual(self.history.abf_quotes, before)
        submit.assert_not_called()
