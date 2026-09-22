from contextlib import ExitStack
from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from asgiref.sync import async_to_sync
from django.test import RequestFactory, SimpleTestCase
from openpyxl import load_workbook

from warehouse.utils.unpacking_notes import append_walmart_height, needs_walmart_height
from warehouse.views.export_file import export_palletization_list_v2


class UnpackingNotesTests(SimpleTestCase):
    def test_destination_matching(self):
        suffixes = ("MEM1s", "ATL3", "ATL2n", "ATL1", "MCO1", "NJ3", "PHL4n",
                    "KY1", "KS1", "W-IND1", "W-IND2", "W-IND3", "W-MCO1")
        for code in suffixes:
            for destination in (code, "location-" + code, "  " + code.lower() + "  "):
                with self.subTest(destination=destination):
                    self.assertTrue(needs_walmart_height(destination))
        for destination in ("Walmart", "Walmart-1234", " walmart-1234 "):
            self.assertTrue(needs_walmart_height(destination))
        for destination in (None, "", "ONT8", "OTHER-Walmart-1234", "ATL3-extra", "MEM1"):
            self.assertFalse(needs_walmart_height(destination))

    def test_note_preservation_and_idempotence(self):
        self.assertEqual(append_walmart_height(None), "80 height")
        self.assertEqual(append_walmart_height("特操"), "特操, 80 height")
        self.assertEqual(append_walmart_height("80 HEIGHT, keep dry"), "80 HEIGHT, keep dry")
        note = append_walmart_height("100 height, keep dry")
        self.assertEqual(append_walmart_height(note), note)

    def export_rows(self, customer_name, rows):
        module = "warehouse.views.export_file"
        queryset = MagicMock()
        for method in ("select_related", "prefetch_related", "filter", "annotate", "values", "order_by"):
            getattr(queryset, method).return_value = queryset
        queryset.__iter__.side_effect = lambda: iter(rows)
        customer = MagicMock()
        customer.filter.return_value.values_list.return_value.first.return_value = customer_name
        order = MagicMock()
        order.filter.return_value.first.return_value = SimpleNamespace(order_type="海运")
        request = RequestFactory().post("/", {"container_number": "TEST1234567", "status": "non_palletized", "warehouse": "NJ"})
        with ExitStack() as stack:
            stack.enter_context(patch(module + ".Customer.objects", customer))
            stack.enter_context(patch(module + ".Order.objects", order))
            stack.enter_context(patch(module + ".PackingList.objects", queryset))
            export = export_palletization_list_v2
            response = async_to_sync(export)(request)
        workbook = load_workbook(BytesIO(response.content))
        return list(workbook.active.values)[2:]

    def cargo(self, destination, note=None):
        return {
            "container_number__container_number": "TEST1234567", "container__container_number": "TEST1234567",
            "destination": destination, "model": destination, "dropshipping_item_model_number": destination,
            "custom_delivery_method": "卡车派送", "note": note, "shipping_marks": "MARK", "fba_ids": "FBA",
            "pcs": 10, "n_pallet": 1, "delivery_type": "public", "shipment_batch_number__load_type": "地板",
            "vessel_eta": datetime(2026, 9, 1, tzinfo=timezone.utc), "retrieval_destination_area": "NJ",
        }

    def test_export_matches_each_destination_independently_of_customer(self):
        for customer in ("OTHER", "Walmart", "JINYU"):
            with self.subTest(customer=customer):
                matched = self.cargo("Walmart-1234", "keep dry")
                matched["dropshipping_item_model_number"] = "NOT-WALMART"
                unmatched = self.cargo("ONT8", "leave unchanged")
                unmatched["dropshipping_item_model_number"] = "Walmart-1234"
                rows = self.export_rows(customer, [matched, unmatched, self.cargo("location-ATL2n")])
                self.assertEqual(rows[0][3], "keep dry, 80 height")
                self.assertEqual(rows[1][3], "leave unchanged")
                self.assertEqual(rows[2][3], "80 height")

    def test_existing_height_and_empty_export(self):
        rows = self.export_rows("OTHER", [self.cargo("W-IND1", "80 height"), self.cargo("LAX9")])
        self.assertEqual(rows[0][3], "80 height")
        self.assertEqual(rows[1][3], "100 height")
        self.assertEqual(self.export_rows("OTHER", []), [])
