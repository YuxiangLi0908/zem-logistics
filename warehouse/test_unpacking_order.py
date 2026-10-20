from contextlib import ExitStack
from io import BytesIO
from unittest.mock import MagicMock, patch

from asgiref.sync import async_to_sync
from django.test import RequestFactory, SimpleTestCase
from openpyxl import load_workbook

from warehouse.views.export_file import export_palletization_list
from warehouse.views.post_port.warehouse.palletization import Palletization


class UnpackingOrderTests(SimpleTestCase):
    def queryset(self, rows):
        qs = MagicMock()
        for method in ("select_related", "filter", "annotate", "values", "order_by"):
            getattr(qs, method).return_value = qs
        qs.__iter__.side_effect = lambda: iter(rows)
        return qs

    def test_export_destination_then_method_then_unrounded_volume(self):
        rows = []
        for destination, method, volume, identity in (
            ("ZZZ", "truck", 99, "large"),
            ("AAA", "truck", 1.231, "small"),
            ("AAA", "truck", 1.234, "larger"),
            ("AAA", "pickup", 0.5, "pickup"),
        ):
            rows.append(dict(container_number__container_number="TEST", destination=destination,
                             custom_delivery_method=method, raw_cbm=volume, cbm=round(volume, 2),
                             fba_ids=identity, ref_ids="", shipping_marks="", pcs=1,
                             n_pallet=0, PO_ID=identity, note=""))
        for warehouse, customer, expected in (
            ("NJ-07001", "OTHER", ["pickup", "larger", "small", "large"]),
            ("NJ-07001", "JINYU", ["large", "larger", "small", "pickup"]),
            ("SAV", "OTHER", ["large", "small", "larger", "pickup"]),
        ):
            for row in rows:
                row["dropshipping_item_model_number"] = row["destination"]
            qs = self.queryset(rows)
            customers = MagicMock()
            customers.filter.return_value.values_list.return_value.first.return_value = customer
            request = RequestFactory().post("/", dict(status="non_palletized", warehouse=warehouse, container_number="TEST"))
            with patch("warehouse.views.export_file.PackingList.objects", qs), patch("warehouse.views.export_file.Customer.objects", customers):
                response = async_to_sync(export_palletization_list)(request)
            values = list(load_workbook(BytesIO(response.content)).active.values)
            index = values[0].index("fba_id")
            self.assertEqual([row[index] for row in values[1:]], expected)

    def test_entry_page_query_order_and_other_warehouse_unchanged(self):
        for status, delivery in (("non_palletized", "custom_delivery_method"), ("palletized", "delivery_method")):
            for warehouse, customer, expected in (
                ("NJ", "OTHER", ("destination", delivery, "-cbm")),
                ("NJ", "JINYU", ("-cbm",)),
                ("SAV", "OTHER", ("-cbm",)),
            ):
                qs = self.queryset([])
                with ExitStack() as stack:
                    for model in ("PackingList", "Pallet"):
                        stack.enter_context(patch("warehouse.views.post_port.warehouse.palletization." + model + ".objects", qs))
                    async_to_sync(Palletization()._get_packing_list)("TEST", status, warehouse, customer)
                qs.order_by.assert_called_once_with(*expected)
