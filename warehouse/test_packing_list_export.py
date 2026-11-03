from datetime import date
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from django.http import Http404
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase
from openpyxl import load_workbook

from warehouse.models.packing_list import PackingList
from warehouse.utils.packing_list_export import export_order_packing_list
from warehouse.views.pre_port.order_creation import OrderCreation


class PackingListExportTests(SimpleTestCase):
    def request(self, container="TEST1234567"):
        return RequestFactory().post("/", {
            "step": "export_uploaded_packing_list", "container_number": container,
        })

    def workbook(self, rows):
        with patch("warehouse.utils.packing_list_export.get_object_or_404", return_value=SimpleNamespace(container_number_id=7)), patch("warehouse.utils.packing_list_export.PackingList.objects") as manager:
            manager.filter.return_value.order_by.return_value.iterator.return_value = iter(rows)
            response = export_order_packing_list(self.request())
            manager.filter.assert_called_once_with(container_number_id=7)
            self.assertIn("TEST1234567_packing_list.xlsx", response["Content-Disposition"])
            content = b"".join(response.streaming_content)
            response.close()
        return load_workbook(BytesIO(content)).active

    def test_saved_values_types_and_formula_safety(self):
        sheet = self.workbook([PackingList(
            shipping_mark="=1+1", pcs=2, total_weight_kg=10.5,
            total_weight_lbs=23.15, cbm=0.8, zipcode="00123",
            ref_id="000012345678901234", note="中文备注\n第二行",
            delivery_window_start=date(2026, 11, 3),
        )])
        cells = dict(zip([cell.value for cell in sheet[1]], sheet[2]))
        self.assertEqual(cells["唛头"].data_type, "s")
        self.assertEqual(cells["唛头"].value, "=1+1")
        self.assertEqual(cells["邮编"].value, "00123")
        self.assertEqual(cells["Amazon Reference ID"].value, "000012345678901234")
        self.assertEqual(cells["总箱重kg"].value, 10.5)
        self.assertEqual(cells["单箱重kg"].value, 5.25)
        self.assertEqual(cells["备注"].value, "中文备注\n第二行")
        self.assertEqual(cells["最早派送时间"].value.date(), date(2026, 11, 3))
        self.assertEqual(sheet.freeze_panes, "A2")

    def test_empty_list_and_zero_quantity(self):
        self.assertEqual(self.workbook([]).max_row, 1)
        self.assertEqual(self.workbook([PackingList(pcs=0, total_weight_kg=0)]).max_row, 2)

    def test_missing_or_unknown_order(self):
        self.assertEqual(export_order_packing_list(self.request("")).status_code, 400)
        with patch("warehouse.utils.packing_list_export.get_object_or_404", side_effect=Http404):
            with self.assertRaises(Http404):
                export_order_packing_list(self.request())

    async def test_export_requires_login_and_dispatches_after_authentication(self):
        view = OrderCreation()
        view._user_authenticate = AsyncMock(return_value=False)
        with patch("warehouse.views.pre_port.order_creation.redirect") as redirect, patch("warehouse.views.pre_port.order_creation.export_order_packing_list") as export:
            await view.post(self.request())
            redirect.assert_called_once_with("login")
            export.assert_not_called()
            view._user_authenticate.return_value = True
            await view.post(self.request())
            export.assert_called_once()

    def test_button_is_available_even_after_unpacking(self):
        html = render_to_string("order_management/packing_list_upload_controls.html", {
            "enable_packing_list_export": True, "can_import_packing_list": False,
            "selected_order": SimpleNamespace(container_number=SimpleNamespace(container_number="TEST1234567")),
        })
        self.assertIn('value="export_uploaded_packing_list"', html)
        self.assertIn('value="TEST1234567"', html)
        self.assertNotIn('value="upload_template"', html)
