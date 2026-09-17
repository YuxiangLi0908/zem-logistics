from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import PermissionDenied
from django.core.files.uploadedfile import SimpleUploadedFile
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase
from openpyxl import Workbook

from warehouse.models.container import Container
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.utils.constants import (
    DELIVERY_METHOD_OPTIONS, DROPSHIPPING_DELIVERY_METHOD_OPTIONS,
    DROPSHIPPING_PACKING_LIST_TEMP_COL_MAPPING, PACKING_LIST_TEMP_COL_MAPPING,
)
from warehouse.utils.packing_list_import import can_import_packing_list
from warehouse.views.dropshipping import Dropshipping
from warehouse.views.pre_port.order_creation import OrderCreation


class PackingListImportTests(SimpleTestCase):
    cases = (
        (Dropshipping, "warehouse.views.dropshipping", "dropship_cargo",
         DROPSHIPPING_PACKING_LIST_TEMP_COL_MAPPING, "handle_update_order_packing_list_info_post_v1"),
        (OrderCreation, "warehouse.views.pre_port.order_creation", "packing_list",
         PACKING_LIST_TEMP_COL_MAPPING, "handle_update_order_packing_list_info_post"),
    )

    def make_order(self, offload=None):
        container = Container(id=1, container_number="TEST1234567")
        container.save = MagicMock()
        order = Order(id=1, container_number=container, offload_id=offload or Offload(id=1))
        order.save = MagicMock()
        return order

    def spreadsheet(self, mapping):
        values = {
            "shipping_mark": "MARK-01", "pcs": 61, "total_weight_kg": 10,
            "total_weight_lbs": 22.05, "cbm": 2, "product_name": "TIRE",
            "model": "MODEL-01", "destination": "ONT8", "delivery_method": "自发",
        }
        workbook = Workbook()
        workbook.active.append(list(mapping))
        workbook.active.append([values.get(field, "") for field in mapping.values()])
        content = BytesIO()
        workbook.save(content)
        return SimpleUploadedFile("packing.xlsx", content.getvalue())

    async def test_both_uploads_preview_excel_without_saving(self):
        for view_class, module, context_key, mapping, save_method in self.cases:
            with self.subTest(view=view_class.__name__):
                view = view_class()
                order = self.make_order()
                context = {
                    "selected_order": order, "can_import_packing_list": True,
                    "delivery_types": [("一件代发", "一件代发")],
                    "delivery_options": DROPSHIPPING_DELIVERY_METHOD_OPTIONS if view_class is Dropshipping else DELIVERY_METHOD_OPTIONS,
                }
                view.handle_order_management_container_get = AsyncMock(return_value=("details", context))
                request = RequestFactory().post("/", {
                    "source": "order_management", "container_number": "TEST1234567",
                    "file": self.spreadsheet(mapping),
                })
                request.user = AnonymousUser()
                template, result = await view.handle_upload_template_post(request)
                self.assertEqual(template, view.template_order_details_pl)
                self.assertEqual(request.GET["container_number"], "TEST1234567")
                self.assertEqual(len(result[context_key]), 1)
                self.assertIsNone(result[context_key][0].pk)
                self.assertEqual(result[context_key][0].pcs, 61)
                order.save.assert_not_called()
                html = render_to_string(template, result, request=request)
                self.assertIn('enctype="multipart/form-data"', html)
                self.assertIn('name="step" value="upload_template"', html)
                self.assertIn('name="step" value="download_template"', html)
                if view_class is Dropshipping:
                    self.assertIn('name="step" value="update_order_packing_list_info_v1"', html)
                    self.assertIn('value="self_ship" selected', html)

    async def test_upload_and_save_reject_orders_unpacked_since_preview(self):
        for view_class, module, _, _, save_method in self.cases:
            with self.subTest(view=view_class.__name__):
                view = view_class()
                order = self.make_order(Offload(id=1, offload_other_selfpick_cargos_at="2026-09-17"))
                view.handle_order_management_container_get = AsyncMock(return_value=("details", {"selected_order": order}))
                request = RequestFactory().post("/", {"source": "order_management", "container_number": "TEST1234567"})
                with self.assertRaises(PermissionDenied):
                    await view.handle_upload_template_post(request)
                with patch(f"{module}.Order.objects") as manager:
                    manager.select_related.return_value.get.return_value = order
                    with self.assertRaises(PermissionDenied):
                        await getattr(view, save_method)(request)

    def test_controls_hide_upload_after_any_partial_unpack(self):
        for field in ("offload_at", "offload_other_at", "offload_other_selfdelivery_at",
                      "offload_other_selfpick_cargos_at", "offload_at_container"):
            with self.subTest(field=field):
                self.assertFalse(can_import_packing_list(SimpleNamespace(**{field: "2026-09-17"})))
        for allowed in (True, False):
            html = render_to_string("order_management/packing_list_upload_controls.html", {
                "can_import_packing_list": allowed,
                "selected_order": self.make_order(),
            })
            self.assertEqual('value="upload_template"' in html, allowed)
            self.assertIn('value="download_template"', html)

    def test_missing_import_state_does_not_claim_order_was_unpacked(self):
        html = render_to_string("order_management/packing_list_upload_controls.html", {})
        self.assertNotIn("已拆柜订单", html)
        self.assertIn("未获取到导入状态", html)

    def test_unpacked_orders_keep_selected_rows_update_button(self):
        request = RequestFactory().get("/")
        request.user = AnonymousUser()
        for area in ("order_management", "dropshipping"):
            for unpacked in (False, True):
                with self.subTest(area=area, unpacked=unpacked):
                    order = self.make_order(Offload(id=1, offload_at="2026-09-17" if unpacked else None))
                    html = render_to_string(f"{area}/order_details.html", {
                        "selected_order": order,
                        "can_import_packing_list": not unpacked,
                    }, request=request)
                    self.assertIn("更新选中PackingList信息", html)
                    self.assertIn('form="packing-list-form"', html)
                    self.assertIn('onclick="openAddPackingListModal();"', html)
                    self.assertIn("删除选中行", html)
                    self.assertEqual('value="upload_template"' in html, not unpacked)

    async def test_import_confirmation_builds_complete_replacement(self):
        for view_class, module, _, _, save_method in self.cases:
            with self.subTest(view=view_class.__name__):
                view = view_class()
                order = self.make_order()
                view.handle_order_management_container_get = AsyncMock(return_value=("details", {}))
                view._update_container_unpacking_priority = AsyncMock()
                request = RequestFactory().post("/", {
                    "source": "order_management", "container_number": "TEST1234567",
                    "shipping_mark": "MARK-01", "pcs": "61", "total_weight_kg": "10",
                    "total_weight_lbs": "22.05", "cbm": "2", "product_name": "TIRE",
                    "model": "MODEL-01", "destination": "ONT8", "delivery_method": "自发",
                    "delivery_type": "一件代发" if view_class is Dropshipping else "public",
                })
                model_name = "DropshipCargo" if view_class is Dropshipping else "PackingList"
                with (
                    patch(f"{module}.Order.objects") as orders,
                    patch(f"{module}.Container.objects") as containers,
                    patch(f"{module}.{model_name}.objects") as cargos,
                    patch(f"{module}.replace_packing_list") as replace,
                ):
                    orders.select_related.return_value.get.return_value = order
                    containers.get.return_value = order.container_number
                    cargos.filter.return_value = []
                    if view_class is OrderCreation:
                        with patch(f"{module}.PoCheckEtaSeven.objects") as checks:
                            checks.filter.return_value = []
                            await getattr(view, save_method)(request)
                    else:
                        await getattr(view, save_method)(request)
                    rows = replace.call_args.args[1]
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0].pcs, 61)
                    self.assertEqual(rows[0].shipping_mark, "MARK-01")
                    self.assertTrue(rows[0].PO_ID)
                    if view_class is Dropshipping:
                        self.assertEqual(rows[0].delivery_method, "self_ship")
                    view.handle_order_management_container_get.assert_awaited_once()
