from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from django.test import RequestFactory, SimpleTestCase

from warehouse.views.dropshipping import Dropshipping


class DropshippingUnpackTests(SimpleTestCase):
    async def test_actual_counts_are_saved_and_discrepancies_are_visible(self):
        for actual_counts in ([61, 75], [0, 0], [60, 76]):
            with self.subTest(actual_counts=actual_counts):
                offload = SimpleNamespace(offload_at=None, save=MagicMock())
                container = MagicMock()
                order = SimpleNamespace(
                    offload_id=offload, container_number=container,
                    warehouse=SimpleNamespace(name="NJ"),
                )
                cargos = [
                    SimpleNamespace(id=1, pcs=60),
                    SimpleNamespace(id=2, pcs=76),
                ]
                request = RequestFactory().post("/", {
                    "ids": ["1", "2"], "n_pallet": ["8", "10"],
                    "pcs_actul": actual_counts, "pcs_reported": ["60", "76"],
                    "cbms": ["10", "10"], "weights": ["100", "100"],
                    "product_names": ["TIRE", "TIRE"], "models": ["A", "B"],
                    "address": ["", ""], "delivery_method": ["pickup", "pickup"],
                    "delivery_type": ["一件代发", "一件代发"],
                    "shipping_marks": ["M1", "M2"], "notes": ["", ""],
                    "po_ids": ["PO1", "PO2"], "offload_time": "2026-09-17T10:00",
                })
                view = Dropshipping()
                view.handle_warehouse_post_palletize = AsyncMock(return_value=("page", {}))
                view.handle_palletization_abnormal_get = AsyncMock(return_value=("abnormal", {}))
                with (
                    patch("warehouse.views.dropshipping.Order") as order_model,
                    patch("warehouse.views.dropshipping.Container") as container_model,
                    patch("warehouse.views.dropshipping.DropshipCargo") as cargo_model,
                    patch("warehouse.views.dropshipping.DropshipInventory") as inventory_model,
                    patch("warehouse.views.dropshipping.AbnormalOffloadStatus") as abnormal_model,
                    patch("warehouse.views.dropshipping.bulk_create_with_history"),
                ):
                    order_model.objects.select_related.return_value.prefetch_related.return_value.get.return_value = order
                    container_model.objects.get.return_value = container
                    cargo_model.objects.filter.return_value = cargos
                    result = await view.handle_packing_list_post(request, 1)

                    self.assertEqual([cargo.pcs for cargo in cargos], list(actual_counts))
                    self.assertEqual([cargo.pallets for cargo in cargos], [8, 10])
                    self.assertEqual(cargo_model.objects.bulk_update.call_args.args[1], ["pcs", "pallets", "status"])
                    self.assertEqual(
                        [call.kwargs["pcs_change"] for call in inventory_model.call_args_list],
                        list(actual_counts),
                    )
                    expected = [(reported, actual) for reported, actual in zip([60, 76], actual_counts) if reported != actual]
                    self.assertEqual(
                        [(call.kwargs["pcs_reported"], call.kwargs["pcs_actual"]) for call in abnormal_model.call_args_list],
                        expected,
                    )
                    for call in abnormal_model.call_args_list:
                        self.assertEqual(call.kwargs["delivery_type"], "一件代发")
                    if expected:
                        self.assertEqual(result, ("abnormal", {}))
                        view.handle_palletization_abnormal_get.assert_awaited_once_with()
                        view.handle_warehouse_post_palletize.assert_not_awaited()
                    else:
                        self.assertEqual(result, ("page", {}))
                        view.handle_warehouse_post_palletize.assert_awaited_once_with(request)
                        view.handle_palletization_abnormal_get.assert_not_awaited()
