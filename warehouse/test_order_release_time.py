from contextlib import ExitStack
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from django.contrib.auth.models import AnonymousUser
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase
from django.utils import timezone

from warehouse.views.dropshipping import Dropshipping
from warehouse.views.pre_port.order_creation import OrderCreation


class OrderReleaseTimeTests(SimpleTestCase):
    cases = (
        (Dropshipping, "warehouse.views.dropshipping", "dropshipping/order_details.html"),
        (OrderCreation, "warehouse.views.pre_port.order_creation", "order_management/order_details.html"),
    )

    async def test_save_clear_and_omitted_field(self):
        for view_class, module, _ in self.cases:
            for value in ("2026-09-17T10:30", "", None):
                with self.subTest(view=view_class.__name__, value=value):
                    original = timezone.make_aware(datetime(2026, 9, 16, 8, 0))
                    retrieval = SimpleNamespace(
                        retrieval_destination_precise="LA-91730",
                        planned_release_time=original, save=MagicMock(),
                    )
                    order = SimpleNamespace(order_type="直送", retrieval_id="R1", save=MagicMock())
                    data = {
                        "container_number": "TEST1234567", "retrieval_note": "",
                        "retrieval_destination_precise": "LA-91730",
                    }
                    if value is not None:
                        data["planned_release_time"] = value
                    request = RequestFactory().post("/", data)
                    view = view_class()
                    view.handle_order_management_container_get = AsyncMock(return_value=("details", {}))
                    with ExitStack() as stack:
                        orders = stack.enter_context(patch(f"{module}.Order.objects"))
                        retrievals = stack.enter_context(patch(f"{module}.Retrieval.objects"))
                        stack.enter_context(patch(f"{module}.ZemWarehouse.objects"))
                        if view_class is Dropshipping:
                            cargos = stack.enter_context(patch(f"{module}.DropshipCargo.objects"))
                            cargos.filter.return_value = []
                        orders.select_related.return_value.get.return_value = order
                        retrievals.get.return_value = retrieval
                        await view.handle_update_order_retrieval_info_post(request)
                    expected = (
                        timezone.make_aware(datetime(2026, 9, 17, 10, 30)) if value
                        else original if value is None else None
                    )
                    self.assertEqual(retrieval.planned_release_time, expected)
                    retrieval.save.assert_called_once()

    async def test_invalid_time_rejected_before_saving(self):
        for view_class, _, _ in self.cases:
            request = RequestFactory().post("/", {"planned_release_time": "invalid"})
            template, context = await view_class().handle_update_order_retrieval_info_post(request)
            self.assertEqual(template, "error_template.html")
            self.assertIn("放行时间", context["error"])

    def test_both_dispatch_forms_display_saved_time(self):
        request = RequestFactory().get("/")
        request.user = AnonymousUser()
        for _, _, template in self.cases:
            with self.subTest(template=template):
                html = render_to_string(template, {
                    "retrieval": SimpleNamespace(
                        planned_release_time=timezone.make_aware(datetime(2026, 9, 17, 10, 30)),
                    ),
                }, request=request)
                self.assertIn('name="planned_release_time" value="2026-09-17T10:30"', html)
                self.assertIn('for="planned-release-time">放行时间</label>', html)
