from datetime import datetime, timedelta
from typing import Any

from asgiref.sync import sync_to_async
from django.db import models
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View

from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.retrieval import Retrieval
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    ADDITIONAL_CONTAINER,
    CONTAINER_PICKUP_CARRIER,
    WAREHOUSE_OPTIONS,
)


class TerminalDispatch(View):
    template_terminal_dispatch = (
        "pre_port/vessel_terminal_tracking/02_terminal_dispatch.html"
    )
    template_schedule_container_pickup = (
        "pre_port/vessel_terminal_tracking/03_schedule_container_pickup.html"
    )
    template_update_container_pickup_schedule = (
        "pre_port/vessel_terminal_tracking/04_update_container_pickup_schedule.html"
    )

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "schedule_container_pickup":
            template, context = await self.handle_schedule_container_pickup_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_pickup_schedule":
            template, context = await self.hanlde_update_pickup_schedule_get(request)
            return await sync_to_async(render)(request, template, context)
        else:
            context = {}
            return await sync_to_async(render)(
                request, self.template_terminal_dispatch, context
            )

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "pickup_schedule_confirmation":
            template, context = await self.handle_pickup_schedule_confirmation_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "pickup_schedule_update":
            template, context = await self.handle_pickup_schedule_confirmation_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "confirm_pickup":
            template, context = await self.handle_confirm_pickup_post(request)
            return await sync_to_async(render)(request, template, context)

    async def handle_all_get(self) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        orders_not_scheduled = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
                    & models.Q(retrieval_id__retrieval_carrier__isnull=True)
                    & models.Q(
                        vessel_id__vessel_eta__lte=datetime.now() + timedelta(weeks=2)
                    )
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("vessel_id__vessel_eta")
        )
        orders_not_pickup = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
                    & models.Q(retrieval_id__retrieval_carrier__isnull=False)
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("vessel_id__vessel_eta")
        )
        context = {
            "orders_not_scheduled": orders_not_scheduled,
            "orders_not_pickup": orders_not_pickup,
            "current_date": current_date,
        }
        return self.template_terminal_dispatch, context

    async def handle_schedule_container_pickup_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        _, context = await self.handle_all_get()
        order = await sync_to_async(
            Order.objects.select_related(
                "container_number", "customer_name", "vessel_id", "retrieval_id"
            ).get
        )(container_number__container_number=container_number)
        if order.order_type == "直送":
            packing_list = await sync_to_async(PackingList.objects.filter)(
                container_number__container_number=container_number
            )
            context["packing_list"] = packing_list
        context["container_number"] = container_number
        context["selected_order"] = order
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
        return self.template_schedule_container_pickup, context

    async def hanlde_update_pickup_schedule_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        _, context = await self.handle_schedule_container_pickup_get(request)
        return self.template_update_container_pickup_schedule, context

    async def handle_pickup_schedule_confirmation_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        destination = request.POST.get("retrieval_destination").upper().strip()
        order_type = request.POST.get("order_type")
        order = await sync_to_async(Order.objects.select_related("retrieval_id").get)(
            models.Q(container_number__container_number=container_number)
        )
        if order_type == "转运" or order_type == "转运组合":
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=destination)
            order.warehouse = warehouse
            await sync_to_async(order.save)()
        retrieval = order.retrieval_id
        retrieval.retrieval_destination_precise = destination
        retrieval.retrieval_carrier = request.POST.get("retrieval_carrier").strip()
        if request.POST.get("target_retrieval_timestamp"):
            retrieval.target_retrieval_timestamp = request.POST.get(
                "target_retrieval_timestamp"
            )
        else:
            retrieval.target_retrieval_timestamp = None
        if request.POST.get("target_retrieval_timestamp_lower"):
            retrieval.target_retrieval_timestamp_lower = request.POST.get(
                "target_retrieval_timestamp_lower"
            )
        else:
            retrieval.target_retrieval_timestamp_lower = None
        retrieval.note = request.POST.get("note", "").strip()
        retrieval.scheduled_at = datetime.now()
        if request.POST.get("retrieval_carrier") == "客户自提":
            if request.POST.get("target_retrieval_timestamp"):
                retrieval.target_retrieval_timestamp = request.POST.get(
                    "target_retrieval_timestamp"
                )
            else:
                retrieval.target_retrieval_timestamp = None
            if request.POST.get("target_retrieval_timestamp_lower"):
                retrieval.target_retrieval_timestamp_lower = request.POST.get(
                    "target_retrieval_timestamp_lower"
                )
            else:
                retrieval.target_retrieval_timestamp_lower = None
        await sync_to_async(retrieval.save)()
        # 有提柜计划后，就将记录归为“提柜前一天
        orders = await sync_to_async(list)(
            PoCheckEtaSeven.objects.filter(
                container_number__container_number=container_number
            )
        )
        try:
            for o in orders:
                o.time_status = False
                await sync_to_async(o.save)()
        except PoCheckEtaSeven.DoesNotExist:
            pass

        return await self.handle_all_get()

    async def handle_confirm_pickup_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        retrieval = await sync_to_async(Retrieval.objects.get)(
            order__container_number__container_number=container_number
        )
        retrieval.actual_retrieval_timestamp = request.POST.get(
            "actual_retrieval_timestamp"
        )
        # 填了实际提柜但是没有写预计提柜的，就默认预计提柜时间为实际提柜时间
        if not retrieval.target_retrieval_timestamp:
            retrieval.target_retrieval_timestamp = request.POST.get(
                "actual_retrieval_timestamp"
            )
        if not retrieval.target_retrieval_timestamp_lower:
            retrieval.target_retrieval_timestamp_lower = request.POST.get(
                "actual_retrieval_timestamp"
            )
        today = datetime.now()
        actual_ts = request.POST.get("actual_retrieval_timestamp")
        actual_ts = datetime.strptime(actual_ts, "%Y-%m-%dT%H:%M")
        # 如果是当天提柜
        if actual_ts <= today + timedelta(days=1):
            orders = await sync_to_async(list)(
                PoCheckEtaSeven.objects.filter(
                    container_number__container_number=container_number
                )
            )
            try:
                for o in orders:
                    o.time_status = False
                    await sync_to_async(o.save)()
            except PoCheckEtaSeven.DoesNotExist:
                pass
        await sync_to_async(retrieval.save)()
        return await self.handle_all_get()

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
