from datetime import datetime, timezone
from typing import Any

import pytz
from asgiref.sync import sync_to_async
from django.db import models
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View

from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.utils.constants import ADDITIONAL_CONTAINER


class ContainerPickupStatus(View):
    template_status_summary = (
        "pre_port/container_status/01_container_status_summary.html"
    )

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get()
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
        if step == "arrive_at_destination":
            template, context = await self.handle_arrive_at_destination_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "empty_return":
            template, context = await self.handle_empty_return_post(request)
            return await sync_to_async(render)(request, template, context)

    async def handle_all_get(self) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        orders_pickup_scheduled = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
                    & models.Q(retrieval_id__arrive_at_destination=False)
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("retrieval_id__actual_retrieval_timestamp")
        )
        orders_at_warehouse = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
            )
            .filter(
                (
                    (
                        models.Q(container_number__order__order_type="转运")
                        | models.Q(container_number__order__order_type="转运组合")
                    )
                    & models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
                    & models.Q(retrieval_id__arrive_at_destination=True)
                    & models.Q(offload_id__offload_at__isnull=True)
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("retrieval_id__arrive_at")
        )
        orders_palletized = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
                    & models.Q(retrieval_id__arrive_at_destination=True)
                    & models.Q(offload_id__offload_at__isnull=False)
                    & models.Q(retrieval_id__empty_returned=False)
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("offload_id__offload_at")
        )
        context = {
            "orders_pickup_scheduled": orders_pickup_scheduled,
            "orders_at_warehouse": orders_at_warehouse,
            "orders_palletized": orders_palletized,
            "current_date": current_date,
        }
        return self.template_status_summary, context

    async def handle_arrive_at_destination_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        arrive_at = request.POST.get("arrive_at")
        order = await sync_to_async(
            Order.objects.select_related("retrieval_id", "offload_id").get
        )(container_number__container_number=container_number)
        retrieval = order.retrieval_id
        tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
        arrive_at = self._parse_ts(arrive_at, tzinfo)
        retrieval.arrive_at = arrive_at
        retrieval.arrive_at_destination = True
        await sync_to_async(retrieval.save)()
        if order.order_type == "直送":
            offload = order.offload_id
            offload.offload_at = arrive_at
            await sync_to_async(offload.save)()
        return await self.handle_all_get()

    async def handle_empty_return_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        empty_returned_at = request.POST.get("empty_returned_at")
        retrieval = await sync_to_async(Retrieval.objects.get)(
            order__container_number__container_number=container_number
        )
        tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
        empty_returned_at = self._parse_ts(empty_returned_at, tzinfo)
        retrieval.empty_returned = True
        retrieval.empty_returned_at = empty_returned_at
        await sync_to_async(retrieval.save)()
        return await self.handle_all_get()

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    def _parse_tzinfo(self, s: str | None) -> str:
        if not s:
            return "America/New_York"
        elif "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: str, tzinfo: str) -> str:
        ts_naive = datetime.fromisoformat(ts)
        tz = pytz.timezone(tzinfo)
        ts = tz.localize(ts_naive).astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
