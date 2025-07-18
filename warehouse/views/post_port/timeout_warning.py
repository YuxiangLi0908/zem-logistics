from datetime import datetime, timedelta
from typing import Any

from asgiref.sync import sync_to_async
from django.contrib.auth.models import User
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View

from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment


class TimeoutWarning(View):
    template_shipment = "post_port/timeout_inventory/timeout_shipment.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        # if not await self._validate_user_group(request.user):
        #     return HttpResponseForbidden(
        #         "You are not authenticated to access this page!"
        #     )
        context = {"warehouse_options": self.warehouse_options}
        return await sync_to_async(render)(request, self.template_shipment, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden(
                "You are not authenticated to access this page!"
            )
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_timeout_inventory_get(request)
            return render(request, template, context)

    async def handle_timeout_inventory_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        # 拆柜入库3周，没有约的板子，只看2/1号之后的
        pallets = await self._get_packing_list(warehouse)
        # 预约时间过期没有排车
        now = timezone.now() + timezone.timedelta(days=1)
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(
                shipment_appointment__lte=now,
                shipment_type="FTL",
                fleet_number__isnull=True,
                origin=warehouse,
                is_canceled=False,
            ).order_by("shipment_appointment")
        )
        # 提货时间已过期没有确认出库
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(
                appointment_datetime__lte=now,
                departured_at__isnull=True,
                origin=warehouse,
            )
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )
        context = {
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options,
            "pallets": pallets,
            "shipments": shipments,
            "fleets": fleets,
        }
        return self.template_shipment, context

    async def _get_packing_list(
        self,
        warehouse: str,
    ) -> list[Any]:
        now = timezone.now() + timezone.timedelta(days=1)
        three_weeks_ago = now - timedelta(weeks=3)
        pallets = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number" "container_number__order__offload_id",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
                "container_number__order__vessel_id",
            )
            .filter(
                models.Q(
                    container_number__order__offload_id__offload_at__lte=three_weeks_ago
                )
                & models.Q(
                    container_number__order__offload_id__offload_at__gte="2025-02-01"
                )
                & models.Q(shipment_batch_number__shipment_batch_number__isnull=True)
                & ~Q(delivery_method="暂扣留仓(HOLD)")
                & models.Q(location=warehouse)
            )
            .annotate(
                schedule_status=Case(
                    When(
                        Q(
                            container_number__order__offload_id__offload_at__lte=datetime.now().date()
                            + timedelta(days=-7)
                        ),
                        then=Value("past_due"),
                    ),
                    default=Value("on_time"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
                str_length=Cast("length", CharField()),
                str_width=Cast("width", CharField()),
                str_height=Cast("height", CharField()),
                str_pcs=Cast("pcs", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__order__customer_name__zem_name",
                "destination",
                "address",
                "delivery_method",
                "container_number__order__offload_id__offload_at",
                "schedule_status",
                "abnormal_palletization",
                "po_expired",
                "container_number__order__vessel_id__vessel_eta",
                "sequence_number",
                "PO_ID",
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                eta=F("container_number__order__vessel_id__vessel_eta"),
                custom_delivery_method=F("delivery_method"),
                fba_ids=F("fba_id"),
                ref_ids=F("ref_id"),
                shipping_marks=F("shipping_mark"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet_act=Count("pallet_id", distinct=True),
                label=Value("ACT"),
                length=StringAgg("str_length", delimiter=",", ordering="str_length"),
                width=StringAgg("str_width", delimiter=",", ordering="str_width"),
                height=StringAgg("str_height", delimiter=",", ordering="str_height"),
                n_pcs=StringAgg("str_pcs", delimiter=",", ordering="str_pcs"),
            )
            .order_by(
                "container_number__order__offload_id__offload_at", "sequence_number"
            )
        )
        return pallets

    async def _user_authenticate(self, request: HttpRequest):
        return await sync_to_async(lambda: request.user.is_authenticated)()

    async def _validate_user_group(self, user: User) -> bool:
        is_staff = await sync_to_async(lambda: user.is_staff)()
        if is_staff:
            return True
        return await sync_to_async(
            lambda: user.groups.filter(name="shipmnet_leader").exists()
        )()
