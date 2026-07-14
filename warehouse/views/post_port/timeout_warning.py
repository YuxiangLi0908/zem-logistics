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
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Coalesce
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View

from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse


class TimeoutWarning(View):
    template_shipment = "post_port/timeout_inventory/timeout_shipment.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        context = {"warehouse_options": [("", "")] + await sync_to_async(list)(
            ZemWarehouse.objects
            .order_by("name")
            .values_list("name", "name")
        ), }
        return await sync_to_async(render)(request, self.template_shipment, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "timeout_warehouse_select":
            template, context = await self.handle_timeout_inventory_get(request)
            return render(request, template, context)

    async def handle_timeout_inventory_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''库存时效预警的界面筛选'''
        warehouse = request.POST.get("warehouse")
        delivery_type = request.POST.get("delivery_type")
        # 拆柜入库1周，没有约的板子，只看2/1号之后的
        pallets = await self._get_packing_list(warehouse, delivery_type)

        # 预约时间过期没有排车
        now = timezone.now() + timezone.timedelta(days=1)
        filter_conditions = (
            Q(shipment_appointment__lte=now)
            & Q(shipment_type="FTL")
            & Q(fleet_number__isnull=True)
            & Q(origin=warehouse)
            & Q(is_canceled=False)
        )
        if delivery_type:
            filter_conditions &= (Q(delivery_type__isnull=True) | Q(delivery_type=""))
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(filter_conditions).order_by("shipment_appointment")
        )

        # 提货时间已过期没有确认出库
        fleet_filter_conditions = Q(
            appointment_datetime__lte=now,
            departured_at__isnull=True,
            origin=warehouse
        )
        if delivery_type:
            fleet_filter_conditions &= (Q(delivery_type__isnull=True) | Q(delivery_type=""))

        fleets = await sync_to_async(list)(
            Fleet.objects.filter(fleet_filter_conditions)
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )
        
        # 逾期未确认
        target_date = datetime(2025, 6, 1)
        un_confirmed_fleets_conditions = Q(
            departured_at__isnull=False,
            origin=warehouse,
            arrived_at__isnull=True,
        )
        if delivery_type:
            un_confirmed_fleets_conditions &= (Q(delivery_type__isnull=True) | Q(delivery_type=""))
        un_confirmed_fleets = await sync_to_async(list)(
            Fleet.objects.filter(un_confirmed_fleets_conditions)
            .filter(
                models.Q(shipment__shipment_appointment__lte=now - timedelta(days=3))
                & models.Q(shipment__shipment_appointment__gte=target_date)
                & models.Q(shipment__arrived_at__isnull=True)
            )
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )

        # 逾期未上传POD
        un_podlinks_conditions = Q(
            departured_at__isnull=False,
            origin=warehouse,
            arrived_at__isnull=False,
        )

        # 如果 delivery_type 有值，添加 delivery_type 过滤条件
        if delivery_type:
            un_podlinks_conditions &= (Q(delivery_type__isnull=True) | Q(delivery_type=""))
        un_podlinks = await sync_to_async(list)(
            Fleet.objects.filter(un_podlinks_conditions)
            .filter(
                models.Q(shipment__pod_link__isnull=True)
                & models.Q(shipment__shipment_appointment__isnull=False)
                & models.Q(shipment__shipment_appointment__gte=target_date)
            )
            .annotate(
                shipment_batch_numbers=Coalesce(
                    Subquery(
                        Shipment.objects.filter(
                            fleet_number_id=OuterRef("pk"),  # 关联当前 Fleet
                            pod_link__isnull=True,
                            shipment_appointment__isnull=False,
                        )
                        .annotate(
                            batch_numbers=StringAgg(
                                "shipment_batch_number", delimiter=","
                            )
                        )
                        .values("batch_numbers")[:1],
                        output_field=CharField(),
                    ),
                    Value(""),  # 如果没有满足条件的 Shipment，返回空字符串
                ),
                appointment_ids=Coalesce(
                    Subquery(
                        Shipment.objects.filter(
                            fleet_number_id=OuterRef("pk"),
                            pod_link__isnull=True,
                            shipment_appointment__isnull=False,
                        )
                        .annotate(ids=StringAgg("appointment_id", delimiter=","))
                        .values("ids")[:1],
                        output_field=CharField(),
                    ),
                    Value(""),
                ),
            )
            .order_by("appointment_datetime")
        )
        context = {
            "warehouse": warehouse,
            "warehouse_options": [("", "")] + await sync_to_async(list)(
                ZemWarehouse.objects
                .order_by("name")
                .values_list("name", "name")
            ),
            "pallets": pallets,  # 未预约
            "shipments": shipments,  # 未排车
            "fleets": fleets,  # 未出库
            "un_confirmed_fleets": un_confirmed_fleets,
            "un_podlinks": un_podlinks,
            "delivery_type": delivery_type
        }
        return self.template_shipment, context

    async def _get_packing_list(
        self,
        warehouse: str,
        delivery_type = None,
    ) -> list[Any]:
        now = timezone.now() + timezone.timedelta(days=1)
        one_weeks_ago = now - timedelta(weeks=1)

        # 构建基础 filter 条件
        filter_conditions = (
            models.Q(
                container_number__orders__offload_id__offload_at__lte=one_weeks_ago
            )
            & models.Q(
                container_number__orders__offload_id__offload_at__gte="2025-02-01"
            )
            & models.Q(shipment_batch_number__shipment_batch_number__isnull=True)
            & ~Q(delivery_method="暂扣留仓(HOLD)")
            & models.Q(location=warehouse)
        )
        if delivery_type:
            filter_conditions &= models.Q(delivery_type=delivery_type)

        pallets = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__orders",
                "container_number__orders__warehouse",
                "shipment_batch_number",
                "container_number__orders__offload_id",
                "container_number__orders__customer_name",
                "container_number__orders__retrieval_id",
                "container_number__orders__vessel_id",
            )
            .filter(filter_conditions)
            .annotate(
                schedule_status=Case(
                    When(
                        Q(
                            container_number__orders__offload_id__offload_at__lte=datetime.now().date()
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
                "container_number__orders__customer_name__zem_name",
                "destination",
                "location",
                "address",
                "delivery_method",
                "container_number__orders__offload_id__offload_at",
                "schedule_status",
                "abnormal_palletization",
                "po_expired",
                "container_number__orders__vessel_id__vessel_eta",
                "sequence_number",
                "PO_ID",
                warehouse=F(
                    "container_number__orders__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                eta=F("container_number__orders__vessel_id__vessel_eta"),
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
                "container_number__orders__offload_id__offload_at", "sequence_number"
            )
        )
        return pallets

    async def _user_authenticate(self, request: HttpRequest):
        return await sync_to_async(lambda: request.user.is_authenticated)()

