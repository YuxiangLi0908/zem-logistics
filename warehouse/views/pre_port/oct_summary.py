from typing import Any

from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.db.models import F, Sum, Case, When, IntegerField, Max, CharField, Value, Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View
from sqlalchemy import values

from warehouse.models.order import Order
from warehouse.models.pallet import Pallet
from warehouse.utils.constants import PORT_TO_WAREHOUSE_AREA


class OctSummaryView(View):
    main_template="pre_port/oct_summary/01_pre_port_oct_summary.html"
    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.check_disnation()
            return render(request, template, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "warehouse":
            template, context = await self.oct_handle_warehouse_post(request)
            return render(request, template, context)

    async def check_disnation(self) -> tuple[Any, Any]:
        context = {"warehouse_options": self.warehouse_options}
        return self.main_template, context

    async def _user_authenticate(self, request: HttpRequest) -> bool:
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def oct_handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        eta_start = request.POST.get("eta_start")
        eta_end = request.POST.get("eta_end")
        pallets_list = await self._get_pallet(warehouse, eta_start, eta_end)
        context = {
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
            "datas": pallets_list,
            "eta_start": eta_start,
            "eta_end": eta_end,
        }
        return self.main_template, context

    async def _get_pallet(self, warehouse: str, eta_start: str, eta_end: str) -> list[dict]:
        # 时间处理：将年月转换为完整的时间范围（月初到月末最后一秒）
        eta_start_datetime = timezone.datetime.strptime(eta_start, "%Y-%m")
        eta_end_datetime = timezone.datetime.strptime(eta_end, "%Y-%m")
        eta_end_plus_1month = eta_end_datetime + relativedelta(months=1)
        eta_end_last_second = eta_end_plus_1month - relativedelta(seconds=1)

        eta_start_datetime = timezone.make_aware(eta_start_datetime)
        eta_end_datetime = timezone.make_aware(eta_end_last_second)

        # 构建Q对象用于条件过滤，区分不同订单类型的仓点过滤逻辑
        filter_conditions = Q(
            container_number__order__vessel_id__vessel_eta__gte=eta_start_datetime,
            container_number__order__vessel_id__vessel_eta__lte=eta_end_datetime,
        )

        # 组合条件：直送订单通过港口映射获取仓点，其他订单直接匹配仓点
        filter_conditions &= Q(
            # 直送订单：港口映射到仓点
            Q(
                container_number__order__order_type="直送",
                container_number__order__vessel_id__destination_port__in=[
                    port for port, wh in PORT_TO_WAREHOUSE_AREA.items() if wh == warehouse
                ]
            ) |
            # 其他订单：直接匹配仓点
            Q(
                container_number__order__order_type__in=["转运组合", "转运"],
                container_number__order__retrieval_id__retrieval_destination_area=warehouse
            )
        )

        return await sync_to_async(list)(
            Order.objects.prefetch_related(
                "container_number",
                "shipment_id",
                "customer_name",
                "retrieval_id",
                "vessel_id",
                "shipment_id__packinglist",
            )
            .filter(filter_conditions)
            .values("container_number__container_number")
            .annotate(
                created_at=Max("created_at"),
                customer_name=F("customer_name__zem_name"),
                retrieval_destination_area=Case(
                    When(order_type="直送",
                        # 直送订单从港口映射获取仓点
                        then=Value(warehouse)
                    ),
                    When(order_type="转运组合",
                        then=F("retrieval_id__retrieval_destination_area")
                    ),
                    When(order_type="转运",
                        then=F("retrieval_id__retrieval_destination_area")
                    ),
                    default=Value(""),
                    output_field=CharField()
                ),
                retrieval_note=F("retrieval_id__note"),
                mbl=F("vessel_id__master_bill_of_lading"),
                shipping_line=F("vessel_id__shipping_line"),
                vessel=F("vessel_id__vessel"),
                container_type=F("container_number__container_type"),
                vessel_date=F("vessel_id__vessel_eta") -
                            F("vessel_id__vessel_etd"),
                vessel_etd=F("vessel_id__vessel_etd"),
                vessel_eta=F("vessel_id__vessel_eta"),
                temp_t49_available_for_pickup=F("retrieval_id__temp_t49_available_for_pickup"),
                retrieval_carrier=F("retrieval_id__retrieval_carrier"),
                ke_destination_num=Sum(
                    Case(
                        When(
                            shipment_id__packinglist__delivery_type="other",
                            shipment_id__packinglist__destination="客户自提",
                            then=1
                        ),
                        default=0,
                        output_field=IntegerField()
                    )
                ),
                # 其他配送类型数量（聚合统计：delivery_type=other 的所有记录数）
                total_other_delivery=Sum(
                    Case(
                        When(
                            shipment_id__packinglist__delivery_type="other",
                            then=1
                        ),
                        default=0,
                        output_field=IntegerField()
                    )
                ),
                # 私仓数量 = 其他配送总数 - 客户自提数量
                si_destination_num=F("total_other_delivery") - F("ke_destination_num"),
                # 重命名柜号字段（前端直接使用 container 字段，无需处理长关联路径）
                container=F("container_number__container_number"),
                # 保留订单类型（便于前端后续扩展逻辑，如按类型筛选）
                order_type=F("order_type"),
                retrieval_destination_precise=F("retrieval_id__retrieval_destination_precise")
            )
            # 最终返回字段（与前端模板的字段严格对应，避免冗余）
            .values(
                "container", "created_at", "customer_name", "retrieval_destination_area",
                "retrieval_note", "mbl", "shipping_line", "vessel", "container_type",
                "vessel_date", "vessel_etd", "vessel_eta", "temp_t49_available_for_pickup",
                "retrieval_carrier", "ke_destination_num", "si_destination_num", "order_type"
            )
            # 按创建时间倒序（最新数据优先显示，符合用户习惯）
            .order_by("-created_at")
        )

    warehouse_options = {
        "": "",
        "NJ": "NJ",
        "SAV": "SAV",
        "LA": "LA",
        "MO": "MO",
        "TX": "TX",
    }