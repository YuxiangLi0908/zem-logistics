from typing import Any

from asgiref.sync import sync_to_async
from django.db.models import F, Sum, Case, When, IntegerField, Max
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View
from sqlalchemy import values

from warehouse.models.order import Order
from warehouse.models.pallet import Pallet


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
        eta_date = request.POST.get("eta_date")
        etd_date = request.POST.get("etd_date")
        pallets_list = await self._get_pallet(warehouse, eta_date, etd_date)
        context = {
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
            "datas": pallets_list,
            "etd_date": etd_date,
            "eta_date": eta_date,
        }
        return self.main_template, context


    async def _get_pallet(self, warehouse: str, eta: str, etd: str) -> list[
        dict]:  # 1. 修正返回类型注解（values返回dict列表，非Pallet实例）
        # 2. 时间字符串转datetime对象（适配前端传递的datetime-local格式，如"2025-10-01T12:00"）
        try:
            # 处理前端传递的 datetime-local 格式（分割"T"符号，转为datetime）
            etd_datetime = timezone.datetime.strptime(etd, "%Y-%m-%dT%H:%M")
            eta_datetime = timezone.datetime.strptime(eta, "%Y-%m-%dT%H:%M")
            # 转为带时区的datetime（若模型字段是DateTimeField且带时区）
            etd_datetime = timezone.make_aware(etd_datetime)
            eta_datetime = timezone.make_aware(eta_datetime)
        except ValueError as e:
            # 时间格式错误时返回空列表（或根据业务抛异常）
            print(f"时间格式错误: {e}")
            return []

        return await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "shipment_batch_number",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
                "container_number__order__vessel_id",
                "shipment_batch_number__packinglist",
            )
            # 3. 筛选条件：仓库 + 时间范围（确保字段与模型一致）
            .filter(
                location=warehouse,
                # 注意：确保模型中 vessel_etd/vessel_eta 是 DateTimeField 类型
                container_number__order__vessel_id__vessel_etd__gte=etd_datetime,
                container_number__order__vessel_id__vessel_eta__lte=eta_datetime,
            )
            # 4. 先按柜号分组（values放在annotate前才是分组，原代码顺序错误导致分组失效）
            .values("container_number__container_number")
            # 5. 分组后聚合计算（Max/Min/Sum等，避免重复数据）
            .annotate(
                # 取每个柜号的最新创建时间（分组后聚合）
                created_at=Max("container_number__order__created_at"),
                # 关联字段聚合（确保同一柜号下这些字段唯一，否则需用Max/Min）
                customer_name=F("container_number__order__customer_name__zem_name"),
                retrieval_destination_area=F("container_number__order__retrieval_id__retrieval_destination_area"),
                retrieval_note=F("container_number__order__retrieval_id__note"),
                mbl=F("container_number__order__vessel_id__master_bill_of_lading"),
                shipping_line=F("container_number__order__vessel_id__shipping_line"),
                vessel=F("container_number__order__vessel_id__vessel"),
                container_type=F("container_number__container_type"),
                # 计算船期天数（注意：日期差返回timedelta，前端需处理.days）
                vessel_date=F("container_number__order__vessel_id__vessel_eta")
                            - F("container_number__order__vessel_id__vessel_etd"),
                vessel_etd=F("container_number__order__vessel_id__vessel_etd"),
                vessel_eta=F("container_number__order__vessel_id__vessel_eta"),
                temp_t49_available_for_pickup=F("container_number__order__retrieval_id__temp_t49_available_for_pickup"),
                retrieval_carrier=F("container_number__order__retrieval_id__retrieval_carrier"),
                # 客户自提数量（按分组聚合）
                ke_destination_num=Sum(
                    Case(
                        When(
                            shipment_batch_number__packinglist__delivery_type="other",
                            shipment_batch_number__packinglist__destination="客户自提",
                            then=1
                        ),
                        default=0,
                        output_field=IntegerField()
                    )
                ),
                # 其他配送类型数量（按分组聚合）
                total_other_delivery=Sum(
                    Case(
                        When(
                            shipment_batch_number__packinglist__delivery_type="other",
                            then=1
                        ),
                        default=0,
                        output_field=IntegerField()
                    )
                ),
                # 私仓数量 = 其他配送总数 - 客户自提数量
                si_destination_num=F("total_other_delivery") - F("ke_destination_num"),
                # 6. 重命名柜号字段（避免长关联路径，前端直接用container）
                container=F("container_number__container_number")
            )
            # 7. 最终返回字段（确保与前端模板字段匹配）
            .values(
                "container", "created_at", "customer_name", "retrieval_destination_area",
                "retrieval_note", "mbl", "shipping_line", "vessel", "container_type",
                "vessel_date", "vessel_etd", "vessel_eta", "temp_t49_available_for_pickup",
                "retrieval_carrier", "ke_destination_num", "si_destination_num"
            )
            # 8. 按创建时间倒序（最新数据在前）
            .order_by("-created_at")
        )

    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }