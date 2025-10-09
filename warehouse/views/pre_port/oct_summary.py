from typing import Any

from asgiref.sync import sync_to_async
from django.db.models import F, Sum, Case, When, IntegerField
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
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
        pallets_list = await self._get_pallet(warehouse)
        context = {
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
            "datas": pallets_list  # 原有的数据列表
        }
        return self.main_template, context

    async def _get_pallet(self, warehouse: str) -> list[Pallet]:
        return await sync_to_async(list)(Pallet.objects.prefetch_related(
            "container_number",
            "shipment_batch_number",
            "container_number__order__customer_name",
            "container_number__order__retrieval_id",
            "container_number__order__vessel_id",
            "shipment_batch_number__packinglist",
        )
        .filter(location=warehouse)
        .annotate(
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
            total_other_delivery=Sum(
                Case(
                    When(
                        shipment_batch_number__packinglist__delivery_type="other",
                        then=1
                    ),
                    default=0,
                    output_field=IntegerField()
                )
            )
        )
        .values(
            created_at=F("container_number__order__created_at"),
            customer_name=F("container_number__order__customer_name__zem_name"),
            retrieval_destination_area=F("container_number__order__retrieval_id__retrieval_destination_area"),
            retrieval_note=F("container_number__order__retrieval_id__note"),
            container=F("container_number__container_number"),
            mbl=F("container_number__order__vessel_id__master_bill_of_lading"),
            shipping_line=F("container_number__order__vessel_id__shipping_line"),
            vessel=F("container_number__order__vessel_id__vessel"),
            container_type=F("container_number__container_type"),
            vessel_date=F("container_number__order__vessel_id__vessel_eta") - F(
                "container_number__order__vessel_id__vessel_etd"),
            vessel_etd=F("container_number__order__vessel_id__vessel_etd"),
            vessel_eta=F("container_number__order__vessel_id__vessel_eta"),
            temp_t49_available_for_pickup=F("container_number__order__retrieval_id__temp_t49_available_for_pickup"),
            retrieval_carrier=F("container_number__order__retrieval_id__retrieval_carrier"),
            ke_destination_num=F("ke_destination_num"),
            si_destination_num=F("total_other_delivery") - F("ke_destination_num")
        ).order_by('-created_at'))

    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }