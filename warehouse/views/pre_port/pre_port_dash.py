import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from asgiref.sync import sync_to_async
from django.core.cache import cache
from django.db import models
from django.db.models import Count
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.retrieval import Retrieval
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    CONTAINER_PICKUP_CARRIER,
    DELIVERY_METHOD_OPTIONS,
    PACKING_LIST_TEMP_COL_MAPPING,
    SHIPPING_LINE_OPTIONS,
    WAREHOUSE_OPTIONS,
)


class PrePortDash(View):
    template_main = "pre_port/dashboard/01_pre_port_summary.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get(tab="summary")
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
        # 根据建单时间和ETA进行筛选
        if step == "search_orders":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            start_date_eta = request.POST.get("start_date_eta")
            end_date_eta = request.POST.get("end_date_eta")
            template, context = await self.handle_all_get(
                start_date=start_date,
                end_date=end_date,
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                tab="summary",
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "download_eta_file":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            start_date_eta = request.POST.get("start_date_eta")
            end_date_eta = request.POST.get("end_date_eta")
            return await self.download_eta_file(
                start_date, end_date, start_date_eta, end_date_eta
            )
        else:
            return await sync_to_async(render)(request, self.template_main, {})

    async def download_eta_file(
        self, start_date, end_date, start_date_eta, end_date_eta
    ) -> HttpResponse:
        current_date = datetime.now().date()
        start_date_eta = (
            current_date.strftime("%Y-%m-%d") if not start_date_eta else start_date_eta
        )
        end_date_eta = (
            (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d")
            if not end_date_eta
            else end_date_eta
        )
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            )
            .filter(
                models.Q(
                    created_at__gte=start_date,
                    created_at__lte=end_date,
                )
                & (
                    models.Q(
                        vessel_id__vessel_eta__gte=start_date_eta,
                        vessel_id__vessel_eta__lte=end_date_eta,
                    )
                    | models.Q(eta__gte=start_date_eta, eta__lte=end_date_eta)
                )
            )
            .values(
                "container_number__container_number",
                "customer_name__zem_code",
                "retrieval_id__retrieval_destination_area",
                "warehouse__name",
                "vessel_id__shipping_line",
                "vessel_id__vessel",
                "vessel_id__vessel_eta",
            )
        )
        df = pd.DataFrame(orders)
        # 修改列名为柜号，客户，所属仓/直送地址，具体仓库，ETA，shipping/vessel信息
        df = df.rename(
            {
                "container_number__container_number": "container",
                "customer_name__zem_code": "customer",
                "retrieval_id__retrieval_destination_area": "destination_area",
                "warehouse__name": "warehouse",
                "vessel_id__shipping_line": "shipping_line",
                "vessel_id__vessel": "vessel",
                "vessel_id__vessel_eta": "ETA",
            },
            axis=1,
        )
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename=ETA_week.csv"
        df.to_csv(path_or_buf=response, index=False, encoding="utf-8-sig")
        return response

    async def handle_all_get(
        self,
        start_date: str = None,
        end_date: str = None,
        start_date_eta: str = None,
        end_date_eta: str = None,
        tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (
            (datetime.now().date() + timedelta(days=-7)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d")
            if not end_date
            else end_date
        )
        criteria = models.Q(
            created_at__gte=start_date,
            created_at__lte=end_date,
            cancel_notification=False,
        )
        if start_date_eta:
            criteria &= models.Q(vessel_id__vessel_eta__gte=start_date_eta) | models.Q(
                eta__gte=start_date_eta
            )
        if end_date_eta:
            criteria &= models.Q(vessel_id__vessel_eta__lte=end_date_eta) | models.Q(
                eta__lte=end_date_eta
            )
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            )
            .filter(criteria)
            .order_by("vessel_id__vessel_eta")
        )
        context = {
            "customers": customers,
            "orders": orders,
            "start_date": start_date,
            "end_date": end_date,
            "start_date_eta": start_date_eta,
            "end_date_eta": end_date_eta,
            "current_date": current_date,
            "tab": tab,
        }
        return self.template_main, context

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
