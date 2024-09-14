import uuid
import os
import pandas as pd
import numpy as np
from asgiref.sync import sync_to_async
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.core.cache import cache
from django.db.models import Count


from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.offload import Offload
from warehouse.models.vessel import Vessel
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    PACKING_LIST_TEMP_COL_MAPPING, SHIPPING_LINE_OPTIONS,
    DELIVERY_METHOD_OPTIONS, WAREHOUSE_OPTIONS, CONTAINER_PICKUP_CARRIER
)

class PrePortDash(View):
    template_main = 'pre_port/dashboard/01_pre_port_summary.html'
    
    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get(tab="summary")
            return await sync_to_async(render)(request, template, context)
        else:
            context = {}
            return await sync_to_async(render)(request, self.template_terminal_dispatch, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        print("step",step)
        #根据建单时间进行筛选       
        if step == "search_orders_by_created_at":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            template, context = await self.handle_all_get(start_date=start_date, end_date=end_date, tab="summary")
            return await sync_to_async(render)(request, template, context)
        #根据ETA进行筛选
        if step == "search_orders_by_eta":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            start_date_ETA = request.POST.get("start_date_ETA")
            end_date_ETA = request.POST.get("end_date_ETA")
            template, context = await self.handle_all_get_ETA(start_date,end_date,start_date_ETA=start_date_ETA, end_date_ETA=end_date_ETA, tab="summary")
            return await sync_to_async(render)(request, template, context)
        if step == "down_eta_file":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            start_date_ETA = request.POST.get("start_date_ETA")
            end_date_ETA = request.POST.get("end_date_ETA")
            return await self.down_eta_file(start_date,end_date,start_date_ETA, end_date_ETA)
        elif step == "empty_return":
            template, context = await self.handle_empty_return_post(request)
            return await sync_to_async(render)(request, template, context)

    async def down_eta_file(self, start_date,end_date,start_date_ETA, end_date_ETA) -> HttpResponse:  
        current_date = datetime.now().date()
        start_date_ETA = current_date.strftime('%Y-%m-%d') if not start_date_ETA else start_date_ETA
        end_date_ETA = (datetime.now().date() + timedelta(days=7)).strftime('%Y-%m-%d') if not end_date_ETA else end_date_ETA
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id", "offload_id", "warehouse"
            ).filter(
                (
                    models.Q(created_at__gte=start_date) &
                    models.Q(created_at__lte=end_date)
                ) & (
                    models.Q(vessel_id__vessel_eta__gte=start_date_ETA) &
                    models.Q(vessel_id__vessel_eta__lte=end_date_ETA)
                )
            ).values(
                "container_number__container_number", "customer_name__zem_code", "retrieval_id__retrieval_destination_area","warehouse__name", "vessel_id__shipping_line","vessel_id__vessel","vessel_id__vessel_eta"
            )
        )
        df = pd.DataFrame(orders)
        # 修改列名为柜号，客户，所属仓/直送地址，具体仓库，ETA，shipping/vessel信息
        df = df.rename(
            {
                "container_number__container_number": "container",
                "customer_name__zem_code": "customer",
                "retrieval_id__retrieval_destination_area":"destination_area",
                "warehouse__name":"warehouse",
                "vessel_id__shipping_line": "shipping_line",
                "vessel_id__vessel": "vessel",
                "vessel_id__vessel_eta": "ETA",
            },
            axis=1
        )
        response = HttpResponse(content_type="text/csv")
        response['Content-Disposition'] = f"attachment; filename=ETA_week.csv"
        df.to_csv(path_or_buf=response, index=False, encoding='utf-8-sig')
        return response
        
    async def handle_all_get(
        self, 
        start_date:str = None,
        end_date: str = None,
        tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (datetime.now().date() + timedelta(days=-7)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = (datetime.now().date() + timedelta(days=7)).strftime('%Y-%m-%d') if not end_date else end_date
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id", "offload_id", "warehouse"
            ).filter(
                models.Q(created_at__gte=start_date) &
                models.Q(created_at__lte=end_date)
            ).order_by("vessel_id__vessel_eta")
        )
        context = {
            "customers": customers,
            "orders": orders,
            "start_date": start_date,
            "end_date": end_date,
            "current_date": current_date,
            "tab": tab,
        }
        return self.template_main, context
    
    async def handle_all_get_ETA(
        self, 
        start_date:str = None,
        end_date:str = None,
        start_date_ETA:str = None,
        end_date_ETA: str = None,
        tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date_ETA = current_date.strftime('%Y-%m-%d') if not start_date_ETA else start_date_ETA
        end_date_ETA = (datetime.now().date() + timedelta(days=7)).strftime('%Y-%m-%d') if not end_date_ETA else end_date_ETA
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id", "offload_id", "warehouse"
            ).filter(
                (
                    models.Q(created_at__gte=start_date) &
                    models.Q(created_at__lte=end_date)
                )&(
                    models.Q(vessel_id__vessel_eta__gte=start_date_ETA) &
                models.Q(vessel_id__vessel_eta__lte=end_date_ETA)
                )              
            ).order_by("vessel_id__vessel_eta")
        )
        context = {
            "customers": customers,
            "orders": orders,
            "start_date":start_date,
            "end_date": end_date,
            "start_date_ETA": start_date_ETA,
            "end_date_ETA": end_date_ETA,
            "current_date": current_date,
            "tab": tab,
        }
        return self.template_main, context
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
