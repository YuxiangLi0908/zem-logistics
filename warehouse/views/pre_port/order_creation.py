import uuid
import ast
import os
import shortuuid
import pandas as pd
import numpy as np
from asgiref.sync import sync_to_async
from pathlib import Path
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.forms import formset_factory
from django.forms.models import model_to_dict
from django.views import View
from django.utils.decorators import method_decorator, sync_and_async_middleware
from django.db import models
from django.core.cache import cache
from django.db.models import Count


from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.order import Order
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.offload import Offload
from warehouse.models.shipment import Shipment
from warehouse.forms.container_form import ContainerForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.forms.order_form import OrderForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.clearance_form import ClearanceSelectForm
from warehouse.forms.offload_form import OffloadForm
from warehouse.forms.retrieval_form import RetrievalForm, RetrievalSelectForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import ORDER_TYPES, PACKING_LIST_TEMP_COL_MAPPING

from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth.decorators import user_passes_test
from functools import wraps
from django.views.decorators.csrf import csrf_exempt


class OrderCreation(View):
    # template_main = 'pre_port/create_order/01_order_creation_and_management.html'
    template_order_create_base = 'pre_port/create_order/02_base_order_creation_status.html'
    template_order_create_supplement = 'pre_port/create_order/03_order_creation.html'
    order_type = {"": "", "转运": "转运", "直送": "直送"}
    area = {"NJ": "NJ", "SAV": "SAV"}
    container_type = {
        '45HQ/GP':'45HQ/GP', '40HQ/GP':'40HQ/GP', '20GP':'20GP', '53HQ':'53HQ'
    }
    
    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_order_basic_info_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "container_info_supplement":
            template, context = await self.handle_order_supplemental_info_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "create_order_basic":
            template, context = await self.handle_create_order_basic_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_basic_info":
            template, context = await self.handle_update_order_basic_info_post(request)
            return await sync_to_async(render)(request, template, context)

    async def handle_order_basic_info_get(self, request: HttpRequest) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related("vessel_id", "container_number", "customer_name", "container_number__packinglist").values(
                "container_number__container_number", "customer_name__zem_name", "vessel_id", "order_type"
            ).annotate(
                n_pl=Count('container_number__packinglist__id', distinct=True),
            )
        )
        unfinished_orders = []
        for o in orders:
            if o.get("order_type") == "直送":
                if not o.get("vessel_id"):
                    unfinished_orders.append(o)
            elif o.get("order_type") == "转运":
                if not o.get("vessel_id") or o.get("n_pl") == 0:
                    unfinished_orders.append(o)
        context = {
            "customers": customers,
            "order_type": self.order_type,
            "area": self.area,
            "container_type": self.container_type,
            "unfinished_orders": unfinished_orders,
        }
        return self.template_order_create_base, context
    
    async def handle_order_supplemental_info_get(self, request: HttpRequest) -> tuple[Any, Any]:
        _, context = await self.handle_order_basic_info_get(request)
        container_number = request.GET.get("container_number")
        order = await sync_to_async(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "vessel_id",
        ).get)(container_number__container_number=container_number)
        packing_list = await sync_to_async(list)(PackingList.objects.filter(
            models.Q(container_number__container_number=container_number)
        ))
        context["selected_order"] = order
        context["packing_list"] = packing_list
        return self.template_order_create_supplement, context
    
    async def handle_create_order_basic_post(self, request: HttpRequest) -> tuple[Any, Any]:
        customer_id = request.POST.get("customer")
        customer = await sync_to_async(Customer.objects.get)(id=customer_id)
        created_at = datetime.now()
        order_type = request.POST.get("order_type")
        area = request.POST.get("area")
        destination = request.POST.get("destination")
        container_number = request.POST.get("container_number")
        if await sync_to_async(list)(Order.objects.filter(container_number__container_number=container_number)):
            raise RuntimeError(f"Container {container_number} exists!")
        weight = float(request.POST.get("weight"))
        weight_unit = request.POST.get("weight_unit")
        if weight_unit == "kg":
            weight *= 2.20462
        is_special_container = True if request.POST.get("is_special_container", None) else False
        order_id = str(uuid.uuid3(
            uuid.NAMESPACE_DNS,
            str(uuid.uuid4())+customer.zem_name+created_at.strftime('%Y-%m-%d %H:%M:%S')
        ))
        retrieval_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + container_number))
        offload_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + order_type))

        container_data = {
            "container_number": request.POST.get("container_number").upper().strip(),
            "container_type": request.POST.get("container_type"),
            "weight_lbs": weight,
            "is_special_container": is_special_container,
            "note": request.POST.get("note"),
        }
        container = Container(**container_data)
        retrieval_data = {
            "retrieval_id": retrieval_id,
            "retrieval_destination_area": area,
        }
        retrieval = Retrieval(**retrieval_data)
        offload_data = {
            "offload_id": offload_id,
            "offload_required": True if order_type=="转运" else False,
        }
        offload = Offload(**offload_data)
        order_data = {
            "order_id": order_id,
            "customer_name": customer,
            "created_at": created_at,
            "order_type": order_type,
            "container_number": container,
            "retrieval_id": retrieval,
            "offload_id": offload,
        }
        order = Order(**order_data)
        await sync_to_async(container.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(offload.save)()
        await sync_to_async(order.save)()
        if order_type == "直送":
            await sync_to_async(PackingList(**{
                "container_number": container,
                "destination": destination.upper().strip(),
                "pcs": 0,
                "total_weight_lbs": weight,
                "cbm": 0,                
                "note": "DD Placeholder",
            }).save)()
        return await self.handle_order_basic_info_get(request)
    
    async def handle_update_order_basic_info_post(self, request: HttpRequest) -> tuple[Any, Any]:
        # check if container number is changed
        input_container_number = request.POST.get("container_number")
        original_container_number = request.POST.get("original_container_number")
        order = await sync_to_async(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "vessel_id", "offload_id"
        ).get)(container_number__container_number=original_container_number)
        container = order.container_number
        retrieval = order.retrieval_id
        offload = order.offload_id
        if input_container_number != original_container_number:
            # check if the input container exists
            new_container = await sync_to_async(list)(Container.objects.filter(container_number=input_container_number))
            if new_container:
                raise ValueError(f"container {input_container_number} exists!")
            else:
                container.container_number = input_container_number
        container.container_type = request.POST.get("container_type")
        container.weight_lbs = request.POST.get("weight")
        container.is_special_container = True if request.POST.get("is_special_container", None) else False
        if not request.POST.get("is_special_container", None):
            container.note = ''
        else:
            container.note = request.POST.get("note")

        # check cunstomer
        input_customer_id = request.POST.get("customer")
        original_customer_id = request.POST.get("original_customer")
        if input_customer_id != original_customer_id:
            order.customer_name = await sync_to_async(Customer.objects.get)(id=input_customer_id)

        # check order_type
        input_order_type = request.POST.get("order_type")
        original_order_type = request.POST.get("original_order_type")
        if input_order_type == original_order_type:
            # order type not changed
            if original_order_type == "直送":
                # update destination
                packing_list = await sync_to_async(PackingList.objects.get)(
                    models.Q(container_number__container_number=original_container_number)
                )
                packing_list.destination = request.POST.get("destination").upper().strip()
                await sync_to_async(packing_list.save)()
            else:
                # update retrieval area
                retrieval.retrieval_destination_area = request.POST.get("area")
        else:
            order.order_type = input_order_type
            if original_order_type == "直送":
                # DD to TD
                packing_list = await sync_to_async(PackingList.objects.get)(
                    models.Q(container_number__container_number=original_container_number)
                )
                offload.offload_required = True
                retrieval.retrieval_destination_area = request.POST.get("area")
                await sync_to_async(packing_list.delete)()
            else:
                # TD to DD
                offload.offload_required = False
                retrieval.retrieval_destination_area = None
                await sync_to_async(PackingList(**{
                    "container_number": container,
                    "destination": request.POST.get("destination").upper().strip(),
                    "pcs": 0,
                    "total_weight_lbs": request.POST.get("weight"),
                    "cbm": 0,                
                    "note": "DD Order Placeholder",
                }).save)()

        await sync_to_async(offload.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(container.save)()
        await sync_to_async(order.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container.container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        return await self.handle_order_supplemental_info_get(request) 
        
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False()
