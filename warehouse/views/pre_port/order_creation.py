import uuid
import os
import pandas as pd
import numpy as np
import json
from asgiref.sync import sync_to_async
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any
import pytz
import chardet

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.core.cache import cache
from django.db.models import Count


from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.views.export_file import export_do
from warehouse.models.packing_list import PackingList
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.offload import Offload
from warehouse.models.vessel import Vessel
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    PACKING_LIST_TEMP_COL_MAPPING, SHIPPING_LINE_OPTIONS,
    DELIVERY_METHOD_OPTIONS, ADDITIONAL_CONTAINER,CONTAINER_PICKUP_CARRIER,WAREHOUSE_OPTIONS
)

class OrderCreation(View):
    # template_main = 'pre_port/create_order/01_order_creation_and_management.html'
    template_order_create_base = 'pre_port/create_order/02_base_order_creation_status.html'
    template_order_create_supplement = 'pre_port/create_order/03_order_creation.html'
    template_order_create_supplement_pl_tab = 'pre_port/create_order/03_order_creation_packing_list_tab.html'
    template_order_list = 'order_management/order_list.html'
    template_order_details = 'order_management/order_details.html'
    template_order_details_pl = 'order_management/order_details_pl_tab.html'
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
            template, context = await self.handle_order_basic_info_get()
            return await sync_to_async(render)(request, template, context)            
        elif step == "container_info_supplement":
            template, context = await self.handle_order_supplemental_info_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "order_management_list":
            template, context = await self.handle_order_management_list_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "order_management_container":
            template, context = await self.handle_order_management_container_get(request)
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
        elif step == "update_order_shipping_info":
            template, context = await self.handle_update_order_shipping_info_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_packing_list_info":   
            template, context = await self.handle_update_order_packing_list_info_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_retrieval_info":
            template, context = await self.handle_update_order_retrieval_info_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "upload_template":
            template, context = await self.handle_upload_template_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "download_template":
            return await self.handle_download_template_post()
        elif step == "order_management_search":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            template, context = await self.handle_order_management_list_get(start_date, end_date)
            return await sync_to_async(render)(request, template, context)
        elif step == "export_do":
            return await sync_to_async(export_do)(request)
        elif step == "delete_order":
            template, context = await self.handle_delete_order_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "cancel_notification":
            template, context = await self.handle_cancel_notification(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "export_forecast":
            return await self.handle_export_forecast(request)
            

    async def handle_export_forecast(self, request: HttpRequest) -> tuple[Any, Any]:           
        selected_orders = json.loads(request.POST.get('selectedOrders', '[]'))
        selected_orders = list(set(selected_orders))
        orders = await sync_to_async(list)(
                Order.objects.select_related(
                    "vessel_id", "container_number", "customer_name", "retrieval_id "
                ).values(
                    "container_number__container_number", "customer_name__zem_code", "vessel_id__vessel_eta", "cancel_time", "created_at",
                    "retrieval_id__retrieval_carrier", "vessel_id__destination_port","vessel_id__master_bill_of_lading"
                ).filter(
                    models.Q(container_number__container_number__in=selected_orders)
                ) 
            )
        for order in orders:
            #由于carrier的内容为中文，导出的文件中为乱码，所以修改编码，但是这段代码并没有解决编码问题，依旧是乱码，没有找到解决方案
            if order.get('retrieval_id__retrieval_carrier'):  
                raw_data = order['retrieval_id__retrieval_carrier']
                raw_data = raw_data.encode('utf-8')
                encoding = chardet.detect(raw_data)['encoding']
                order['retrieval_id__retrieval_carrier'] = raw_data.decode(encoding)
            
        df = pd.DataFrame(orders)
        df = df.rename(
            {
                "container_number__container_number": "container",
                "customer_name__zem_code": "customer",
                "vessel_id__master_bill_of_lading":"MBL",
                "vessel_id__destination_port":"destination_port",
                "vessel_id__vessel_eta": "ETA",
                "retrieval_id__retrieval_carrier": "carrier",
            },
            axis=1
        )
        
        response = HttpResponse(content_type="text/csv")
        response['Content-Disposition'] = f"attachment; filename=cancel_notification.csv"
        df.to_csv(path_or_buf=response, index=False, encoding='utf-8-sig')
        return response
            
        
    async def handle_order_basic_info_get(self) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "container_number__packinglist", "retrieval_id "
            ).values(
                "container_number__container_number", "customer_name__zem_name", "vessel_id", "order_type", 
                "retrieval_id__retrieval_destination_area", "packing_list_updloaded","cancel_notification"
            ).filter(
                models.Q(created_at__gte='2024-08-19') |
                models.Q(container_number__container_number__in=ADDITIONAL_CONTAINER)
            )
        )
        unfinished_orders = []
        for o in orders:
            if not o.get("vessel_id") or not o.get("packing_list_updloaded"):
                if not o.get("cancel_notification"):
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
        _, context = await self.handle_order_basic_info_get()
        container_number = request.GET.get("container_number")
        order = await sync_to_async(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "vessel_id",
        ).get)(container_number__container_number=container_number)
        packing_list = await sync_to_async(list)(PackingList.objects.filter(
            models.Q(container_number__container_number=container_number)
        ))
        try:
            vessel = await sync_to_async(Vessel.objects.get)(
                order__container_number__container_number=container_number
            )
        except:
            vessel = []
        if vessel and order.packing_list_updloaded:
            return await self.handle_order_basic_info_get()
        context["selected_order"] = order
        context["packing_list"] = packing_list
        context["vessel"] = vessel
        context["shipping_lines"] = SHIPPING_LINE_OPTIONS
        context["delivery_options"] = DELIVERY_METHOD_OPTIONS
        context["packing_list_upload_form"] = UploadFileForm()
        return self.template_order_create_supplement, context
    
    async def handle_order_management_list_get(self, start_date: str = None, end_date: str = None) -> tuple[Any, Any]:
        start_date = (datetime.now().date() + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = (datetime.now().date() + timedelta(days=30)).strftime('%Y-%m-%d') if not end_date else end_date
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id", "offload_id", "warehouse"
            ).filter(
                models.Q(   #订单列表中显示所有已建单的数据
                    created_at__gte=start_date,
                    created_at__lte=end_date,
                ) 
            )
        )
        context = {
            "orders": orders,
            "start_date": start_date,
            "end_date": end_date,
        }
        return self.template_order_list, context
    
    async def handle_order_management_container_get(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        order = await sync_to_async(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "vessel_id", "warehouse", "offload_id", "shipment_id"
        ).get)(container_number__container_number=container_number)
        packing_list = await sync_to_async(list)(PackingList.objects.filter(
            models.Q(container_number__container_number=container_number)
        ))
        
        context = {
            "selected_order": order,
            "packing_list": packing_list,
            "vessel": order.vessel_id,
            "retrieval":order.retrieval_id,
            "shipping_lines": SHIPPING_LINE_OPTIONS,
            "delivery_options": DELIVERY_METHOD_OPTIONS,
            "packing_list_upload_form": UploadFileForm(),
            "order_type": self.order_type,
            "container_type": self.container_type,
            "customers": customers,
            "area": self.area,
        }
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
        context["warehouse_options"] = [(k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]]
        return self.template_order_details, context

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
            "retrieval_destination_area": area if order_type=="转运" else destination,
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
            "packing_list_updloaded": False,
        }
        order = Order(**order_data)
        await sync_to_async(container.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(offload.save)()
        await sync_to_async(order.save)()
        return await self.handle_order_basic_info_get()
    
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
                retrieval.retrieval_destination_area = request.POST.get("destination").upper().strip()
            else:
                # update retrieval area
                retrieval.retrieval_destination_area = request.POST.get("area")
        else:
            order.order_type = input_order_type
            if original_order_type == "直送":
                # DD to TD
                offload.offload_required = True
                retrieval.retrieval_destination_area = request.POST.get("area")
                order.packing_list_updloaded = False
            else:
                # TD to DD
                offload.offload_required = False
                retrieval.retrieval_destination_area = request.POST.get("destination").upper().strip()
        await sync_to_async(offload.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(container.save)()
        await sync_to_async(order.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container.container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        source = request.POST.get("source")
        if source == "order_management":
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_supplemental_info_get(request)
    
    async def handle_update_order_shipping_info_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        if request.POST.get("is_vessel_created").upper().strip() == "YES":
            vessel = await sync_to_async(Vessel.objects.get)(
                models.Q(order__container_number__container_number=container_number)
            )
            vessel.master_bill_of_lading = request.POST.get("mbl").upper().strip()
            vessel.destination_port = request.POST.get("pod").upper().strip()
            vessel.shipping_line = request.POST.get("shipping_line").strip()
            vessel.vessel = request.POST.get("vessel").upper().strip()
            vessel.voyage = request.POST.get("voyage").upper().strip()
            vessel.vessel_eta = request.POST.get("eta")
            await sync_to_async(vessel.save)()
        else:
            order = await sync_to_async(Order.objects.get)(
                models.Q(container_number__container_number=container_number)
            )
            vessel_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, container_number + request.POST.get("mbl")))
            vessel = Vessel(
                vessel_id=vessel_id,
                master_bill_of_lading=request.POST.get("mbl").upper().strip(),
                destination_port=request.POST.get("pod").upper().strip(),
                shipping_line=request.POST.get("shipping_line"),
                vessel=request.POST.get("vessel").upper().strip(),
                voyage=request.POST.get("voyage").upper().strip(),
                vessel_eta=request.POST.get("eta"),
            )
            await sync_to_async(vessel.save)()
            order.vessel_id = vessel
            await sync_to_async(order.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        source = request.POST.get("source")
        if source == "order_management":
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_supplemental_info_get(request)
    
    async def handle_update_order_retrieval_info_post(self, request:HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(Order.objects.select_related(
            "retrieval_id"
        ).get)(container_number__container_number=container_number)
        retrieval = await sync_to_async(Retrieval.objects.get)(models.Q(retrieval_id = order.retrieval_id))
        retrieval.retrieval_carrier = request.POST.get("retrieval_carrier")
        retrieval.retrieval_destination_precise = request.POST.get("retrieval_destination_precise")
        retrieval.target_retrieval_timestamp = request.POST.get("target_retrieval_timestamp")
        retrieval.actual_retrieval_timestamp = request.POST.get("actual_retrieval_timestamp")
        retrieval.note = request.POST.get("retrieval_note").strip()
        await sync_to_async(retrieval.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        return await self.handle_order_management_container_get(request)

    async def handle_update_order_packing_list_info_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(
            Order.objects.select_related("container_number","offload_id","vessel_id").get
        )(container_number__container_number=container_number)
        container = order.container_number
        offload = order.offload_id
        if offload.offload_at:
            updated_pl = []
            pl_ids = request.POST.getlist("pl_id")
            pl_id_idx_mapping = {int(pl_ids[i]): i for i in range(len(pl_ids))}
            packing_list = await sync_to_async(list)(PackingList.objects.filter(
                container_number__container_number=container_number
            ))
            for pl in packing_list:
                idx = pl_id_idx_mapping[pl.id]
                pl.product_name = request.POST.getlist("product_name")[idx]
                pl.delivery_method = request.POST.getlist("delivery_method")[idx]
                pl.shipping_mark = request.POST.getlist("shipping_mark")[idx]
                pl.fba_id = request.POST.getlist("fba_id")[idx]
                pl.ref_id = request.POST.getlist("ref_id")[idx]
                pl.destination = request.POST.getlist("destination")[idx].upper().strip()
                pl.contact_name = request.POST.getlist("contact_name")[idx]
                pl.contact_method = request.POST.getlist("contact_method")[idx]
                pl.address = request.POST.getlist("address")[idx]
                pl.zipcode = request.POST.getlist("zipcode")[idx]
                pl.note = request.POST.getlist("note")[idx]
                updated_pl.append(pl)
            await sync_to_async(PackingList.objects.bulk_update)(
                updated_pl,
                [
                    "product_name", "delivery_method", "shipping_mark", "fba_id", "ref_id", "destination",
                    "contact_name", "contact_method", "address", "zipcode", "note",
                ]
            )
        else:
            await sync_to_async(PackingList.objects.filter(
                container_number__container_number=container_number
            ).delete)()
            pl_data = zip(
                request.POST.getlist("product_name"),
                request.POST.getlist("delivery_method"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                request.POST.getlist("ref_id"),
                [d.upper().strip() for d in request.POST.getlist("destination")],
                request.POST.getlist("contact_name"),
                request.POST.getlist("contact_method"),
                request.POST.getlist("address"),
                request.POST.getlist("zipcode"),
                request.POST.getlist("pcs"),
                request.POST.getlist("total_weight_kg"),
                request.POST.getlist("total_weight_lbs"),
                request.POST.getlist("cbm"),
                request.POST.getlist("note"),
                strict=True,
            )
            pl_to_create = [
                PackingList(
                    container_number=container,
                    product_name=d[0],
                    delivery_method=d[1],
                    shipping_mark=d[2],
                    fba_id=d[3],
                    ref_id=d[4],
                    destination=d[5],
                    contact_name=d[6],
                    contact_method=d[7],
                    address=d[8],
                    zipcode=d[9],
                    pcs=d[10],
                    total_weight_kg=d[11],
                    total_weight_lbs=d[12],
                    cbm=d[13],
                    note=d[14],
                ) for d in pl_data
            ]
            await sync_to_async(PackingList.objects.bulk_create)(pl_to_create)
            order.packing_list_updloaded = True
            await sync_to_async(order.save)()
        #新建pl的时候，就在po_check表新建记录
        #因为上面已经将新的packing_list存到表里，所以直接去pl表查
        packing_list = await sync_to_async(list)(PackingList.objects.filter(container_number__container_number = container))
        for pl in packing_list:
            try:
                # 直接在查询集中查找是否存在具有相同container_number的对象，如果是建单填写不应该查到pl，如果是更改数据就可能查到
                existing_obj = await sync_to_async(PoCheckEtaSeven.objects.get)(packing_list = pl)  
                #查到了就是更改数据，可能更改唛头、fba、ref
                if existing_obj.shipping_mark != pl.shipping_mark:
                    existing_obj.shipping_mark = pl.shipping_mark
                if existing_obj.fba_id != pl.fba_id:
                    existing_obj.fba_id = pl.fba_id
                if existing_obj.ref_id != pl.ref_id:
                    existing_obj.ref_id = pl.ref_id
                await sync_to_async(existing_obj.save)()
            except PoCheckEtaSeven.DoesNotExist:
                #如果没查到，就建新纪录
                po_check_dict = {
                    'container_number': container,
                    'vessel_eta': order.vessel_id.vessel_eta,
                    'packing_list': pl,
                    'time_status': True,
                    'destination': pl.destination,
                    'fba_id': pl.fba_id,
                    'ref_id': pl.ref_id,
                    'shipping_mark': pl.shipping_mark,
                    #其他的字段用默认值
                }
                new_obj = PoCheckEtaSeven(**po_check_dict)
                await sync_to_async(new_obj.save)()
        source = request.POST.get("source")
        if source == "order_management":
            mutable_get = request.GET.copy()
            mutable_get["container_number"] = container_number
            mutable_get["step"] = "container_info_supplement"
            request.GET = mutable_get
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_basic_info_get()
    
    async def handle_cancel_notification(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        #查询order表的contain_number
        order = await sync_to_async(Order.objects.get)(
                models.Q(container_number__container_number=container_number)
            )    
        order.cancel_notification = True
        order.cancel_time = datetime.now()  
        await sync_to_async(order.save)()
        #如果取消预报了，po_check也要做对应处理，但是怕可能会有取消预报后又不想取消的情况，现在不在po_check表删除，把vessel_eta改成2024/1/2
        orders = await sync_to_async(list)(PoCheckEtaSeven.objects.filter(container_number__container_number = container_number))
        try:
            for o in orders:
                o.vessel_eta = datetime(2024,1,2)
                await sync_to_async(o.save)()
        except PoCheckEtaSeven.DoesNotExist:
            pass
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "cancel_notification"
        request.GET = mutable_get
        return await self.handle_order_management_container_get(request)
        
    async def handle_upload_template_post(self, request: HttpRequest) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            df = df.rename(columns=PACKING_LIST_TEMP_COL_MAPPING)
            df = df.dropna(how="all", subset=[c for c in df.columns if c not in ["delivery_method", "note"]])
            df = df.replace(np.nan, '')
            df = df.reset_index(drop=True)
            if df["cbm"].isna().sum():
                raise ValueError(f"cbm number N/A error!")
            if df["total_weight_lbs"].isna().sum() and df["total_weight_kg"].isna().sum():
                raise ValueError(f"weight number N/A error!")
            if df["pcs"].isna().sum():
                raise ValueError(f"boxes number N/A error!")
            for idx, row in df.iterrows():
                if row["unit_weight_kg"] and not row["unit_weight_lbs"]:
                    df.loc[idx, "unit_weight_lbs"] = round(df.loc[idx, "unit_weight_kg"] * 2.20462, 2)
                if row["total_weight_kg"] and not row["total_weight_lbs"]:
                    df.loc[idx, "total_weight_lbs"] = round(df.loc[idx, "total_weight_kg"] * 2.20462, 2)
            model_fields = [field.name for field in PackingList._meta.fields]
            col = [c for c in df.columns if c in model_fields]
            pl_data = df[col].to_dict("records")
            packing_list = [PackingList(**data) for data in pl_data]
        else:
            raise ValueError(f"invalid file format!")
        source = request.POST.get("source")
        if source == "order_management":
            container_number = request.POST.get("container_number")
            mutable_get = request.GET.copy()
            mutable_get["container_number"] = container_number
            request.GET = mutable_get
            _, context = await self.handle_order_management_container_get(request)
            context["packing_list"] = packing_list
            return self.template_order_details_pl, context
        else:
            _, context = await self.handle_order_supplemental_info_get(request)
            context["packing_list"] = packing_list
            return self.template_order_create_supplement_pl_tab, context
    
    async def handle_download_template_post(self) -> HttpResponse:
        file_path = Path(__file__).parent.parent.parent.resolve().joinpath("templates/export_file/packing_list_template.xlsx")
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, 'rb') as file:
            response = HttpResponse(file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="zem_packing_list_template.xlsx"'
            return response

    async def handle_delete_order_post(self, request: HttpRequest) -> tuple[Any, Any]:           
        selected_orders = json.loads(request.POST.get('selectedOrders', '[]'))
        selected_orders = list(set(selected_orders))
        #在这里进行订单删除操作，例如：
        for order_number in selected_orders:
            await sync_to_async(Order.objects.filter(
                container_number__container_number=order_number
            ).delete)()
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        return await self.handle_order_management_list_get(start_date,end_date)
        

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
