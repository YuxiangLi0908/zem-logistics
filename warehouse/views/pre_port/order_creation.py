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
from warehouse.views.export_file import export_do
from warehouse.models.packing_list import PackingList
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.offload import Offload
from warehouse.models.vessel import Vessel
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    PACKING_LIST_TEMP_COL_MAPPING, SHIPPING_LINE_OPTIONS,
    DELIVERY_METHOD_OPTIONS, ADDITIONAL_CONTAINER
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

    async def handle_order_basic_info_get(self) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related("vessel_id", "container_number", "customer_name", "container_number__packinglist").values(
                "container_number__container_number", "customer_name__zem_name", "vessel_id", "order_type",
                "packing_list_updloaded"
            ).filter(
                models.Q(created_at__gte='2024-08-19') |
                models.Q(container_number__container_number__in=ADDITIONAL_CONTAINER)
            )
        )
        unfinished_orders = []
        for o in orders:
            if not o.get("vessel_id") or not o.get("packing_list_updloaded"):
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
                models.Q(
                    vessel_id__vessel_eta__gte=start_date,
                    vessel_id__vessel_eta__lte=end_date,
                    packing_list_updloaded=True,
                ) |
                models.Q(
                    eta__gte=start_date,
                    eta__lte=end_date,
                    packing_list_updloaded=True,
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
            "shipping_lines": SHIPPING_LINE_OPTIONS,
            "delivery_options": DELIVERY_METHOD_OPTIONS,
            "packing_list_upload_form": UploadFileForm(),
            "order_type": self.order_type,
            "container_type": self.container_type,
            "customers": customers,
            "area": self.area,
        }
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
    
    async def handle_update_order_packing_list_info_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(Order.objects.get)(container_number__container_number=container_number)
        container = await sync_to_async(Container.objects.get)(container_number=container_number)
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
        source = request.POST.get("source")
        if source == "order_management":
            mutable_get = request.GET.copy()
            mutable_get["container_number"] = container_number
            mutable_get["step"] = "container_info_supplement"
            request.GET = mutable_get
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_basic_info_get()
    
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
        raise ValueError(request.POST)

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
