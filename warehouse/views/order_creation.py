import uuid
import ast
import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.forms import modelformset_factory, formset_factory
from django.forms.models import model_to_dict
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.core.cache import cache

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
from warehouse.forms.clearance_form import ClearanceForm, ClearanceSelectForm
from warehouse.forms.offload_form import OffloadForm
from warehouse.forms.retrieval_form import RetrievalForm, RetrievalSelectForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import ORDER_TYPES, PACKING_LIST_TEMP_COL_MAPPING

@method_decorator(login_required(login_url='login'), name='dispatch')
class OrderCreation(View):
    template_main = 'create_order.html'
    context = {
        "order_form": OrderForm(),
        "warehouse_form": ZemWarehouseForm(),
        "container_form": ContainerForm(),
        "clearance_select_form": ClearanceSelectForm(),
        "retrieval_form": RetrievalForm(),
        "retrieval_select_form": RetrievalSelectForm(),
        "offload_form": OffloadForm(),
        "shipment_form": ShipmentForm(),
        "packing_list_form": formset_factory(PackingListForm, extra=1)
    }
    
    def get(self, request: HttpRequest) -> HttpResponse:
        order_type = request.GET.get("type")
        step = request.GET.get("step", None)
        if step == "download_template":
            return self.handle_download_pl_template_post(request)
        else:
            self.context["step"] = 1
            self.context["order_type"] = ORDER_TYPES[order_type]
            return render(request, self.template_main, self.context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get('step')
        if step == "container_info":
            self.handle_container_post(request)
        elif step == "packing_list":
            if self.handle_packing_list_post(request):
                return redirect("home")
        elif step == "place_order":
            self.handle_place_order_post(request)
            return redirect("home")
        elif step == "upload_template":
            self.handle_upload_pl_template_post(request)
        elif step == "download_template":
            self.handle_download_pl_template_post(request)
        else:
            raise ValueError(f"{request.POST}")
        return render(request, self.template_main, self.context)
    
    def handle_container_post(self, request: HttpRequest) -> None:
        customer = Customer.objects.get(id=request.POST.get("customer_name"))
        if request.POST.get("name") == "N/A":
            warehouse = None
        else:
            warehouse = ZemWarehouse.objects.get(name=request.POST.get("name"))
        eta = request.POST.get("eta")
        clearance_option = request.POST.get("clearance_option")
        retrieval_option = request.POST.get("retrieval_option")
        order_type = request.POST.get("order_type")
        order_id = str(uuid.uuid3(
            uuid.NAMESPACE_DNS,
            str(uuid.uuid4())+customer.zem_name+eta+datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        clear_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + clearance_option))
        retrieval_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + retrieval_option))
        offload_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + order_type))
        
        context = {
            "order_id": order_id,
            "clear_id": clear_id,
            "retrieval_id": retrieval_id,
            "offload_id": offload_id,
        }
        context = context | dict(request.POST.items())
        self.context["step"] = 2
        self.context["order_data"] = context
        self.context["order_type"] = order_type
    
    def handle_packing_list_post(self, request: HttpRequest) -> Any:
        order_type = request.POST.get("order_type")
        container_number = request.POST.get("container_number").strip()
        if self._check_duplicated_container(container_number):
                raise RuntimeError(f"{container_number} exists!")
        if order_type == "直送":
            order_data = ast.literal_eval(request.POST.get("order_data"))
            container_data = dict(request.POST.items())
            container_data["container_number"] = container_number
            order = self._create_order_object(order_data, container_data)
            return redirect("home")
        else:
            self.context["container_data"] = {
                "container_number": container_number,
                "container_type": request.POST.get("container_type"),
                "shipping_line": request.POST.get("shipping_line"),
                "origin": request.POST.get("origin"),
                "destination": request.POST.get("destination"),
                "retrieval_location": request.POST.get("retrieval_location"),
                "shipping_order_number": request.POST.get("shipping_order_number")
            }
            self.context["order_data"] = request.POST.get("order_data")
            self.context["upload_file_form"] = UploadFileForm()
            self.context["packing_list_form"] = formset_factory(PackingListForm, extra=1)
            self.context["step"] = 3

    def handle_place_order_post(self, request: HttpRequest) -> None:
        order_data = ast.literal_eval(request.POST.get("order_data"))
        container_data = ast.literal_eval(request.POST.get("container_data"))
        if self._check_duplicated_container(container_data["container_number"]):
            raise RuntimeError(f"{container_data.get('container_number')} exists!")
        packing_list_formsets = formset_factory(PackingListForm, extra=1)
        packing_list_form = packing_list_formsets(request.POST)
        pl_valid = all([pl.is_valid() for pl in packing_list_form])
        if pl_valid:
            order = self._create_order_object(order_data, container_data)
            container = Container.objects.get(container_number=container_data["container_number"])
            for pl in packing_list_form:
                for k in pl.cleaned_data.keys():
                    if isinstance(pl.cleaned_data[k], str):
                        pl.cleaned_data[k] = pl.cleaned_data[k].strip()
                    if k == "destination":
                        pl.cleaned_data[k] = pl.cleaned_data[k].upper()
                pl.cleaned_data["container_number"] = container
                PackingList.objects.create(**pl.cleaned_data)
        else:
            raise ValueError("invalid packing list!")
    
    def handle_upload_pl_template_post(self, request: HttpRequest) -> None:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            df = df.rename(columns=PACKING_LIST_TEMP_COL_MAPPING)
            df = df.dropna(how="all", subset=[c for c in df.columns if c not in ["delivery_method", "note"]])
            df = df.replace(np.nan, None)
            df = df.reset_index(drop=True)
            for idx, row in df.iterrows():
                if row["unit_weight_kg"] and not row["unit_weight_lbs"]:
                    df.loc[idx, "unit_weight_lbs"] = df.loc[idx, "unit_weight_kg"] * 2.20462
                if row["total_weight_kg"] and not row["total_weight_lbs"]:
                    df.loc[idx, "total_weight_lbs"] = df.loc[idx, "total_weight_kg"] * 2.20462
            model_fields = [field.name for field in PackingList._meta.fields]
            col = [c for c in df.columns if c in model_fields]
            pl_data = df[col].to_dict("records")
            
            packing_list = [PackingList(**data) for data in pl_data]            
            packing_list_formset = formset_factory(PackingListForm, extra=0)
            packing_list_form = packing_list_formset(initial=[model_to_dict(obj) for obj in packing_list])
            self.context["container_data"] = request.POST.get("container_data")
            self.context["order_data"] = request.POST.get("order_data")
            self.context["packing_list_form"] = packing_list_form
            self.context["upload_file_form"] = UploadFileForm()
            self.context["step"] = 3
            cache.clear()
        else:
            raise ValueError(f"invalid file format!")
        
    def handle_download_pl_template_post(self, request: HttpRequest) -> HttpResponse:
        file_path = Path(__file__).parent.parent.resolve().joinpath("templates/export_file/packing_list_template.xlsx")
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, 'rb') as file:
            response = HttpResponse(file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="zem_packing_list_template.xlsx"'
            return response

    def _create_model_object(self, model: models.Model, data: dict[str, Any], save: bool = True) -> models.Model:
        model_object = model(**data)
        if save:
            model_object.save()
        return model_object
    
    def _create_clearance_object(self, order_data: dict[str, Any]) -> Clearance:
        clearance_data = {
            "clearance_id": order_data["clear_id"],
            "is_clearance_required": False if order_data["clearance_option"]=="N/A" else True,
            "clear_by_zem": True if order_data["clearance_option"]=="代理清关" else False,
        }
        return self._create_model_object(Clearance, clearance_data)
    
    def _create_retrieval_object(
        self,
        order_data: dict[str, Any],
        container_data: dict[str, Any]
    ) -> Retrieval:
        retrieval_data = {
            "retrieval_id": order_data["retrieval_id"],
            "retrive_by_zem": True if order_data["retrieval_option"]=="代理卡车" else False,
            "origin": container_data.get("origin"),
            "destination": container_data.get("destination"),
            "retrieval_location": container_data.get("retrieval_location"),
            "shipping_line": container_data.get("shipping_line"),
            "shipping_order_number": container_data.get("shipping_order_number")
        }
        return self._create_model_object(Retrieval, retrieval_data)
    
    def _create_offload_obejct(self, order_data: dict[str, Any]) -> Offload:
        offload_data = {
            "offload_id": order_data["offload_id"],
            "offload_required": True if order_data["order_type"]=="转运" else False,
        }
        return self._create_model_object(Offload, offload_data)
    
    def _create_container_object(self, container_data: dict[str, Any]) -> Container:
        container_data = {
            "container_number": container_data["container_number"],
            "container_type": container_data["container_type"],
        }
        return self._create_model_object(Container, container_data)
    
    def _creat_shipment_object(self, order_data: dict[str, Any], container_data: dict[str, Any]) -> Shipment:
        shipment_id = str(uuid.uuid3(
            uuid.NAMESPACE_DNS,
            str(uuid.uuid4())+order_data.get("customer_name")+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+container_data.get("container_number")
        ))
        shipment_data = {
            "shipment_batch_number": shipment_id,
            "address": container_data.get("address"),
            "destination": container_data.get("destination"),
        }
        return self._create_model_object(Shipment, shipment_data)
    
    def _create_order_object(
        self,
        order_data: dict[str, Any],
        container_data: dict[str, Any]
    ) -> Order:
        if order_data.get("order_type", None) == "直送":
            shipment = self._creat_shipment_object(order_data, container_data)
        else:
            shipment = None
        clearance = self._create_clearance_object(order_data)
        retrieval = self._create_retrieval_object(order_data, container_data)
        offload = self._create_offload_obejct(order_data)
        container = self._create_container_object(container_data)
        if order_data.get("name") == "N/A":
            warehouse = None
        else:
            warehouse = ZemWarehouse.objects.get(name=order_data.get("name"))
        order_data = {
            "order_id": order_data["order_id"],
            "customer_name": Customer.objects.get(id=order_data.get("customer_name")),
            "warehouse": warehouse,
            "created_at": datetime.now(),
            "eta": order_data["eta"],
            "order_type": order_data["order_type"],
            "container_number": container,
            "clearance_id": clearance,
            "retrieval_id": retrieval,
            "offload_id": offload,
            "shipment_id": shipment,
        }
        return self._create_model_object(Order, order_data)
    
    def _check_duplicated_container(self, container_number: str) -> bool:
        return Container.objects.filter(container_number=container_number).exists()