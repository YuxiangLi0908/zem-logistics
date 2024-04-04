from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.forms.shipment_form import ShipmentForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class ShipmentDispatch(View):
    template_main = 'outbound.html'

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "batch":
            return self.handle_batch_get(request)
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_main, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "warehouse":
            return self.handle_warehouse_post(request)
        elif step =="confirm":
            return self.handle_confirm_post(request)
        else:
            raise ValueError(f"{request.POST}")

    def handle_warehouse_post(self, request: HttpRequest) -> HttpResponse:
        warehouse = request.POST.get("name")
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        warehouse = None if warehouse=="N/A(直送)" else warehouse
        shipment_data = self._get_shipment(warehouse)
        shipment_list = []
        for s in shipment_data:
            shipment_list.append((s, ShipmentForm()))
        context = {
            "warehouse_form": warehouse_form,
            "shipment_list": shipment_list,
            "warehouse": warehouse,
        }
        return render(request, self.template_main, context)
    
    def handle_confirm_post(self, request: HttpRequest) -> HttpResponse:
        batch_id = request.POST.get("batch_id")
        shipped_at = request.POST.get("shipped_at")
        shipment_batch = Shipment.objects.get(shipment_batch_number=batch_id)
        shipment_batch.shipped_at = shipped_at
        shipment_batch.is_shipped = True
        shipment_batch.save()
        mutable_post = request.POST.copy()
        mutable_post["name"] = request.POST.get("name")
        request.POST = mutable_post
        
        return self.handle_warehouse_post(request)
        
    def _get_shipment(self, warehouse: str) -> Shipment:
        return Shipment.objects.filter(
            models.Q(origin=warehouse)&
            models.Q(is_shipped=False)
        ).order_by('shipment_appointment')
