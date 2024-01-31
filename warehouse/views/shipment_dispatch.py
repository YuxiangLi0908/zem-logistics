import pytz
import uuid
import time
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
        shipment_data = self._get_packing_list_scheduled(warehouse)
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
        
    def _get_packing_list_scheduled(self, warehouse: str) -> PackingList:
        return PackingList.objects.filter(
            models.Q(container_number__order__warehouse__name=warehouse) &
            models.Q(shipment_batch_number__isnull=False) &
            models.Q(shipment_batch_number__is_shipped=False)
        ).values(
            "shipment_batch_number__shipment_batch_number", "shipment_batch_number__destination",
            "shipment_batch_number__carrier", "shipment_batch_number__shipment_appointment"
        ).annotate(
            total_cbm=models.Sum('cbm'),
            total_n_pallet=models.Sum('n_pallet'),
            total_pcs=models.Sum('pcs'),
            total_weight_lbs=models.Sum('total_weight_lbs')
        ).order_by('shipment_batch_number__shipment_appointment')
