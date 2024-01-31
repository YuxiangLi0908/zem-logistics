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
class ScheduleShipment(View):
    template_main = 'schedule_shipment.html'

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "destination":
            return self.handle_destination_get(request)
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_main, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "warehouse":
            return self.handle_warehouse_post(request)
        elif step == "appointment":
            return self.handle_appointment_post(request)
        else:
            raise ValueError(f"{request.POST}")
    
    def handle_destination_get(self, request: HttpRequest) -> HttpResponse:
        warehouse = request.GET.get("warehouse")
        destination = request.GET.get("destination")
        packing_list = PackingList.objects.filter(
            container_number__order__warehouse__name=warehouse,
            destination=destination,
            n_pallet__isnull=False,
            shipment_batch_number__isnull=True,
        )
        packing_list = packing_list.values(
            'id', 'delivery_method', 'shipping_mark', 'fba_id', 'ref_id', 'destination',
            'pcs', 'total_weight_lbs', 'cbm', 'n_pallet',
            'container_number__container_number',
            customer_name=models.F('container_number__order__customer_name__zem_name'),
        ).order_by('-n_pallet')
        order_packing_list = []
        for pl in packing_list:
            order_packing_list.append((pl, ShipmentForm()))
        context = {
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "packing_list_not_scheduled": self._get_packing_list_not_scheduled(warehouse),
            "order_packing_list": order_packing_list,
            "ids": [pl["id"] for pl in packing_list],
        }
        return render(request, self.template_main, context)
    
    def handle_warehouse_post(self, request: HttpRequest) -> HttpResponse:
        warehouse = request.POST.get("name")
        packing_list_not_scheduled = self._get_packing_list_not_scheduled(warehouse)
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        context = {
            "warehouse_form": warehouse_form,
            "packing_list_not_scheduled": packing_list_not_scheduled,
        }
        return render(request, self.template_main, context)
    
    def handle_appointment_post(self, request: HttpRequest) -> HttpResponse:
        shipment_appointment = request.POST.getlist("shipment_appointment")
        carrier = request.POST.getlist("carrier")
        ids = request.POST.get("ids").strip('][').split(', ')
        warehouse = request.GET.get("warehouse")
        destination = request.GET.get("destination")
        
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn)
        appointment_batch_mapping = {}
        for appointment, c in zip(shipment_appointment, carrier):
            batch_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(uuid.uuid4()) + appointment + c + str(time.time()))
            appointment_batch_mapping[appointment + c] = str(batch_id)
        for appointment, c, id in zip(shipment_appointment, carrier, ids):
            pl = PackingList.objects.get(id=id)
            if appointment:
                try:
                    shipment = Shipment.objects.get(shipment_batch_number=appointment_batch_mapping[appointment + c])
                except:
                    Shipment.objects.create(**{
                        "shipment_batch_number": appointment_batch_mapping[appointment + c],
                        "origin": warehouse,
                        "destination": destination,
                        "address": pl.address,
                        "carrier": c,
                        "is_shipment_schduled": True,
                        "shipment_appointment": appointment,
                        "shipment_schduled_at": current_time_cn,
                    })
                    shipment = Shipment.objects.get(shipment_batch_number=appointment_batch_mapping[appointment + c])
                pl.shipment_batch_number = shipment
                pl.save()
            else:
                pl.shipment_batch_number = None
                pl.save()

        mutable_post = request.POST.copy()
        mutable_post['name'] = warehouse
        request.POST = mutable_post
        return self.handle_warehouse_post(request)

    def _get_packing_list_not_scheduled(self, warehouse: str) -> PackingList:
        return PackingList.objects.filter(
            models.Q(container_number__order__warehouse__name=warehouse) &
            models.Q(n_pallet__isnull=False) &
            models.Q(shipment_batch_number__isnull=True)
        ).values('destination').annotate(
            total_cbm=models.Sum('cbm'),
            total_n_pallet=models.Sum('n_pallet'),
            total_pcs=models.Sum('pcs'),
            total_weight_lbs=models.Sum('total_weight_lbs')
        ).order_by('-total_n_pallet')