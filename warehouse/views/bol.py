import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.customer import Customer
from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.customer_form import CustomerForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_bol

@method_decorator(login_required(login_url='login'), name='dispatch')
class BOL(View):
    template_main = "bol.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "select":
            return render(request, self.template_main, self.handle_select_get(request))
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
        return render(request, self.template_main, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "search":
            return render(request, self.template_main, self.handle_search_post(request))
        elif step == "export_bol":
            return export_bol(self.handle_bol_post(request))
        raise ValueError(f"{request.POST}")
    
    def handle_select_get(self, request: HttpRequest) -> dict[str, Any]:
        request.POST = request.GET
        batch_number = request.GET.get("batch_number")
        context = self.handle_search_post(request)
        context['shipment_list'] = context['shipment_list'].filter(shipment_batch_number=batch_number)
        packling_list = PackingList.objects.filter(
            shipment_batch_number__shipment_batch_number=batch_number
        ).values(
            'id', 'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
            'container_number__container_number',
            'container_number__order__customer_name__zem_name',
            'container_number__order__offload_id__offload_at',
        ).annotate(
            total_cbm=models.Sum('cbm'),
            total_n_pallet=models.Sum('n_pallet'),
            total_pcs=models.Sum('pcs'),
            total_weight_lbs=models.Sum('total_weight_lbs')
        ).order_by(
            'container_number__container_number',
            '-total_weight_lbs'
        )
        context["packing_list"] = packling_list
        return context
    
    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = request.POST.get("name")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        criteria = models.Q(packinglist__container_number__order__warehouse__name=warehouse)
        if start_date:
            criteria &= models.Q(shipment_schduled_at__gte=start_date)
        if end_date:
            criteria &= models.Q(shipment_schduled_at__lte=end_date)
        shipment = Shipment.objects.filter(criteria).distinct()
        context = {
            "shipment_list": shipment,
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "name": warehouse,
            "warehouse": ZemWarehouse.objects.get(name=warehouse),
            "start_date": start_date,
            "end_date": end_date,
        }
        return context
    
    def handle_bol_post(self, request: HttpRequest) -> dict[str, Any]:
        batch_number = request.POST.get("batch_number")
        warehouse = request.POST.get("warehouse")
        shipment = Shipment.objects.get(shipment_batch_number=batch_number)
        packing_list = PackingList.objects.filter(shipment_batch_number__shipment_batch_number=batch_number)
        context = {
            "batch_number": batch_number,
            "warehouse": warehouse,
            "shipment": shipment,
            "packing_list": packing_list,
        }
        return context

