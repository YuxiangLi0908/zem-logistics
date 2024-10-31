from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count

from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.warehouse import ZemWarehouse
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
            current_date = datetime.now().date()
            start_date = current_date + timedelta(days=-30)
            end_date = current_date + timedelta(days=30)
            context = {
                "warehouse_form": ZemWarehouseForm(),
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
            }
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
        packling_list = PackingList.objects.select_related(
            "container_number", "container_number__order__customer_name"
        ).filter(
            shipment_batch_number__shipment_batch_number=batch_number
        ).values(
            'id', 'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
            'container_number__container_number',
            'container_number__order__customer_name__zem_name',
            'container_number__order__offload_id__offload_at',
        ).annotate(
            total_pcs=Sum("pallet__pcs", output_field=IntegerField()),
            total_cbm=Sum("pallet__cbm", output_field=FloatField()),
            total_weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
            total_n_pallet=Count('pallet__pallet_id', distinct=True),
        ).order_by(
            'container_number__container_number',
            '-total_weight_lbs'
        )
        context["packing_list"] = packling_list if packling_list else [None]
        return context
    
    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = request.POST.get("name")
        warehouse = None if warehouse=="N/A(直送)" else warehouse
        start_date = request.POST.get("start_date", None)
        end_date = request.POST.get("end_date", None)
        container_number = request.POST.get("container_number", None)
        criteria = models.Q(packinglist__container_number__order__warehouse__name=warehouse)
        if start_date:
            criteria &= models.Q(shipment_schduled_at__gte=start_date)
        if end_date:
            criteria &= models.Q(shipment_schduled_at__lte=end_date)
        if container_number:
            criteria &= (
                models.Q(packinglist__container_number__container_number=container_number) |
                models.Q(order__container_number__container_number=container_number)
            )
        shipment = Shipment.objects.prefetch_related(
            "packinglist", "packinglist__container_number", "packinglist__container_number__order",
            "packinglist__container_number__order__warehouse", "order"
        ).filter(criteria).distinct()
        warehouse_object = ZemWarehouse.objects.get(name=warehouse) if warehouse else None
        warehouse = warehouse if warehouse else "N/A(直送)"
        context = {
            "shipment_list": shipment,
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "name": warehouse,
            "warehouse": warehouse_object,
            "start_date": start_date,
            "end_date": end_date,
            "container_number": container_number,
        }
        return context
    
    def handle_bol_post(self, request: HttpRequest) -> dict[str, Any]:
        batch_number = request.POST.get("batch_number")
        warehouse = request.POST.get("warehouse")
        shipment = Shipment.objects.get(shipment_batch_number=batch_number)
        packing_list = list(PackingList.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
        ))
        pallet = list(Pallet.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
            container_number__order__offload_id__offload_at__isnull=True,
        ).values(
            "container_number__container_number", "destination"
        ).annotate(
            total_cbm=Sum("cbm"),
            total_n_pallet=Count("pallet_id", distinct=True),
        ).order_by("container_number__container_number"))
        pallet += list(PackingList.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
            container_number__order__offload_id__offload_at__isnull=False,
        ).values(
            "container_number__container_number", "destination"
        ).annotate(
            total_cbm=Sum("cbm"),
            total_n_pallet=Sum("cbm")/2,
        ).order_by("container_number__container_number"))
        address_chinese_char = False if shipment.address.isascii() else True
        destination_chinese_char = False if shipment.destination.isascii() else True
        try:
            note_chinese_char = False if shipment.note.isascii() else True
        except:
            note_chinese_char = False
        context = {
            "batch_number": batch_number,
            "warehouse": warehouse,
            "shipment": shipment,
            "packing_list": packing_list,
            "pallet": pallet,
            "address_chinese_char": address_chinese_char,
            "destination_chinese_char": destination_chinese_char,
            "note_chinese_char": note_chinese_char,
        }
        return context
