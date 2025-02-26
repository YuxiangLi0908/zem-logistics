import os
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count
from django.contrib.postgres.aggregates import StringAgg
from django.forms.models import model_to_dict

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.fleet import Fleet
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.upload_file import UploadFileForm
from django.utils import timezone
from warehouse.utils.constants import APP_ENV, SP_USER, SP_PASS, SP_URL, SP_DOC_LIB, SYSTEM_FOLDER


@method_decorator(login_required(login_url='login'), name='dispatch')
class POD(View):
    template_main = "pod/shipment_list.html"
    template_shipment_detail = "pod/shipment_detail.html"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761":"LA-91761",
        "MO-62025":"MO-62025",
        "HX-77503":"HX-77503"
    }

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "select":
            return render(request, self.template_shipment_detail, self.handle_select_get(request))
        else:
            return render(request, self.template_main, self.handle_delivery_and_pod_get(request))

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "confirm_delivery":
            return render(request, self.template_main, self.handle_confirm_delivery_post(request))
        elif step == "fleet_delivery_search":
            context = self.handle_delivery_and_pod_get(request)
            return render(request, self.template_main, context)
        context = {}
        return render(request, self.template_main, context)

    def handle_init_get(self) -> dict[str, Any]:
        shipment_list = self._get_not_delivered_shipment()
        context = {
            "shipment_list": shipment_list,
            "warehouse_options": self.warehouse_options,
        }
        return context
    
    def handle_select_get(self, request: HttpRequest) -> dict[str, Any]:
        area = request.GET.get("area")
        batch_number = request.GET.get("batch_number")
        shipment = Shipment.objects.get(shipment_batch_number=batch_number)
        shipment_form = ShipmentForm(initial=model_to_dict(shipment))
        packing_list = PackingList.objects.select_related(
            "container_number", "container_number__order__customer_name", "pallet"
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
        context = {
            "shipment": shipment,
            "packing_list": packing_list,
            "shipment_form": shipment_form,
            "upload_file_form": UploadFileForm(required=True),
            "area":area
        }
        return context
    
    def handle_confirm_delivery_post(self, request: HttpRequest) -> dict[str, Any]:
        conn = self._get_sharepoint_auth()
        pod_form = UploadFileForm(request.POST, request.FILES)
        arrived_at = request.POST.get("arrived_at")
        batch_number = request.POST.get("batch_number")
        if pod_form.is_valid():
            file = request.FILES['file']
            file_extension = os.path.splitext(file.name)[1]
            file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/pod/{APP_ENV}")
            sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
            resp = sp_folder.upload_file(f"{batch_number}{file_extension}", file).execute_query()
            link = resp.share_link(SharingLinkKind.OrganizationView).execute_query().value.to_json()["sharingLinkInfo"]["Url"]
        else:
            raise RuntimeError("invalid file uploaded.")
        shipment = Shipment.objects.get(shipment_batch_number=batch_number)
        shipment.pod_link = link
        shipment.arrived_at = arrived_at
        shipment.is_arrived = True
        shipment.save()
        return self.handle_delivery_and_pod_get(request)

    def _get_not_delivered_shipment(self) -> Shipment:
        return Shipment.objects.filter(
            models.Q(is_shipped=True) &
            models.Q(is_arrived=False)
        ).order_by("shipped_at")
    
    def handle_delivery_and_pod_get(self, request: HttpRequest) ->  dict[str, Any]:
        fleet_number = request.GET.get("fleet_number", "")
        batch_number = request.GET.get("batch_number", "")

        area = request.POST.get('area') or None
        
        criteria = models.Q(
            shipped_at__isnull=False,
            arrived_at__isnull=True,
            shipment_schduled_at__gte='2024-12-01'
        )       
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        if area and area is not None and area != 'None':
            criteria &= models.Q(origin=area)
        shipment = list(
            Shipment.objects.select_related("fleet_number").filter(criteria).order_by("shipped_at")
        )
            
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": shipment,
            "warehouse_options": self.warehouse_options,
            "area": area
        }
        return context
    
    def _get_sharepoint_auth(self) -> ClientContext:
        return  ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))