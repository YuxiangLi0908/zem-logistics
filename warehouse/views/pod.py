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
    warehouse_options = {"": "", "NJ-07001": "NJ-07001", "NJ-08817": "NJ-08817", "SAV-31326": "SAV-31326","LA-91761":"LA-91761"}

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
        print(shipment_list)
        return context
    
    def handle_select_get(self, request: HttpRequest) -> dict[str, Any]:
        fleet_number = request.GET.get("fleet_number")
        fleet = Fleet.objects.get(fleet_number=fleet_number)
            

        shipment = Shipment.objects.filter(fleet_number=fleet)
        shipment_batch_numbers = [s.shipment_batch_number for s in shipment] 
        packing_list = PackingList.objects.select_related(
            "container_number", "container_number__order__customer_name", "pallet"
        ).filter(
            shipment_batch_number__shipment_batch_number__in=shipment_batch_numbers
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
            "fleet":fleet,
            "shipment": shipment,
            "packing_list": packing_list,
            "upload_file_form": UploadFileForm(required=True),
        }
        return context
    
    def handle_confirm_delivery_post(self, request: HttpRequest) -> dict[str, Any]:
        #先更新时间
        arrived_at = request.POST.get("arrived_at")
        fleet_number = request.POST.get("fleet_number")
        fleet = Fleet.objects.get(fleet_number=fleet_number)
        shipment =list(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        fleet.arrived_at = arrived_at
        updated_shipment = []
        for s in shipment:
            s.arrived_at = arrived_at
            s.is_arrived = True
            updated_shipment.append(s)
        fleet.save()
        Shipment.objects.bulk_update(
            updated_shipment,
            ["arrived_at", "is_arrived"]
        )
        #再上传POD
        conn = self._get_sharepoint_auth()
        pod_form = UploadFileForm(request.POST, request.FILES)
        shipment_batch_number = request.POST.get("shipment_batch_number")
        shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
        if pod_form.is_valid():
            file = request.FILES['file']
            file_extension = os.path.splitext(file.name)[1]
            file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/pod/{APP_ENV}")
            sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
            resp = sp_folder.upload_file(f"{shipment_batch_number}{file_extension}", file).execute_query()
            link = resp.share_link(SharingLinkKind.OrganizationView).execute_query().value.to_json()["sharingLinkInfo"]["Url"]
        else:
            raise ValueError("invalid file uploaded.")
        shipment.pod_link = link
        shipment.pod_uploaded_at = timezone.now()
        shipment.save()
        return self.handle_init_get()

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
            departured_at__isnull=False,
            arrived_at__isnull=True,
            is_canceled=False,
            fleet_type__in=["FTL", "LTL", "外配/快递"]  #LTL和客户自提的不需要确认送达
        )
        if fleet_number:
            criteria &= models.Q(fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment__shipment_batch_number=batch_number)
        if area:
            criteria &= models.Q(origin=area)
        fleet = list(
            Fleet.objects.prefetch_related("shipment")
            .filter(criteria)
            .annotate(
                shipment_batch_numbers=StringAgg("shipment__shipment_batch_number", delimiter=","),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            ).order_by("departured_at")
        )
        fleet_numbers = [f.fleet_number for f in fleet]
        shipment = list(
            Shipment.objects.select_related("fleet_number")
            .filter(fleet_number__fleet_number__in=fleet_numbers)
        )
        shipment_fleet_dict = {}
        for s in shipment:
            if s.shipment_appointment is None:
                shipment_appointment = ""
            else:
                shipment_appointment = s.shipment_appointment.replace(microsecond=0).isoformat()
            if s.fleet_number.fleet_number not in shipment_fleet_dict:
                shipment_fleet_dict[s.fleet_number.fleet_number] = [{
                    "shipment_batch_number": s.shipment_batch_number,
                    "appointment_id": s.appointment_id,
                    "destination": s.destination,
                    "carrier": s.carrier,
                    "shipment_appointment": shipment_appointment,
                    "origin": s.origin,
                }]
            else:
                shipment_fleet_dict[s.fleet_number.fleet_number].append({
                    "shipment_batch_number": s.shipment_batch_number,
                    "appointment_id": s.appointment_id,
                    "destination": s.destination,
                    "carrier": s.carrier,
                    "shipment_appointment": shipment_appointment,
                    "origin": s.origin,
                })
            
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": fleet,
            "warehouse_options": self.warehouse_options,
            "area": area
        }
        return context
    
    def _get_sharepoint_auth(self) -> ClientContext:
        return  ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))