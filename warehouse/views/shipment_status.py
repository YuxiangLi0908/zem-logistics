from typing import Any

from django.contrib.auth.decorators import login_required
from django.db import models
from django.db.models import Count, FloatField, IntegerField, Sum
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View

from warehouse.models.packing_list import PackingList


@method_decorator(login_required(login_url="login"), name="dispatch")
class ShipmentStatus(View):
    template_main = "shipment_status/status.html"
    template_search_results = "shipment_status/shipment_details.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        context = {}
        return render(request, self.template_main, context)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "search":
            return render(
                request, self.template_search_results, self.handle_search_post(request)
            )
        else:
            return self.get(self, request)

    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        batch_number = self._process_search_string(
            request.POST.get("batch_number"), to_upper=False
        )
        container_number = self._process_search_string(
            request.POST.get("container_number")
        )
        destination = self._process_search_string(request.POST.get("destination"))
        shipping_mark = self._process_search_string(
            request.POST.get("shipping_mark"), to_upper=False
        )
        fba_id = self._process_search_string(request.POST.get("fba_id"), to_upper=False)
        ref_id = self._process_search_string(request.POST.get("ref_id"), to_upper=False)
        criteria = models.Q()
        if batch_number:
            criteria &= models.Q(
                shipment_batch_number__shipment_batch_number=batch_number
            )
        if container_number:
            criteria &= models.Q(container_number__container_number=container_number)
        if destination:
            criteria &= models.Q(destination=destination)
        if shipping_mark:
            criteria &= models.Q(shipping_mark=shipping_mark)
        if fba_id:
            criteria &= models.Q(fba_id=fba_id)
        if ref_id:
            criteria &= models.Q(ref_id=ref_id)
        packing_list = (
            PackingList.objects.select_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "container_number__order__customer_name",
                "shipment_batch_number",
                "pallet",
            )
            .filter(criteria)
            .values(
                "container_number__order__created_at",
                "container_number__order__customer_name__zem_name",
                "container_number__order__warehouse__name",
                "container_number__container_number",
                "container_number__order__offload_id__offload_at",
                "shipment_batch_number__shipment_batch_number",
                "shipment_batch_number__appointment_id",
                "destination",
                "shipment_batch_number__carrier",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__shipped_at",
                "shipment_batch_number__arrived_at",
                "shipment_batch_number__pod_link",
                "fba_id",
                "shipping_mark",
                "ref_id",
                "note",
            )
            .annotate(
                total_pcs=Sum("pallet__pcs", output_field=IntegerField()),
                total_cbm=Sum("pallet__cbm", output_field=FloatField()),
                total_weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
                total_n_pallet=Count("pallet__pallet_id", distinct=True),
            )
            .order_by("container_number__container_number")
        )
        context = {
            "batch_number": batch_number,
            "container_number": container_number,
            "destination": destination,
            "packing_list": packing_list,
            "shipping_mark": shipping_mark,
            "fba_id": fba_id,
            "ref_id": ref_id,
        }
        return context

    def _process_search_string(self, search_string: str, to_upper=True) -> str:
        try:
            if to_upper:
                return search_string.upper().strip()
            else:
                return search_string.strip()
        except:
            return search_string
