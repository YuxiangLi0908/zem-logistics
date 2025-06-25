import random
import re
import string
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from django.contrib.auth.decorators import login_required
from django.db import models
from django.db.models import Count, FloatField, IntegerField, Sum
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
from simple_history.utils import bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.clearance import Clearance
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import DELIVERY_METHOD_CODE


@method_decorator(login_required(login_url="login"), name="dispatch")
class StuffPower(View):
    template_1 = "stuff_user_clean_data.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_staff:
            return HttpResponseForbidden(
                "You don't have permission to access this page."
            )
        context = {
            "pre_port_t49_tracking": UploadFileForm(),
        }
        return render(request, self.template_1, context)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "update_pl_weight_kg":
            invalid_cases = self._update_pl_weight_kg_20240410()
            context = {
                "pl_update_success": True,
                "invalid_cases": invalid_cases,
            }
            return render(request, self.template_1, context)
        elif step == "backfill_master_shipment":
            template, context = self.backfill_master_shipment(request)
            return render(request, template, context)
        else:
            self._remove_offload()
            self._remove_clearance()
            self._remove_retrieval()
            self._remove_shipment()
            context = {"success": True}
            return render(request, self.template_1, context)

    def _remove_offload(self) -> None:
        Offload.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_clearance(self) -> None:
        Clearance.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_retrieval(self) -> None:
        Retrieval.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_shipment(self) -> None:
        Shipment.objects.filter(
            models.Q(order__isnull=True) & models.Q(packinglist__isnull=True)
        ).delete()

    def _update_pl_weight_kg_20240410(self):
        invalid_cases = []
        pl = PackingList.objects.all()
        for p in pl:
            try:
                p.total_weight_kg = round(p.total_weight_lbs / 2.20462, 2)
            except:
                p.total_weight_kg = 0
                invalid_cases.append(p)
        bulk_update_with_history(pl, PackingList, fields=["total_weight_kg"])
        return invalid_cases

    def backfill_master_shipment(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = Pallet.objects.select_related("container_number").filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=False,
            shipment_batch_number__isnull=False,
            master_shipment_batch_number__isnull=True,
        )
        packinglist = PackingList.objects.select_related("container_number").filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
            shipment_batch_number__isnull=False,
            master_shipment_batch_number__isnull=True,
        )
        cnt = 0
        for p in pallet:
            p.master_shipment_batch_number = p.shipment_batch_number
            cnt += 1
        for p in packinglist:
            p.master_shipment_batch_number = p.shipment_batch_number
        bulk_update_with_history(
            pallet,
            Pallet,
            fields=["master_shipment_batch_number"],
        )
        bulk_update_with_history(
            packinglist,
            PackingList,
            fields=["master_shipment_batch_number"],
        )
        context = {
            "start_date": start_date,
            "end_date": end_date,
            "count": cnt,
            "master_shipment_updated": True,
        }
        return self.template_1, context

    def _format_string_datetime(
        self, datetime_str: str, datetime_part: str = "date"
    ) -> str | None:
        if not datetime_str:
            return None
        datetime_obj = datetime.fromisoformat(datetime_str)
        if datetime_part == "date":
            return datetime_obj.strftime("%Y-%m-%d")
        else:
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
