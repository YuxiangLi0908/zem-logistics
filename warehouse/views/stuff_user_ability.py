import uuid

from typing import Any
from datetime import datetime
import pandas as pd

from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.db import models

from warehouse.models.order import Order
from warehouse.models.vessel import Vessel
from warehouse.models.offload import Offload
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.packing_list import PackingList
from warehouse.models.shipment import Shipment
from warehouse.forms.upload_file import UploadFileForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class StuffPower(View):
    template_1 = "stuff_user_clean_data.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_staff:
            return HttpResponseForbidden("You don't have permission to access this page.")
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
        elif step == "update_delivery_method":
            cnt = self._update_delivery_method()
            context = {
                "delivery_update_success": True,
                "count": cnt,
            }
            return render(request, self.template_1, context)
        elif step == "pre_port_update_vessel_pl_data":
            template, context = self.update_vessel_pl_data(request)
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
            models.Q(order__isnull=True) &
            models.Q(packinglist__isnull=True)
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
        PackingList.objects.bulk_update(pl, ["total_weight_kg"])
        return invalid_cases
    
    def _update_delivery_method(self) -> int:
        pl = PackingList.objects.all()
        cnt = 0
        for p in pl:
            if p.delivery_method == "暂扣留仓":
                p.delivery_method = "暂扣留仓(HOLD)"
                cnt += 1
        PackingList.objects.bulk_update(pl, ["delivery_method"])
        return cnt
    
    def update_vessel_pl_data(self, request: HttpRequest) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        orders = Order.objects.select_related(
            "customer_name", "container_number", "vessel_id", "retrieval_id"
        ).filter(
            created_at__gte="2024-07-01"
        )
        order_pl_count = Order.objects.select_related(
            "container_number"
        ).filter(
            models.Q(created_at__gte="2024-07-01") &
            models.Q(order_type="转运")
        ).values(
            "container_number__container_number"
        ).annotate(
            n_pl=models.Count("container_number__packinglist__id", distinct=True),
        )
        order_pl_count = {o["container_number__container_number"]: o["n_pl"] for o in order_pl_count}
        context = {
            "vessel_pl_data_update_success": False,
            "orders_count": 0,
        }
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_csv(file)
            df = df.fillna("")
            t49_container_numbers = df["Container number"].to_list()
            updated_orders = []
            updated_retrievals = []
            cnt = 0
            for o in orders:
                if o.container_number.container_number in t49_container_numbers:
                    mbl = df.loc[df["Container number"]==o.container_number.container_number, "Shipment number"].values[0]
                    vessel_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, o.container_number.container_number + mbl))
                    if not o.vessel_id:
                        vessel = Vessel.objects.create(
                            vessel_id=vessel_id,
                            master_bill_of_lading=mbl,
                            origin_port = (
                                df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].values[0]
                                if df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].any()
                                else ""
                            ),
                            destination_port=o.retrieval_id.destination_port,
                            shipping_line=df.loc[df["Container number"]==o.container_number.container_number, "Carrier name"].values[0],
                            vessel=df.loc[df["Container number"]==o.container_number.container_number, "Vessel name"].values[0],
                            voyage=df.loc[df["Container number"]==o.container_number.container_number, "Voyage number"].values[0],
                            vessel_eta=(
                                self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge estimated time of arrival"].values[0])
                                if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge estimated time of arrival"].any()
                                else self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge actual time of arrival"].values[0])
                            ),
                        )
                        vessel.save()
                        o.vessel_id = vessel
                        o.add_to_t49 = True
                    retrieval = o.retrieval_id
                    retrieval.temp_t49_lfd = (
                        self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Current last free day at the POD terminal"].values[0])
                        if df.loc[df["Container number"]==o.container_number.container_number, "Current last free day at the POD terminal"].any()
                        else None
                    )
                    retrieval.temp_t49_available_for_pickup = (
                        True 
                        if df.loc[df["Container number"]==o.container_number.container_number, "Available for pickup"].values[0] == "Yes"
                        else False
                    )
                    retrieval.temp_t49_pod_arrive_at = (
                        self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge arrival time"].values[0], "datetime")
                        if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge arrival time"].any()
                        else None
                    )
                    retrieval.temp_t49_pod_discharge_at = (
                        self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge discharged event"].values[0], "datetime")
                        if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge discharged event"].any()
                        else None
                    )
                    retrieval.temp_t49_hold_status = (
                        True 
                        if df.loc[df["Container number"]==o.container_number.container_number, "Holds at POD status (0)"].values[0] == "Hold"
                        else False
                    )
                    retrieval.master_bill_of_lading = (
                        df.loc[df["Container number"]==o.container_number.container_number, "Shipment number"].values[0]
                        if df.loc[df["Container number"]==o.container_number.container_number, "Shipment number"].any()
                        else None
                    )
                    retrieval.origin_port = (
                        df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].values[0]
                        if df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].any()
                        else None
                    )
                    retrieval.shipping_line = o.vessel_id.shipping_line
                    updated_retrievals.append(retrieval)
                    cnt += 1
                if o.order_type == "直送" or order_pl_count.get(o.container_number.container_number, 0) > 0:
                    o.packing_list_updloaded == True
                updated_orders.append(o)
            Order.objects.bulk_update(
                updated_orders, ["add_to_t49", "packing_list_updloaded", "vessel_id"]
            )
            Retrieval.objects.bulk_update(
                updated_retrievals,
                [
                    "temp_t49_lfd", "temp_t49_available_for_pickup", "temp_t49_pod_arrive_at", "temp_t49_pod_discharge_at",
                    "temp_t49_hold_status", "master_bill_of_lading", "origin_port", "shipping_line"
                ]
            )
            context["vessel_pl_data_update_success"] = True
            context["orders_count"] = cnt
        return self.template_1, context
            
    
    def _format_string_datetime(self, datetime_str: str, datetime_part: str = "date") -> str|None:
        if not datetime_str:
            return None
        datetime_obj = datetime.fromisoformat(datetime_str)
        if datetime_part == "date":
            return datetime_obj.strftime('%Y-%m-%d')
        else:
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
