import pandas as pd
from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Count


from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.vessel import Vessel
from warehouse.forms.upload_file import UploadFileForm


class PrePortTracking(View):
    template_t49_tracking = 'pre_port/vessel_terminal_tracking/01_pre_port_tracking.html'
    
    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get()
            return render(request, template, context)
        else:
            context = {}
            return render(request, self.template_t49_tracking, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "t49_tracking_template":
            return await self.download_t49_tracking_file(request)
        elif step == "t49_tracking_upload":
            template, context = await self.handle_t49_tracking_upload_post(request)
            return render(request, template, context)
        
    async def handle_all_get(self) -> tuple[Any, Any]:
        orders_need_track = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "container_number__packinglist",
                "retrieval_id", "offload_id"
            ).values(
                "container_number__container_number", "customer_name__zem_name", "vessel_id", "order_type",
                "vessel_id__vessel_eta", "vessel_id__master_bill_of_lading", "vessel_id__vessel", "vessel_id__voyage",
                "vessel_id__shipping_line"
            ).filter(
                models.Q(vessel_id__isnull=False) &
                models.Q(packing_list_updloaded=True) &
                models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True) &
                models.Q(vessel_id__vessel_eta__lte=datetime.now() + timedelta(weeks=2)) &
                models.Q(add_to_t49=False) &
                models.Q(cancel_notification=False)
            )
        )
        orders_under_tracking = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id", "offload_id"
            ).filter(
                models.Q(add_to_t49=True) &
                models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True) &
                models.Q(cancel_notification=False)
            )
        )
        context = {
            "orders_need_track": orders_need_track,
            "orders_under_tracking": orders_under_tracking,
            "t49_tracking_data": UploadFileForm(),
        }
        return self.template_t49_tracking, context
    
    async def download_t49_tracking_file(self, request: HttpRequest) -> HttpResponse:
        container_number = request.POST.getlist("container_number")
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number"
            ).filter(
                models.Q(container_number__container_number__in=container_number)
            ).values(
                "vessel_id__master_bill_of_lading", "vessel_id__shipping_line", "container_number__container_number"
            )
        )
        df = pd.DataFrame(orders)
        df = df.rename(
            {
                "vessel_id__master_bill_of_lading": "Master Bill of Lading / Booking Number",
                "vessel_id__shipping_line": "Shipping Line",
                "container_number__container_number": "Container Number",
            },
            axis=1
        )
        df["Reference Numbers"] = ""
        df["Shipping Tags"] = ""
        df["Customer"] = ""
        response = HttpResponse(content_type="text/csv")
        response['Content-Disposition'] = f"attachment; filename=Terminal49_SampleCSV_Template.csv"
        df.to_csv(path_or_buf=response, index=False)
        return response

    async def handle_t49_tracking_upload_post(self, request: HttpRequest) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_csv(file)
            orders = await sync_to_async(list)(
                Order.objects.select_related(
                    "vessel_id", "container_number", "retrieval_id",
                ).filter(
                    models.Q(vessel_id__isnull=False) &
                    models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
                )
            )
            t49_container_numbers = df["Container number"].to_list()
            vessels = []
            retrievals = []
            orders_updated = []
            for o in orders:
                if o.container_number.container_number in t49_container_numbers:
                    try:
                        o.vessel_id.vessel_eta = (
                            self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge estimated time of arrival"].values[0])
                            if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge estimated time of arrival"].any()
                            else self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge actual time of arrival"].values[0])
                        )
                    except:
                        pass
                    try:
                        o.vessel_id.origin_port = (
                            df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].values[0]
                            if df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].any()
                            else ""
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.temp_t49_lfd = (
                            self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Current last free day at the POD terminal"].values[0])
                            if df.loc[df["Container number"]==o.container_number.container_number, "Current last free day at the POD terminal"].any()
                            else None
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.temp_t49_available_for_pickup = (
                            True 
                            if df.loc[df["Container number"]==o.container_number.container_number, "Available for pickup"].values[0] == "Yes"
                            else False
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.temp_t49_pod_arrive_at = (
                            self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge arrival time"].values[0], "datetime")
                            if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge arrival time"].any()
                            else None
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.temp_t49_pod_discharge_at = (
                            self._format_string_datetime(df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge discharged event"].values[0], "datetime")
                            if df.loc[df["Container number"]==o.container_number.container_number, "Port of Discharge discharged event"].any()
                            else None
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.temp_t49_hold_status = (
                            True 
                            if df.loc[df["Container number"]==o.container_number.container_number, "Holds at POD status (0)"].values[0] == "Hold"
                            else False
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.master_bill_of_lading =(
                            df.loc[df["Container number"]==o.container_number.container_number, "Shipment number"].values[0]
                            if df.loc[df["Container number"]==o.container_number.container_number, "Shipment number"].any()
                            else None
                        )
                    except:
                        pass
                    try:
                        o.retrieval_id.origin_port = (
                            df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].values[0]
                            if df.loc[df["Container number"]==o.container_number.container_number, "Port of Lading"].any()
                            else None
                        )
                    except:
                        pass
                    o.retrieval_id.destination_port = o.vessel_id.destination_port
                    o.retrieval_id.shipping_line = o.vessel_id.shipping_line
                    o.add_to_t49 = True
                    vessels.append(o.vessel_id)
                    retrievals.append(o.retrieval_id)
                    orders_updated.append(o)
            await sync_to_async(Vessel.objects.bulk_update)(
                vessels,
                ["vessel_eta", "origin_port"]
            )
            await sync_to_async(Retrieval.objects.bulk_update)(
                retrievals,
                [
                    "temp_t49_lfd", "temp_t49_available_for_pickup", "temp_t49_pod_arrive_at", "temp_t49_pod_discharge_at",
                    "temp_t49_hold_status", "master_bill_of_lading", "origin_port", "destination_port", "shipping_line"
                ]
            )
            await sync_to_async(Order.objects.bulk_update)(
                orders_updated, ["add_to_t49"]
            )
            return await self.handle_all_get()
        
    async def _user_authenticate(self, request: HttpRequest) -> bool:
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
    
    def _format_string_datetime(self, datetime_str: str, datetime_part: str = "date") -> str:
        datetime_obj = datetime.fromisoformat(datetime_str)
        if datetime_part == "date":
            return datetime_obj.strftime('%Y-%m-%d')
        else:
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
