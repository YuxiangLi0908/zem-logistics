import pytz
import uuid
import asyncio
import sys
import os
import pandas as pd
from PIL import Image
from barcode.writer import ImageWriter
from asgiref.sync import sync_to_async
from datetime import datetime
from pathlib import Path
from typing import Any
from xhtml2pdf import pisa
from itertools import zip_longest
from django.db.models import OuterRef, Subquery

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, Min, FloatField, IntegerField, When, Count, Q
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.packing_list import PackingList
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.views.export_file import export_palletization_list
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS, WAREHOUSE_OPTIONS


class Inventory(View):
    template_counting_main = "post_port/inventory/01_inventory_count_main.html"
    template_inventory_list_and_upload = "post_port/inventory/01_1_inventory_list_and_upload.html"
    template_inventory_list_and_counting = "post_port/inventory/01_2_inventory_list_and_counting.html"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "counting":
            template, context = await self.handle_counting_get()
            return render(request, template, context)
        else:
            pass

    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_counting_warehouse_post(request)
            return render(request, template, context)
        elif step == "upload_counting_data":
            template, context = await self.handle_upload_counting_data_post(request)
            return render(request, template, context)
        elif step == "confirm_counting":
            template, context = await self.handle_confirm_counting_post(request)
            return render(request, template, context)
        elif step == "download_counting_template":
            return await self.handle_download_counting_template_post()
        else:
            pass

    async def handle_counting_get(self) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_counting_main, context
    
    async def handle_counting_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        pallet = await self._get_inventory_pallet(warehouse)
        total_cbm = sum([p.get("cbm") for p in pallet])
        total_pallet = sum([p.get("n_pallet") for p in pallet])
        context = {
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options,
            "pallet": pallet,
            "total_cbm": total_cbm,
            "total_pallet": total_pallet,
            "inventory_counting_file_form": UploadFileForm(),
        }
        return self.template_inventory_list_and_upload, context
    
    async def handle_upload_counting_data_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            _, context = await self.handle_counting_warehouse_post(request)
            pallet = context["pallet"]
            df_sys_inv = pd.DataFrame(pallet)
            df_sys_inv = df_sys_inv.rename(columns={"container_number__container_number": "container_number"})
            df = df.merge(df_sys_inv, on=["container_number", "destination"], how="outer", suffixes=["_act", "_sys"])
            df = df.fillna(0)
            context["inventory_data"] = df.to_dict("records")
            context["total_pallet_cnt"] = df["n_pallet_act"].sum()
            return self.template_inventory_list_and_counting, context
        
    async def handle_confirm_counting_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        container_numbers = request.POST.get("container_number")
        destinations = request.POST.get("destination")
        n_pallet_act = request.POST.get("n_pallet_act")
        n_pallet_sys = request.POST.get("n_pallet_sys")
        pallet_ids = request.POST.get("pallet_ids")
        raise ValueError(request.POST)
        
    async def handle_download_counting_template_post(self) -> HttpResponse:
        file_path = Path(__file__).parent.parent.parent.parent.resolve().joinpath("templates/export_file/inventory_counting_template.xlsx")
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, 'rb') as file:
            response = HttpResponse(file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="inventory_counting_template.xlsx"'
            return response
        
    async def _get_inventory_pallet(self, warehouse: str) -> list[Pallet]:
        return await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number", "shipment_number"
            ).filter(
                models.Q(
                    models.Q(container_number__order__retrieval_id__retrieval_destination_precise=warehouse) |
                    models.Q(container_number__order__warehouse__name=warehouse)
                ) &
                models.Q(
                    models.Q(shipment_number__isnull=True) |
                    models.Q(shipment_number__is_shipped=False)
                )
            ).values(
                "container_number__container_number", "destination", 
            ).annotate(
                pallet_ids=StringAgg("pallet_id", delimiter=",", distinct=True, ordering="pallet_id"),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count('pallet_id', distinct=True),
            )
        )

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False