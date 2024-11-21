import pytz
import uuid
import asyncio
import sys
import os
import json
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
from warehouse.models.container import Container
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
    template_inventory_management_main = "post_port/inventory/01_inventory_management_main.html"
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
            template, context = await self.handle_inventory_management_get()
            return render(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "upload_counting_data":
            template, context = await self.handle_upload_counting_data_post(request)
            return render(request, template, context)
        elif step == "confirm_counting":
            template, context = await self.handle_confirm_counting_post(request)
            return render(request, template, context)
        elif step == "download_counting_template":
            return await self.handle_download_counting_template_post()
        elif step == "repalletize":
            template, context = await self.handle_repalletize_post(request)
            return render(request, template, context)
        else:
            pass

    async def handle_counting_get(self) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_counting_main, context

    async def handle_inventory_management_get(self) ->  tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_inventory_management_main, context
    
    async def handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        pallet = await self._get_inventory_pallet(warehouse)
        pallet_json = {
            p.get("plt_ids"): {k: round(v, 2) if isinstance(v, float) else v for k, v in p.items()}
            for p in pallet
        }
        total_cbm = sum([p.get("cbm") for p in pallet])
        total_pallet = sum([p.get("n_pallet") for p in pallet])
        context = {
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options,
            "delivery_method_options": DELIVERY_METHOD_OPTIONS,
            "pallet": pallet,
            "total_cbm": total_cbm,
            "total_pallet": total_pallet,
            "pallet_json": json.dumps(pallet_json)
        }
        return self.template_inventory_management_main, context
    
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

    async def handle_repalletize_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        plt_ids = request.POST.get("plt_ids")
        plt_ids = [int(i) for i in plt_ids.split(",")]
        old_pallet = await sync_to_async(list)(Pallet.objects.filter(id__in=plt_ids))
        container_number = request.POST.get("container")
        container = await sync_to_async(Container.objects.get)(container_number=container_number)
        total_weight = float(request.POST.get("weight"))
        total_cbm = float(request.POST.get("cbm"))
        total_pcs = int(request.POST.get("pcs"))
        warehouse = request.POST.get("warehouse").upper().strip()
        # data of new pallets
        destinations = request.POST.getlist("destination_repalletize")
        delivery_methods = request.POST.getlist("delivery_method_repalletize")
        shipping_marks = request.POST.getlist("shipping_mark_repalletize")
        fba_ids = request.POST.getlist("fba_id_repalletize")
        ref_ids = request.POST.getlist("ref_id_repalletize")
        pcses = request.POST.getlist("pcs_repalletize")
        n_pallets = request.POST.getlist("n_pallet_repalletize")
        notes = request.POST.getlist("note_repalletize")
        pcses = [int(i) for i in pcses]
        n_pallets = [int(i) for i in n_pallets]
        # create new pallets
        new_pallets = []
        for dest, dm, sm, fba, ref, p, n, note in zip(
            destinations, delivery_methods, shipping_marks, fba_ids, ref_ids, pcses, n_pallets, notes
        ):
            new_pallets += [{
                "pallet_id": str(uuid.uuid3(uuid.NAMESPACE_DNS, str(uuid.uuid4()) + dest + dm + str(i))),
                "container_number": container,
                "destination": dest,
                # "address": address,
                # "zipcode": zipcode,
                "delivery_method": dm,
                "pcs": p,
                "cbm": total_cbm * n / total_pcs,
                "weight_lbs": total_weight * n / total_pcs,
                "note": note,
                "shipping_mark": sm if sm else "",
                "fba_id": fba if fba else "",
                "ref_id": ref if ref else "",
                "location": old_pallet[0].get("location"),
            } for i in range(n)]
        # delete old pallets

        raise ValueError(request.POST, plt_ids, n_pallets)
        
    async def _get_inventory_pallet(self, warehouse: str) -> list[Pallet]:
        return await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number", "shipment_batch_number", "container_number__order__customer_name"
            ).filter(
                models.Q(location=warehouse) &
                models.Q(
                    models.Q(shipment_batch_number__isnull=True) |
                    models.Q(shipment_batch_number__is_shipped=False)
                )
            ).annotate(
                str_id=Cast('id', CharField())
            ).values(
                "destination", "delivery_method", "shipping_mark", "fba_id", "ref_id", "note",
                "address", "zipcode",
                customer_name=F("container_number__order__customer_name__zem_name"),
                container=F("container_number__container_number"),
                shipment=F("shipment_batch_number__shipment_batch_number"),
                appointment_id=F("shipment_batch_number__appointment_id")
            ).annotate(
                # shipping_marks=StringAgg("shipping_mark", delimiter=",", distinct=True, ordering="shipping_mark"),
                # fba_ids=StringAgg("fba_id", delimiter=",", distinct=True, ordering="fba_id"),
                # ref_ids=StringAgg("ref_id", delimiter=",", distinct=True, ordering="ref_id"),
                plt_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                weight=Sum("weight_lbs", output_field=FloatField()),
                n_pallet=Count('pallet_id', distinct=True),
            ).order_by("-n_pallet")
        )

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False