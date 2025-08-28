import openpyxl
import re
import json
import pandas as pd
import ast
import uuid
import random
import string
from typing import Any, Tuple
from collections import defaultdict
from datetime import datetime, date
from ast import literal_eval
from asgiref.sync import sync_to_async
from django.contrib.auth.models import User
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import CharField, Count, F, FloatField, IntegerField, Sum
from django.db.models.functions import Cast
from django.http import Http404, HttpRequest, HttpResponse, HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from itertools import groupby
from django.views.decorators.csrf import csrf_protect
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from django.template.loader import get_template
from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.fleet import Fleet
from warehouse.models.transfer_location import TransferLocation
from warehouse.views.export_file import link_callback
from xhtml2pdf import pisa
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS


class TransferPallet(View):
    template_transfer_pallet = (
        "post_port/transfer_pallet/01_transfer_pallet.html"
    )
    template_transfer_history = (
        "post_port/transfer_pallet/01_transfer_history.html"
    )
    template_transfer_bol = "export_file/bol_transfer_template.html"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "operation":
            template, context = await self.handle_operation_get()
            return render(request, template, context)
        elif step == "transfer_history":
            template, context = await self.handle_transfer_history_post(request)
            return render(request, template, context)
        else:
            raise ValueError('step值错误')

    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "transfer_warehouse":
            template, context = await self.handle_transfer_warehouse_post(request)
            return render(request, template, context)
        elif step =="history_warehouse":
            template, context = await self.handle_transfer_history_warehouse_post(request)
            return render(request, template, context)
        elif step =="confirm_arrived":
            template, context = await self.handle_transfer_confirm_arrived_post(request)
            return render(request, template, context)
        elif step == "export_bol_transfer":
            return await self.handle_export_bol_transfer_post(request)
        

    async def handle_transfer_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        shipping_warehouse = request.POST.get("warehouse")
        receiving_warehouse = request.POST.get("receiving_warehouse")
        pickup_number = request.POST.get("pickup_number",None)
        shipping_time = request.POST.get("shipping_time", None)
        note = request.POST.get("note", None)
        eta = request.POST.get("ETA", None)
        selectedIds = request.POST.getlist("plt_ids")
        ids = []

        for plt_ids in selectedIds:
            plt_ids = plt_ids.split(",")
            plt_ids = [int(i) for i in plt_ids]
            ids.extend(plt_ids)
        # 查找板子
        pallets = await sync_to_async(list)(
            Pallet.objects.select_related("container_number").filter(id__in=ids)
        )
        total_weight, total_cbm, total_pcs = 0.0, 0.0, 0
        for plt in pallets:
            plt.location = receiving_warehouse
            total_weight += plt.weight_lbs
            total_pcs += plt.pcs
            total_cbm += plt.cbm
        await sync_to_async(bulk_update_with_history)(
            pallets,
            Pallet,
            fields=["location"],
        )
        # 然后新建transfer_warehouse新记录
        container_numbers = [p.container_number.container_number for p in pallets]
        current_time = datetime.now()
        batch_id = (
            str(uuid.uuid4())[:2].upper()
            + "-"
            + current_time.strftime("%m%d")
            + "-"
            + shipping_warehouse
        )
        batch_id = batch_id.replace(" ", "").upper()

        if not pickup_number:
            ship_w = shipping_warehouse.split('-')[0]
            rec_w = receiving_warehouse.split('-')[0]
            ca = request.POST.get("carrier").strip()
            month_day = current_time.strftime("%m%d")
            random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=2))
            pickup_number = 'ZEM-'+ ship_w +'to' + rec_w + '-' + '' + month_day + ca +'-' + random_code
        fleet = Fleet(
            **{
                "carrier": request.POST.get("carrier").strip(),
                "fleet_type": "transfer",
                "pickup_number": pickup_number,
                "appointment_datetime": shipping_time,  
                "departured_at": shipping_time,  
                "fleet_number": "FO"
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper(),
                "scheduled_at": current_time,
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": len(pallets),
                "total_pcs": total_pcs,
                "origin": receiving_warehouse,
            }
        )
        await sync_to_async(fleet.save)()
        transfer_location = TransferLocation(
            **{
                "shipping_warehouse": shipping_warehouse,
                "receiving_warehouse": receiving_warehouse,
                "shipping_time": shipping_time,
                "ETA": eta,
                "batch_number": batch_id,
                "container_number": container_numbers,
                "plt_ids": ids,
                "total_pallet": len(pallets),
                "total_pcs": total_pcs,
                "total_cbm": total_cbm,
                "total_weight": total_weight,
                "fleet_number": fleet,
                "note": note,
            }
        )
        await sync_to_async(transfer_location.save)()
        
        mutable_get = request.GET.copy()
        mutable_get["warehouse"] = request.POST.get("warehouse")
        request.GET = mutable_get
        return await self.handle_warehouse_post(request)
    
    async def handle_transfer_history_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_transfer_history, context

    async def handle_transfer_history_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        warehouse = request.POST.get("warehouse")
        not_arrived_raw = await sync_to_async(list)(
            TransferLocation.objects.filter(
                receiving_warehouse=warehouse,
                fleet_number__arrived_at__isnull=True
            ).select_related("fleet_number")
        )
        not_arrived = []

        for t in not_arrived_raw:
            pickup_number = t.fleet_number.pickup_number if t.fleet_number else ""
            if t.plt_ids:
                try:
                    plt_id_list = literal_eval(t.plt_ids)
                except Exception:
                    plt_id_list = []
            else:
                plt_id_list = []

            if plt_id_list:
                pallets = await sync_to_async(list)(
                    Pallet.objects.filter(id__in=plt_id_list)
                )
                container_ids = {p.container_number_id for p in pallets}
                containers = await sync_to_async(dict)(
                    Container.objects.filter(id__in=container_ids)
                    .values_list("id", "container_number")
                )

                seen_pairs = set()
                pairs = []
                for p in pallets:
                    pair = (p.container_number_id, p.destination)
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        pairs.append(f"{containers.get(p.container_number_id, '')}-{p.destination}")
                not_arrived.append({
                    "transfer": t,
                    "pairs": pairs, 
                    "pickup_number": pickup_number,
                })
            else:
                not_arrived.append({
                    "transfer": t,
                    "pairs": "",
                    "pickup_number": pickup_number,
                })
        
        arrived_row = await sync_to_async(list)(
            TransferLocation.objects.filter(
                receiving_warehouse = warehouse,
                fleet_number__arrived_at__isnull=False
            ).select_related("fleet_number")
        )
        arrived = []

        for t in arrived_row:
            if t.plt_ids:
                try:
                    plt_id_list = literal_eval(t.plt_ids)
                except Exception:
                    plt_id_list = []
            else:
                plt_id_list = []

            pickup_number = t.fleet_number.pickup_number if t.fleet_number else ""
            if plt_id_list:
                pallets = await sync_to_async(list)(
                    Pallet.objects.filter(id__in=plt_id_list)
                )
                container_ids = {p.container_number_id for p in pallets}
                containers = await sync_to_async(dict)(
                    Container.objects.filter(id__in=container_ids)
                    .values_list("id", "container_number")
                )

                seen_pairs = set()
                pairs = []
                for p in pallets:
                    pair = (p.container_number_id, p.destination)
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        pairs.append(f"{containers.get(p.container_number_id, '')}-{p.destination}")
                arrived.append({
                    "transfer": t,
                    "pairs": pairs, 
                    "pickup_number": pickup_number,
                })
            else:
                arrived.append({
                    "transfer": t,
                    "pairs": "",
                    "pickup_number": pickup_number,
                })
        context = {
            "not_arrived": not_arrived,
            "arrived": arrived,
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options
        }
        return self.template_transfer_history, context
    
    async def handle_export_bol_transfer_post(
        self, request: HttpRequest
    )-> HttpResponse:

        transfer_id = request.POST.get("transfer_id")
        transfer = await sync_to_async(TransferLocation.objects.select_related('fleet_number').get)(
            id=transfer_id
        )
        fleet = transfer.fleet_number
        plt_ids = ast.literal_eval(transfer.plt_ids)
        pallets = await sync_to_async(list)(
            Pallet.objects.filter(id__in=plt_ids)
            .values(
                'container_number__container_number',
                'destination'
            )
            .annotate(
                total_cbm=Sum('cbm'),
                total_pallets=Count('id')
            )
            .order_by('container_number__container_number', 'destination')
        )
        for _, group in groupby(pallets, key=lambda x: x["container_number__container_number"]):
            group_list = list(group)
            rowspan = len(group_list)
            for idx, g in enumerate(group_list):
                g["rowspan"] = rowspan if idx == 0 else 0   # 只有第一行才显示
                g["show_container"] = idx == 0
        warehouse = transfer.receiving_warehouse
        pickup_number = fleet.pickup_number
        recv_warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=transfer.receiving_warehouse)
        ship_warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=transfer.shipping_warehouse)
        context = {
            "recv_warehouse": recv_warehouse,
            "ship_warehouse": ship_warehouse,
            "s_warehouse": transfer.shipping_warehouse,
            "r_warehouse": transfer.receiving_warehouse,
            "pickup_number": pickup_number,
            "fleet_number": transfer.fleet_number,
            "pallets": pallets,
            "note": transfer.note,
            "transfer":transfer
        }
        template = get_template(self.template_transfer_bol)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{warehouse}+{pickup_number}+BOL.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response

    async def handle_transfer_confirm_arrived_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        arrival_date_str = request.POST.get('arrival_date')
        arrival_date = datetime.strptime(arrival_date_str, '%Y-%m-%d')

        transfer_id = request.POST.get("transfer_id")
        transfer = await sync_to_async(TransferLocation.objects.select_related('fleet_number').get)(
            id=transfer_id
        )
        fleet = transfer.fleet_number       
        fleet.arrived_at = arrival_date

        #构建fleetshipmentpallet表
        plt_ids = ast.literal_eval(transfer.plt_ids)
        pallets = await sync_to_async(list)(
            Pallet.objects.filter(id__in=plt_ids)
            .select_related('container_number') 
        )
        grouped_by_po = defaultdict(list)
        for pallet in pallets:
            grouped_by_po[pallet.PO_ID].append(pallet)

        new_fleet_shipment_pallets = []
        for po_id, pallet_list in grouped_by_po.items():
            container = pallet_list[0].container_number
            new_record = FleetShipmentPallet(
                fleet_number=fleet,
                pickup_number=fleet.pickup_number,
                transfer_number=transfer,
                PO_ID=po_id,
                total_pallet=transfer.total_pallet,
                container_number=container,  
            )
            new_fleet_shipment_pallets.append(new_record)

        if new_fleet_shipment_pallets:
            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets,
                batch_size=500  
            )
        await sync_to_async(fleet.save)()
        return await self.handle_transfer_history_warehouse_post(request)

    async def handle_operation_get(self) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_transfer_pallet, context
    
    async def handle_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        #查找在仓的所有板子
        pallet = await self._get_inventory_pallet(warehouse)
        pallet_json = {}
        pallet_json = {
            p.get("plt_ids"): {
                k: (
                    round(v, 2)
                    if isinstance(v, float) or isinstance(v, int)
                    else (
                        re.sub(r'[\x00-\x1F\x7F\t"\']', " ", v)
                        if v != "None" and v
                        else ""
                    )
                )
                for k, v in p.items()
            }
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
            "pallet_json": json.dumps(pallet_json, ensure_ascii=False),
        }
        return self.template_transfer_pallet, context
    
    async def _get_inventory_pallet(
        self, warehouse: str, criteria: models.Q | None = None
    ) -> list[Pallet]:
        if criteria:
            criteria &= models.Q(location=warehouse)
            criteria &= models.Q(
                models.Q(shipment_batch_number__isnull=True)
                | models.Q(shipment_batch_number__is_shipped=False)
            )
        else:
            criteria = models.Q(
                models.Q(location=warehouse)
                & models.Q(
                    models.Q(shipment_batch_number__isnull=True)
                    | models.Q(shipment_batch_number__is_shipped=False)
                )
            )
        return await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "shipment_batch_number",
                "container_number__order__customer_name",
            )
            .filter(criteria)
            .annotate(str_id=Cast("id", CharField()))
            .values(
                "destination",
                "delivery_method",
                "delivery_type",
                "shipping_mark",
                "fba_id",
                "ref_id",
                "note",
                "address",
                "zipcode",
                "location",
                customer_name=F("container_number__order__customer_name__zem_name"),
                container=F("container_number__container_number"),
                shipment=F("shipment_batch_number__shipment_batch_number"),
                appointment_id=F("shipment_batch_number__appointment_id"),
            )
            .annotate(
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                weight=Sum("weight_lbs", output_field=FloatField()),
                n_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-n_pallet")
        )
    



    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False