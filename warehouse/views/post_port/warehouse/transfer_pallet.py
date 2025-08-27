import openpyxl
import re
import json
import pandas as pd
import uuid
from typing import Any, Tuple
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
from django.views.decorators.csrf import csrf_protect
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.pallet_destroyed import PalletDestroyed
from warehouse.models.fleet import Fleet
from warehouse.models.transfer_location import TransferLocation
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS


class TransferPallet(View):
    template_transfer_pallet = (
        "post_port/transfer_pallet/01_transfer_pallet.html"
    )
    template_transfer_history = (
        "post_port/transfer_pallet/01_transfer_history.html"
    )
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
            template, context = await self.handle_export_bol_transfer_post(request)
            return render(request, template, context)
        

    async def handle_transfer_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        shipping_warehouse = request.POST.get("warehouse")
        receiving_warehouse = request.POST.get("receiving_warehouse")
        pickup_number = request.POST.get("pickup_number",None)
        shipping_time = request.POST.get("shipping_time", None)
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
            pickup_number = 'ZEM-'+ ship_w +'to' + rec_w + '-' + '' + month_day + ca
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
            )
        )
        not_arrived = []

        for t in not_arrived_raw:
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
                })
            else:
                not_arrived.append({
                    "transfer": t,
                    "pairs": "",
                })
        
        arrived_row = await sync_to_async(list)(
            TransferLocation.objects.filter(
                receiving_warehouse = warehouse,
                fleet_number__arrived_at__isnull=False
            )
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
                })
            else:
                arrived.append({
                    "transfer": t,
                    "pairs": "",
                })
        context = {
            "not_arrived": not_arrived,
            "arrived": arrived,
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options
        }
        return self.template_transfer_history, context
    
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