import ast
import uuid
import os,json
import pytz
import pandas as pd
import numpy as np

from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, Max, FloatField, IntegerField, When, Count, Q
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template
from django.utils import timezone

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    amazon_fba_locations,
    APP_ENV,
    LOAD_TYPE_OPTIONS,
    SP_USER,
    SP_PASS,
    SP_URL,
    SP_DOC_LIB,
    SYSTEM_FOLDER,
)


class ShippingManagement(View):
    template_main = "post_port/shipment/01_search.html"
    template_td = "post_port/shipment/02_td_shipment.html"
    template_td_schedule = "post_port/shipment/02_1_td_shipment_schedule.html"
    template_td_shipment_info = "post_port/shipment/02_2_td_shipment_info.html"
    template_fleet = "post_port/shipment/03_fleet_main.html"
    template_fleet_schedule = "post_port/shipment/03_1_fleet_schedule.html"
    template_fleet_schedule_info = "post_port/shipment/03_2_fleet_schedule_info.html"
    template_outbound = "post_port/shipment/04_outbound_main.html"
    template_outbound_departure = "post_port/shipment/04_outbound_depature_confirmation.html"
    template_delivery_and_pod = "post_port/shipment/05_delivery_and_pod.html"
    template_bol = "export_file/bol_base_template.html"
    template_appointment_management = "post_port/shipment/06_appointment_management.html"
    area_options = {"NJ": "NJ", "SAV": "SAV"}
    warehouse_options = {"": "", "NJ-07001": "NJ-07001", "NJ-08817": "NJ-08817", "SAV-31326": "SAV-31326"}
    shipment_type_options = {"":"", "FTL/LTL":"FTL/LTL", "外配/快递":"外配/快递"}

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        print('step',step)
        if step == "shipment_info":
            template, context = await self.handle_shipment_info_get(request)
            return render(request, template, context)
        elif step == "outbound":
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_outbound, context)
        elif step == "fleet":
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_fleet, context)
        elif step == "fleet_info":
            template, context = await self.handle_fleet_info_get(request)
            return render(request, template, context)
        elif step == "fleet_depature":
            template, context = await self.handle_fleet_depature_get(request)
            return render(request, template, context)
        elif step == "delivery_and_pod":
            template, context = await self.handle_delivery_and_pod_get(request)
            return render(request, template, context)
        elif step == "appointment_management":
            template, context = await self.handle_appointment_management_get(request)
            return render(request, template, context)
        else:
            context = {"area_options": self.area_options}
            return render(request, self.template_main, context)
    
    async def post(self, request: HttpRequest) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "selection":
            template, context = await self.handle_selection_post(request)
            return render(request, template, context)
        elif step == "appointment":
            template, context = await self.handle_appointment_post(request)
            return render(request, template, context)
        elif step == "alter_po_shipment":
            template, context = await self.handle_alter_po_shipment_post(request)
            return render(request, template, context)
        elif step == "cancel":
            template, context = await self.handle_cancel_post(request)
            return render(request, template, context)
        elif step == "update_appointment":
            template, context = await self.handle_update_appointment_post(request)
            return render(request, template, context)
        elif step == "fleet_warehouse_search":
            template, context = await self.handle_fleet_warehouse_search_post(request)
            return render(request, template, context)
        elif step == "add_appointment_to_fleet":
            template, context = await self.handle_add_appointment_to_fleet_post(request)
            return render(request, template, context)
        elif step == "fleet_confirmation":
            template, context = await self.handle_fleet_confirmation_post(request)
            return render(request, template, context)
        elif step == "update_fleet":
            template, context = await self.handle_update_fleet_post(request)
            return render(request, template, context)
        elif step == "cancel_fleet":
            template, context = await self.handle_cancel_fleet_post(request)
            return render(request, template, context)
        elif step == "outbound_warehouse_search":
            template, context = await self.handle_outbound_warehouse_search_post(request)
            return render(request, template, context)
        elif step == "export_packing_list":
            return await self.handle_export_packing_list_post(request)
        elif step == "export_bol":
            return await self.handle_export_bol_post(request)
        elif step == "fleet_departure":
            template, context = await self.handle_fleet_departure_post(request)
            return render(request, template, context)
        elif step == "fleet_delivery_search":
            mutable_get = request.GET.copy()
            mutable_get["fleet_number"] = request.POST.get("fleet_number", None)
            mutable_get["batch_number"] = request.POST.get("batch_number", None)
            request.GET = mutable_get
            template, context = await self.handle_delivery_and_pod_get(request)
            return render(request, template, context)
        elif step == "confirm_delivery":
            template, context = await self.handle_confirm_delivery_post(request)
            return render(request, template, context)
        elif step == "appointment_warehouse_search":
            template, context = await self.handle_appointment_warehouse_search_post(request)
            return render(request, template, context)
        elif step == "create_empty_appointment":
            template, context = await self.handle_create_empty_appointment_post(request)
            return render(request, template, context)
        elif step == "download_empty_appointment_template":
            return await self.handle_download_empty_appointment_template_post()
        elif step == "upload_and_create_empty_appointment":
            template, context = await self.handle_upload_and_create_empty_appointment_post(request)
            return render(request, template, context)
        else:
            return await self.get(request)
        
    async def handle_shipment_info_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        batch_number = request.GET.get("batch_number")
        mutable_post = request.POST.copy()
        mutable_post['area'] = request.GET.get("area")
        request.POST = mutable_post
        _, context = await self.handle_warehouse_post(request)
        shipment = await sync_to_async(Shipment.objects.select_related("fleet_number").get)(shipment_batch_number=batch_number)
        packing_list_selected = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=True
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=False
            ),
        )
        context.update({
            "shipment": shipment,
            "packing_list_selected": packing_list_selected,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "warehouse": request.GET.get("warehouse"),
            "warehouse_options": self.warehouse_options,
            "shipment_type_options": self.shipment_type_options,
        })
        return self.template_td_shipment_info, context
    
    async def handle_fleet_info_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number")
        mutable_post = request.POST.copy()
        mutable_post['name'] = request.GET.get("warehouse")
        request.POST = mutable_post
        _, context = await self.handle_fleet_warehouse_search_post(request)
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        context.update({
            "fleet": fleet,
            "shipment": shipment,
        })
        return self.template_fleet_schedule_info, context
    
    async def handle_fleet_depature_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        selected_fleet_number = request.GET.get("fleet_number")
        warehouse = request.GET.get("warehouse")
        selected_fleet = await sync_to_async(Fleet.objects.get)(fleet_number=selected_fleet_number)
        shipment = await sync_to_async(list)(
            Pallet.objects.select_related(
                "packing_list", "shipment_batch_number",
                "shipment_batch_number__fleet_number", 
                "packing_list__container_number",
            ).filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number
            ).annotate(
                str_plt_id=Cast("pallet_id", CharField()),
            ).values(
                "shipment_batch_number__shipment_batch_number", "packing_list__container_number__container_number", 
                "packing_list__destination", "shipment_batch_number__appointment_id", "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__note",
            ).annotate(
                plt_ids=StringAgg("str_plt_id", delimiter=",", distinct=True, ordering="str_plt_id"),
                total_weight=Sum("weight_lbs", output_field=FloatField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_pallet=Count('pallet_id', distinct=True)
            ).order_by("-shipment_batch_number__shipment_appointment")
        )
        packing_list = {}
        for s in shipment:
            pl = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=s["shipment_batch_number__shipment_batch_number"]
                )
            )
            packing_list[s["shipment_batch_number__shipment_batch_number"]] = pl

        pl_fleet = await sync_to_async(list)(
            PackingList.objects.select_related(
                "container_number", "shipment_batch_number", "pallet"
            ).filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number
            ).values(
                'container_number__container_number',
                'destination',
                'shipment_batch_number__shipment_batch_number',
                'shipment_batch_number__shipment_appointment',
            ).annotate(
                total_weight=Sum("pallet__weight_lbs"),
                total_cbm=Sum("pallet__cbm"),
                total_n_pallet=Count("pallet__pallet_id", distinct=True),
            ).order_by("-shipment_batch_number__shipment_appointment")
        )
       
        
        shipment_batch_numbers = []
        for s in shipment:
            if s.get("shipment_batch_number__shipment_batch_number") not in shipment_batch_numbers:
                shipment_batch_numbers.append(s.get("shipment_batch_number__shipment_batch_number"))
        mutable_post = request.POST.copy()
        mutable_post['name'] = request.GET.get("warehouse")
        request.POST = mutable_post
        _, context = await self.handle_outbound_warehouse_search_post(request)
        context.update({
            "selected_fleet": selected_fleet,
            "shipment": shipment,
            "warehouse": warehouse,
            "shipment_batch_numbers": shipment_batch_numbers,
            "packing_list": packing_list,
            "pl_fleet":pl_fleet,
        })
        return self.template_outbound_departure, context
    
    async def handle_delivery_and_pod_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number", "")
        batch_number = request.GET.get("batch_number", "")
        criteria = models.Q(
            departured_at__isnull=False,
            arrived_at__isnull=True
        )
        if fleet_number:
            criteria &= models.Q(fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment__shipment_batch_number=batch_number)
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(criteria).order_by("departured_at")
        )
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": fleet,
            "upload_file_form": UploadFileForm(required=True),
        }
        return self.template_delivery_and_pod, context
    
    async def handle_appointment_management_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        context = {
            "warehouse_options": self.warehouse_options,
            "start_date": (datetime.now().date() + timedelta(days=-7)).strftime("%Y-%m-%d"),
            "end_date": (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d"),
        }
        return self.template_appointment_management, context

    async def handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        if request.POST.get("area"):
            area = request.POST.get("area")
        elif request.POST.get("warehouse"):
            area = request.POST.get("warehouse")[:2]
        elif request.GET.get("warehouse"):
            area = request.GET.get("warehouse")[:2]
        else:
            area = None
        shipment = await sync_to_async(list)(
            Shipment.objects.prefetch_related(
                "packinglist", "packinglist__container_number", "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse", "order", "pallet"
            ).filter(
                models.Q(
                    models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area=area) |
                    models.Q(pallet__container_number__order__retrieval_id__retrieval_destination_area=area)
                ) &
                models.Q(
                    is_shipped=False,
                    in_use=True,
                    is_canceled=False,
                )
            ).distinct().order_by('-abnormal_palletization', 'shipment_appointment')
        )
        criteria = models.Q(
            container_number__order__retrieval_id__retrieval_destination_area=area,
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,
            container_number__order__order_type="转运",
            container_number__order__created_at__gte='2024-09-01',
        ) & (
            # TODOs: 考虑按照安排提柜时间筛选
            models.Q(container_number__order__vessel_id__vessel_eta__lte=datetime.now().date() + timedelta(days=7)) |
            models.Q(container_number__order__eta__lte=datetime.now().date() + timedelta(days=7))
        )
        pl_criteria = criteria & models.Q(container_number__order__offload_id__offload_at__isnull=True)
        plt_criteria = criteria & models.Q(container_number__order__offload_id__offload_at__isnull=False)
        packing_list_not_scheduled = await self._get_packing_list(pl_criteria, plt_criteria)
        cbm_act, cbm_est, pallet_act, pallet_est = 0, 0, 0, 0
        for pl in packing_list_not_scheduled:
            if pl.get("label") == "ACT":
                cbm_act += pl.get("total_cbm")
                pallet_act += pl.get("total_n_pallet_act")
            else:
                cbm_est += pl.get("total_cbm")
                if pl.get("total_n_pallet_est") < 1:
                    pallet_est += 1
                elif pl.get("total_n_pallet_est")%1 >= 0.45:
                    pallet_est += int(pl.get("total_n_pallet_est") // 1 + 1)
                else:
                    pallet_est += int(pl.get("total_n_pallet_est") // 1)
        context = {
            "shipment_list": shipment,
            "area_options": self.area_options,
            "area": area,
            "packing_list_not_scheduled": packing_list_not_scheduled,
            "cbm_act": cbm_act,
            "cbm_est": cbm_est,
            "pallet_act": pallet_act,
            "pallet_est": pallet_est,
        }
        return self.template_td, context
        
    async def handle_selection_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        selections = request.POST.getlist("is_shipment_schduled")
        ids = request.POST.getlist("pl_ids")
        ids = [id for s, id in zip(selections, ids) if s == "on"]
        plt_ids = request.POST.getlist("plt_ids")
        plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
        selected = [int(i) for id in ids for i in id.split(",") if i]
        selected_plt = [int(i) for id in plt_ids for i in id.split(",") if i]
        if selected or selected_plt:
            current_time = datetime.now()
            _, context = await self.handle_warehouse_post(request)
            packing_list_selected = await self._get_packing_list(
                models.Q(id__in=selected),
                models.Q(id__in=selected_plt),
            )
            total_weight, total_cbm, total_pcs, total_pallet = .0, .0, 0, 0
            for pl in packing_list_selected:
                total_weight += pl.get("total_weight_lbs") if pl.get("total_weight_lbs") else 0
                total_cbm += pl.get("total_cbm") if pl.get("total_cbm") else 0
                total_pcs += pl.get("total_pcs") if pl.get("total_pcs") else 0
                if pl.get("label") == "ACT":
                    total_pallet += pl.get("total_n_pallet_act")
                else:
                    if pl.get("total_n_pallet_est < 1"):
                        total_pallet += 1
                    elif pl.get("total_n_pallet_est")%1 >= 0.45:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1 + 1)
                    else:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1)
            destination = packing_list_selected[0].get("destination", "RDM")
            batch_id = destination + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper()
            batch_id = batch_id.replace(" ", "").upper()
            address = amazon_fba_locations.get(destination, None) #查找亚马逊地址中是否有该地址
            if destination in amazon_fba_locations: 
                fba = amazon_fba_locations[destination]
                address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
            else:
                address, zipcode = str(packing_list_selected[0].get("address")), str(packing_list_selected[0].get('zipcode'))
                if zipcode.lower() not in address.lower():   #如果不在亚马逊地址中，就从packing_list_selected的第一个元素获取地址和编码，转为字符串类型
                    address += f", {zipcode}"                #如果编码不在地址字符串内，将邮编添加到字符串后面
            shipment_data = {
                "shipment_batch_number": str(batch_id),
                "destination": destination,
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": total_pallet,
                "total_pcs": total_pcs,
            }
            context.update({
                "batch_id": batch_id,
                "packing_list_selected": packing_list_selected,
                "pl_ids": selected,
                "pl_ids_raw": ids,
                "plt_ids": selected_plt,
                "plt_ids_raw": plt_ids,
                "address": address,
                "shipment_data": shipment_data,
                "warehouse_options": self.warehouse_options,
                "load_type_options": LOAD_TYPE_OPTIONS,
                "shipment_type_options": self.shipment_type_options,
            })
            return self.template_td_schedule, context
        else:
            return await self.handle_warehouse_post(request)
        
    async def handle_appointment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        area = request.POST.get("area")
        current_time = datetime.now()
        appointment_type = request.POST.get("type")
        if appointment_type == "td":
            shipment_data = ast.literal_eval(request.POST.get("shipment_data"))
            shipment_type = request.POST.get("shipment_type")
            appointment_id = request.POST.get("appointment_id", None)
            appointment_id = appointment_id.strip() if appointment_id else None
            try:
                existed_appointment = await sync_to_async(Shipment.objects.get)(appointment_id=appointment_id)
            except:
                existed_appointment = None
            if existed_appointment:
                if existed_appointment.in_use:
                    raise RuntimeError(f"Appointment {existed_appointment} already used by other shipment!")
                elif existed_appointment.is_canceled:
                    raise RuntimeError(f"Appointment {existed_appointment} already exists and is canceled!")
                elif existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC) < timezone.now():
                    raise RuntimeError(f"Appointment {existed_appointment} already exists and expired!")
                elif existed_appointment.destination != request.POST.get("destination", None):
                    raise ValueError(f"Appointment {existed_appointment} has a different destination {existed_appointment.destination} - {request.POST.get('destination', None)}!")
                else:
                    shipment = existed_appointment
                    shipment.shipment_batch_number = shipment_data["shipment_batch_number"]
                    shipment.in_use = True
                    shipment.origin = request.POST.get("origin", "")
                    shipment.shipment_type = shipment_type
                    shipment.load_type = request.POST.get("load_type", None)
                    shipment.note = request.POST.get("note", "")
                    shipment.shipment_schduled_at = timezone.now()
                    shipment.is_shipment_schduled = True
                    shipment.destination = request.POST.get("destination", None)
                    shipment.address = request.POST.get("address", None)
                    try:
                        shipment.third_party_address = shipment_data["third_party_address"].strip()
                    except:
                        pass
            else:
                if await self._shipment_exist(shipment_data["shipment_batch_number"]):
                    raise ValueError(f"Shipment {shipment_data['shipment_batch_number']} already exists!")
                shipment_data["appointment_id"] = request.POST.get("appointment_id", None)
                try:
                    shipment_data["third_party_address"] = shipment_data["third_party_address"].strip()
                except:
                    pass
                shipment_data["shipment_type"] = shipment_type
                shipment_data["load_type"] = request.POST.get("load_type", None)
                shipment_data["note"] = request.POST.get("note", "")
                shipment_data["shipment_appointment"] = request.POST.get("shipment_appointment", None)
                shipment_data["shipment_schduled_at"] = current_time
                shipment_data["is_shipment_schduled"] = True
                shipment_data["destination"] = request.POST.get("destination", None)
                shipment_data["address"] = request.POST.get("address", None)
                shipment_data["origin"] = request.POST.get("origin", "")
                if shipment_type == "外配/快递":
                    fleet = Fleet(**{
                        "carrier": request.POST.get("carrier"),
                        "appointment_datetime": request.POST.get("appointment_datetime"),
                        "fleet_number": "FO" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper(),
                        "scheduled_at": current_time,
                        "total_weight": shipment_data["total_weight"],
                        "total_cbm": shipment_data["total_cbm"],
                        "total_pallet": shipment_data["total_pallet"],
                        "total_pcs": shipment_data["total_pcs"],
                        "origin": shipment_data["origin"]
                    })
                    await sync_to_async(fleet.save)()
                    shipment_data["fleet_number"] = fleet
            if not existed_appointment:
                shipment = Shipment(**shipment_data)
            await sync_to_async(shipment.save)()

            container_number = set()
            pl_ids = request.POST.get("pl_ids").strip('][').split(', ')
            try:
                pl_ids = [int(i) for i in pl_ids]
                packing_list = await sync_to_async(list)(PackingList.objects.select_related("container_number").filter(id__in=pl_ids))
                for pl in packing_list:
                    pl.shipment_batch_number = shipment
                    container_number.add(pl.container_number.container_number)
                await sync_to_async(PackingList.objects.bulk_update)(packing_list, ["shipment_batch_number"])
            except:
                pass
           
            plt_ids = request.POST.get("plt_ids").strip('][').split(', ')
            try:
                plt_ids = [int(i) for i in plt_ids]
                pallet = await sync_to_async(list)(Pallet.objects.select_related("container_number").filter(id__in=plt_ids))
                for p in pallet:
                    p.shipment_batch_number = shipment
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])     
            except:
                pass
            order = await sync_to_async(list)(
                Order.objects.select_related(
                    "retrieval_id", "warehouse", "container_number"
                ).filter(container_number__container_number__in=container_number)
            )
            assigned_warehouse = request.POST.get("origin", "")
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=assigned_warehouse)
            updated_order, updated_retrieval = [], []
            for o in order:
                if not o.warehouse or not o.retrieval_id.retrieval_destination_precise:
                    o.warehouse = warehouse
                    o.retrieval_id.retrieval_destination_precise = assigned_warehouse
                    o.retrieval_id.assigned_by_appt = True
                    updated_order.append(o)
                    updated_retrieval.append(o.retrieval_id)
            await sync_to_async(Order.objects.bulk_update)(
                updated_order, ["warehouse"]
            )
            await sync_to_async(Retrieval.objects.bulk_update)(
                updated_retrieval, ["retrieval_destination_precise", "assigned_by_appt"]
            )
            mutable_post = request.POST.copy()
            mutable_post['area'] = area
            request.POST = mutable_post
        else:
            batch_number = request.POST.get("batch_number")
            warehouse = request.POST.get("warehouse")
            shipment_appointment = request.POST.get("shipment_appointment")
            note = request.POST.get("note")
            shipment = Shipment.objects.get(shipment_batch_number=batch_number)
            shipment.shipment_appointment = shipment_appointment
            shipment.note = note
            shipment.is_shipment_schduled = True
            shipment.shipment_schduled_at = current_time
            shipment.save()
            mutable_post = request.POST.copy()
            mutable_post['area'] = warehouse
            request.POST = mutable_post
        return await self.handle_warehouse_post(request)
    
    async def handle_alter_po_shipment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        shipment_batch_number = request.POST.get("shipment_batch_number")
        alter_type = request.POST.get("alter_type")
        shipment = await sync_to_async(Shipment.objects.select_related("fleet_number").get)(shipment_batch_number=shipment_batch_number)
        if alter_type == "add":
            container_number = set()
            selections = request.POST.getlist("is_shipment_added")
            try:
                pl_ids = request.POST.getlist("added_pl_ids")
                pl_ids = [id for s, id in zip(selections, pl_ids) if s == "on"]
                pl_ids = [int(i) for id in pl_ids for i in id.split(",") if i]
                packing_list = await sync_to_async(list)(PackingList.objects.select_related("container_number").filter(id__in=pl_ids))
                for pl in packing_list:
                    pl.shipment_batch_number = shipment
                    shipment.total_weight += pl.total_weight_lbs
                    shipment.total_pcs += pl.pcs
                    shipment.total_cbm += pl.cbm
                    shipment.total_pallet += int(pl.cbm/2)
                    container_number.add(pl.container_number.container_number)
                await sync_to_async(PackingList.objects.bulk_update)(packing_list, ["shipment_batch_number"])
            except:
                pass
            try:
                plt_ids = request.POST.getlist("added_plt_ids")
                plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
                plt_ids = [int(i) for id in plt_ids for i in id.split(",") if i]
                pallet = await sync_to_async(list)(Pallet.objects.select_related("container_number").filter(id__in=plt_ids))
                for p in pallet:
                    p.shipment_batch_number = shipment
                    shipment.total_weight += p.weight_lbs
                    shipment.total_pcs += p.pcs
                    shipment.total_cbm += p.cbm
                shipment.total_pallet += len(set([p.pallet_id for p in pallet]))
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])     
            except:
                pass
            order = await sync_to_async(list)(
                Order.objects.select_related(
                    "retrieval_id", "warehouse", "container_number"
                ).filter(container_number__container_number__in=container_number)
            )
            assigned_warehouse = shipment.origin
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=assigned_warehouse)
            updated_order, updated_retrieval = [], []
            for o in order:
                if not o.warehouse or not o.retrieval_id.retrieval_destination_precise:
                    o.warehouse = warehouse
                    o.retrieval_id.retrieval_destination_precise = assigned_warehouse
                    o.retrieval_id.assigned_by_appt = True
                    updated_order.append(o)
                    updated_retrieval.append(o.retrieval_id)
            await sync_to_async(Order.objects.bulk_update)(
                updated_order, ["warehouse"]
            )
            await sync_to_async(Retrieval.objects.bulk_update)(
                updated_retrieval, ["retrieval_destination_precise", "assigned_by_appt"]
            )
        elif alter_type == "remove":
            selections = request.POST.getlist("is_shipment_removed")
            try:
                pl_ids = request.POST.getlist("removed_pl_ids")
                pl_ids = [id for s, id in zip(selections, pl_ids) if s == "on"]
                pl_ids = [int(i) for id in pl_ids for i in id.split(",") if i]
                packing_list = await sync_to_async(list)(PackingList.objects.select_related("container_number").filter(id__in=pl_ids))
                for pl in packing_list:
                    pl.shipment_batch_number = None
                    shipment.total_weight -= pl.total_weight_lbs
                    shipment.total_pcs -= pl.pcs
                    shipment.total_cbm -= pl.cbm
                    shipment.total_pallet -= int(pl.cbm/2)
                await sync_to_async(PackingList.objects.bulk_update)(packing_list, ["shipment_batch_number"])
            except:
                pass
            try:
                plt_ids = request.POST.getlist("removed_plt_ids")
                plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
                plt_ids = [int(i) for id in plt_ids for i in id.split(",") if i]
                pallet = await sync_to_async(list)(Pallet.objects.select_related("container_number").filter(id__in=plt_ids))
                for p in pallet:
                    p.shipment_batch_number = None
                    shipment.total_weight -= p.weight_lbs
                    shipment.total_pcs -= p.pcs
                    shipment.total_cbm -= p.cbm
                shipment.total_pallet -= len(set([p.pallet_id for p in pallet]))
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])     
            except:
                pass
        else:
            raise ValueError(f"Unknown shipment alter type: {alter_type}")
        await sync_to_async(shipment.save)()
        mutable_get = request.GET.copy()
        mutable_get['batch_number'] = shipment_batch_number
        mutable_get['area'] = request.POST.get("area")
        request.GET = mutable_get
        return await self.handle_shipment_info_get(request)
    
    async def handle_cancel_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        appointment_type = request.POST.get("type")
        if appointment_type == "td":
            shipment_batch_number = request.POST.get("shipment_batch_number")
            shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
            if shipment.is_shipped:
                raise RuntimeError(f"Shipment with batch number {shipment} has been shipped!")
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("shipment_batch_number").filter(shipment_batch_number__shipment_batch_number=shipment_batch_number)
            )
            pallet = await sync_to_async(list)(
                Pallet.objects.select_related("shipment_batch_number").filter(shipment_batch_number__shipment_batch_number=shipment_batch_number)
            )
            for pl in packing_list:
                pl.shipment_batch_number = None
            for p in pallet:
                p.shipment_batch_number = None
            await sync_to_async(PackingList.objects.bulk_update)(
                packing_list, ["shipment_batch_number"]
            )
            await sync_to_async(Pallet.objects.bulk_update)(
                pallet, ["shipment_batch_number"]
            )
            shipment.is_canceled = True
            await sync_to_async(shipment.save)()
        else:
            shipment_batch_number = request.POST.get("batch_number")
            shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
            if shipment.is_shipped:
                raise RuntimeError(f"Shipment with batch number {shipment} has been shipped!")
            shipment.is_shipment_schduled = False
            shipment.shipment_appointment = None
            shipment.note = None
            shipment.shipment_schduled_at = None
            shipment.save()
        warehouse = request.POST.get("warehouse")
        mutable_post = request.POST.copy()
        mutable_post['name'] = warehouse
        request.POST = mutable_post
        return await self.handle_warehouse_post(request)
    
    async def handle_update_appointment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        batch_number = request.POST.get("batch_number")
        shipment_type = request.POST.get("shipment_type")    
        shipment = await sync_to_async(Shipment.objects.select_related("fleet_number").get)(shipment_batch_number=batch_number)
        if shipment_type == shipment.shipment_type:
            if shipment_type == "FTL/LTL":
                shipment.appointment_id = request.POST.get("appointment_id")
                shipment.origin = request.POST.get("origin")
                shipment.carrier = request.POST.get("carrier")
                shipment.third_party_address = request.POST.get("third_party_address")
                shipment.load_type = request.POST.get("load_type")
                shipment.shipment_appointment = request.POST.get("shipment_appointment")
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination")
                shipment.address = request.POST.get("address")
            elif shipment_type == "外配/快递":
                shipment.origin = request.POST.get("origin")
                shipment.shipment_appointment = request.POST.get("shipment_appointment")
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination")
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                fleet.carrier = request.POST.get("carrier")
                fleet.appointment_datetime = request.POST.get("appointment_datetime")
                await sync_to_async(fleet.save)()
        else:
            if shipment_type == "FTL/LTL":
                shipment.shipment_type = shipment_type
                shipment.appointment_id = request.POST.get("appointment_id")
                shipment.origin = request.POST.get("origin")
                shipment.carrier = request.POST.get("carrier")
                shipment.third_party_address = request.POST.get("third_party_address")
                shipment.load_type = request.POST.get("load_type")
                shipment.shipment_appointment = request.POST.get("shipment_appointment")
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination")
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                shipment.fleet_number = None
                await sync_to_async(fleet.delete)()
            elif shipment_type == "外配/快递":
                shipment.shipment_type = shipment_type
                shipment.origin = request.POST.get("origin")
                shipment.shipment_appointment = request.POST.get("shipment_appointment")
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination")
                shipment.address = request.POST.get("address")
                shipment.appointment_id = ""
                shipment.load_type = ""
                shipment.third_party_address = ""
                current_time = datetime.now()
                fleet = Fleet(**{
                    "carrier": request.POST.get("carrier"),
                    "appointment_datetime": request.POST.get("appointment_datetime"),
                    "fleet_number": "FO" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper(),
                    "scheduled_at": current_time,
                    "total_weight": shipment.total_weight,
                    "total_cbm": shipment.total_cbm,
                    "total_pallet": shipment.total_pallet,
                    "total_pcs": shipment.total_pcs,
                    "origin": shipment.origin,
                })
                await sync_to_async(fleet.save)()
                shipment.fleet_number = fleet
        await sync_to_async(shipment.save)()
        mutable_get = request.GET.copy()
        mutable_get['warehouse'] = request.POST.get("warehouse")
        mutable_get['batch_number'] = batch_number
        request.GET = mutable_get
        return await self.handle_shipment_info_get(request)
    
    async def handle_fleet_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("name") if request.POST.get("name") else request.POST.get("warehouse")
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                origin=warehouse,
                fleet_number__isnull=True,
                in_use=True,
                is_canceled=False,
                shipment_type='FTL/LTL',
            ).order_by("-batch", "shipment_appointment")
        )
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(
                origin=warehouse,
                departured_at__isnull=True,
            ).order_by("appointment_datetime")
        )
        context = {
            "shipment_list": shipment,
            "fleet_list": fleet,
            "warehouse_form": warehouse_form,
            "warehouse": warehouse,
            "shipment_ids": [],
        }
        return self.template_fleet, context
    
    async def handle_add_appointment_to_fleet_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        selections = request.POST.getlist("is_appointment_added")
        ids = request.POST.getlist("shipment_ids")
        selected_ids = [int(id) for s, id in zip(selections, ids) if s == "on"]
        if selected_ids:
            current_time = datetime.now()
            _, context = await self.handle_fleet_warehouse_search_post(request)
            fleet_number = "F" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper()
            shipment_selected = await sync_to_async(list)(
                Shipment.objects.filter(id__in=selected_ids)
            )
            total_weight, total_cbm, total_pcs, total_pallet = .0, .0, 0, 0
            for s in shipment_selected:
                total_weight += s.total_weight
                total_cbm += s.total_cbm
                total_pcs += s.total_pcs
                total_pallet += s.total_pallet
            fleet_data = {
                "fleet_number": fleet_number,
                "origin": request.POST.get("warehouse"),
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": total_pallet,
                "total_pcs": total_pcs,
            }
            context.update({
                "shipment_ids": selected_ids,
                "fleet_number": fleet_number,
                "shipment_selected": shipment_selected,
                "fleet_data": fleet_data,
            })
            return self.template_fleet_schedule, context
        else:
            return await self.handle_fleet_warehouse_search_post(request)

    async def handle_fleet_confirmation_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        current_time = datetime.now()
        fleet_data = ast.literal_eval(request.POST.get("fleet_data"))
        shipment_ids = request.POST.get("selected_ids").strip('][').split(', ')
        shipment_ids = [int(i) for i in shipment_ids]
        fleet_data.update({
            "carrier": request.POST.get("carrier", ""),
            "license_plate": request.POST.get("license_plate", ""),
            "motor_carrier_number": request.POST.get("motor_carrier_number", ""),
            "dot_number": request.POST.get("dot_number", ""),
            "third_party_address": request.POST.get("third_party_address", ""),
            "appointment_datetime": request.POST.get("appointment_datetime"),
            "scheduled_at": current_time,
            "note": request.POST.get("note", ""),
            "multipule_destination": True if len(shipment_ids) > 1 else False,
        })
        fleet = Fleet(**fleet_data)
        await sync_to_async(fleet.save)()
        shipment = await sync_to_async(list)(Shipment.objects.filter(id__in=shipment_ids))
        for s in shipment:
            s.fleet_number = fleet
        await sync_to_async(Shipment.objects.bulk_update)(shipment, ["fleet_number"])
        return await self.handle_fleet_warehouse_search_post(request)

    async def handle_update_fleet_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        fleet.carrier = request.POST.get("carrier", "")
        fleet.third_party_address = request.POST.get("third_party_address", "")
        fleet.appointment_datetime = request.POST.get("appointment_datetime")
        fleet.note = request.POST.get("note", "")
        await sync_to_async(fleet.save)()
        mutable_get = request.GET.copy()
        mutable_get['warehouse'] = request.POST.get("warehouse")
        mutable_get['fleet_number'] = fleet_number
        request.GET = mutable_get
        return await self.handle_fleet_info_get(request)
    
    async def handle_cancel_fleet_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        if fleet.departured_at is not None:
            raise RuntimeError(f"Shipment with batch number {fleet_number} has been shipped!")
        await sync_to_async(fleet.delete)()
        warehouse = request.POST.get("warehouse")
        mutable_post = request.POST.copy()
        mutable_post['name'] = warehouse
        request.POST = mutable_post
        return await self.handle_fleet_warehouse_search_post(request)
    
    async def handle_outbound_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("name") if request.POST.get("name") else request.POST.get("warehouse")
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(
                origin=warehouse,
                departured_at__isnull=True,
            ).order_by("appointment_datetime")
        )
        context = {
            "warehouse": warehouse,
            "warehouse_form": warehouse_form,
            "fleet": fleet,
        }
        return self.template_outbound, context
    
    async def handle_export_packing_list_post(self, request: HttpRequest) -> HttpResponse:
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("customerInfo")
        if customerInfo:
            customer_info = json.loads(customerInfo)
            packing_list = []
            for row in customer_info:
                packing_list.append({
                    'container_number__container_number': row[0].strip(),
                    'shipment_batch_number__shipment_batch_number': row[1].strip(),
                    'destination': row[2].strip(),
                    'total_cbm': row[3].strip(),
                    'total_n_pallet': row[4].strip()
                })
        else:
            packing_list = await sync_to_async(list)(
            PackingList.objects.select_related(
                "container_number", "shipment_batch_number", "pallet"
            ).filter(
                shipment_batch_number__fleet_number__fleet_number=fleet_number
            ).values(
                "container_number__container_number", "destination", "shipment_batch_number__shipment_batch_number",
                "shipment_batch_number__shipment_appointment"
            ).annotate(
                total_weight=Sum("pallet__weight_lbs"),
                total_cbm=Sum("pallet__cbm"),
                total_n_pallet=Count("pallet__pallet_id", distinct=True),
            ).order_by("-shipment_batch_number__shipment_appointment")
        )
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number).order_by("-shipment_appointment")
        )
        
        df = pd.DataFrame(packing_list)
        if len(shipment) > 1:
            i = 1
            for s in shipment:
                df.loc[df["shipment_batch_number__shipment_batch_number"]==s.shipment_batch_number, "一提两卸"] = f"第{i}装"
                i += 1
        df = df.rename(
            columns={
                "container_number__container_number": "柜号",
                "destination": "仓点",
                "shipment_batch_number__shipment_batch_number": "预约批次",
                "total_cbm": "CBM",
                "total_n_pallet": "板数",
            }
        )
        if len(shipment) > 1:
            df = df[["柜号", "预约批次", "仓点", "CBM", "板数", "一提两卸"]]
        else:
            df = df[["柜号", "预约批次", "仓点", "CBM", "板数"]]
        response = HttpResponse(content_type="text/csv")
        response['Content-Disposition'] = f"attachment; filename=packing_list_{fleet_number}.csv"
        df.to_csv(path_or_buf=response, index=False)
        return response
    
    async def handle_export_bol_post(self, request: HttpRequest) -> HttpResponse:
        batch_number = request.POST.get("shipment_batch_number")
        warehouse = request.POST.get("warehouse")
        customerInfo = request.POST.get("customerInfo")
        #进行判断，如果在前端进行了表的修改，就用修改后的表，如果没有修改，就用packing_list直接查询的
        if customerInfo:  
            customer_info = json.loads(customerInfo)
            packing_list = []
            for row in customer_info:
                packing_list.append({
                    'container_number': row[0].strip(),
                    'fba_id': row[1].strip(),
                    'ref_id': row[2].strip(),
                    'pcs': row[3].strip()
                })
        else:
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=batch_number
                )
            )
        warehouse_obj = await sync_to_async(ZemWarehouse.objects.get)(name=warehouse) if warehouse else None
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        address_chinese_char = False if shipment.address.isascii() else True
        destination_chinese_char = False if shipment.destination.isascii() else True
        try:
            note_chinese_char = False if shipment.note.isascii() else True
        except:
            note_chinese_char = False
        context = {
            "warehouse": warehouse_obj.address,
            "batch_number": batch_number,
            "fleet_number": shipment.fleet_number.fleet_number,
            "shipment": shipment,
            "packing_list": packing_list,
            "address_chinese_char": address_chinese_char,
            "destination_chinese_char": destination_chinese_char,
            "note_chinese_char": note_chinese_char,
        }
        template = get_template(self.template_bol)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response['Content-Disposition'] = f'attachment; filename="BOL_{batch_number}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response
    
    async def handle_fleet_departure_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        departured_at = request.POST.get("departured_at")
        actual_shipped_pallet = request.POST.getlist("actual_shipped_pallet")
        actual_shipped_pallet = [int(n) for n in actual_shipped_pallet]
        scheduled_pallet = request.POST.getlist("scheduled_pallet")
        scheduled_pallet = [int(n) for n in scheduled_pallet]
        scheduled_cbm = request.POST.getlist("scheduled_cbm")
        scheduled_cbm = [float(n) for n in scheduled_cbm]
        scheduled_weight = request.POST.getlist("scheduled_weight")
        scheduled_weight = [float(n) for n in scheduled_weight]
        batch_number = request.POST.getlist("batch_number")
        plt_ids = request.POST.getlist("plt_ids")
        plt_ids = [ids.split(",") for ids in plt_ids]
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                shipment_batch_number__in=batch_number
            ).order_by("-shipment_appointment")
        )
        unshipped_pallet_ids = []
        for plt_id, p_schedule, p_shipped in zip(plt_ids, scheduled_pallet, actual_shipped_pallet):
            if p_schedule > p_shipped:
                unshipped_pallet_ids += plt_id[:p_schedule-p_shipped]
        unshipped_pallet = await sync_to_async(list)(
            Pallet.objects.select_related("shipment_batch_number").filter(pallet_id__in=unshipped_pallet_ids)
        )
        shipment_pallet = {}
        for p in unshipped_pallet:
            if p.shipment_batch_number.shipment_batch_number not in shipment_pallet:
                shipment_pallet[p.shipment_batch_number.shipment_batch_number] = [p]
            else:
                shipment_pallet[p.shipment_batch_number.shipment_batch_number].append(p)
        updated_shipment = []
        updated_pallet = []
        sub_shipment = {s.shipment_batch_number: None for s in shipment}
        fleet_shipped_weight, fleet_shipped_cbm, fleet_shipped_pallet, fleet_shipped_pcs = 0, 0, 0, 0
        for s in shipment:
            dumped_pallets = len(set([p.pallet_id for p in shipment_pallet.get(s.shipment_batch_number)]))
            s.is_shipped = True
            s.shipped_at = departured_at
            s.shipped_pallet = s.total_pallet - dumped_pallets
            if dumped_pallets > 0:
                dumped_weight = sum([p.weight_lbs for p in shipment_pallet.get(s.shipment_batch_number)])
                dumped_cbm = sum([p.cbm for p in shipment_pallet.get(s.shipment_batch_number)])
                dumped_pcs = sum([p.pcs for p in shipment_pallet.get(s.shipment_batch_number)])
                s.pallet_dumpped = dumped_pallets
                s.is_full_out = False
                s.shipped_weight = s.total_weight - dumped_weight
                s.shipped_cbm = s.total_weight - dumped_cbm
                s.shipped_pcs = s.total_weight - dumped_pcs
                sub_shipment_data = {
                    "shipment_batch_number": f"{s.shipment_batch_number.split('_')[0]}_{s.batch + 1}",
                    "master_batch_number": s.shipment_batch_number.split("_")[0],
                    "batch": s.batch + 1,
                    "appointment_id": s.appointment_id,
                    "origin": s.origin,
                    "destination": s.destination,
                    "is_shipment_schduled": s.is_shipment_schduled,
                    "shipment_schduled_at": s.shipment_schduled_at,
                    "shipment_appointment": s.shipment_appointment,
                    "load_type": s.load_type,
                    "total_weight": dumped_weight,
                    "total_cbm": dumped_cbm,
                    "total_pallet": dumped_pallets,
                    "total_pcs": dumped_pcs,
                }
                sub_shipment = Shipment(**sub_shipment_data)
                await sync_to_async(sub_shipment.save)()
                for p in shipment_pallet.get(s.shipment_batch_number):
                    p.shipment_batch_number = sub_shipment
                    updated_pallet.append(p)
            else:
                s.pallet_dumpped = 0
                s.is_full_out = True
                s.shipped_weight = s.total_weight
                s.shipped_cbm = s.total_weight
                s.shipped_pcs = s.total_weight
            fleet_shipped_weight += s.shipped_weight
            fleet_shipped_cbm += s.shipped_cbm
            fleet_shipped_pallet += s.shipped_pallet
            fleet_shipped_pcs += s.shipped_pcs
            updated_shipment.append(s)
        fleet.departured_at = departured_at
        fleet.shipped_weight = fleet_shipped_weight
        fleet.shipped_cbm = fleet_shipped_cbm
        fleet.shipped_pallet = fleet_shipped_pallet
        fleet.shipped_pcs = fleet_shipped_pcs
        await sync_to_async(Shipment.objects.bulk_update)(
            updated_shipment,
            [
                "shipped_pallet", "shipped_weight", "shipped_cbm", "shipped_pcs", "is_shipped", 
                "shipped_at", "pallet_dumpped", "is_full_out"
            ]
        )
        await sync_to_async(Pallet.objects.bulk_update)(
            updated_pallet, ["shipment_batch_number"]
        )
        await sync_to_async(fleet.save)()
        return await self.handle_outbound_warehouse_search_post(request)
    
    async def handle_confirm_delivery_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        conn = await self._get_sharepoint_auth()
        pod_form = UploadFileForm(request.POST, request.FILES)
        arrived_at = request.POST.get("arrived_at")
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        if pod_form.is_valid():
            file = request.FILES['file']
            file_extension = os.path.splitext(file.name)[1]
            file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/pod/{APP_ENV}")
            sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
            resp = sp_folder.upload_file(f"{fleet_number}{file_extension}", file).execute_query()
            link = resp.share_link(SharingLinkKind.OrganizationView).execute_query().value.to_json()["sharingLinkInfo"]["Url"]
        else:
            raise ValueError("invalid file uploaded.")
        fleet.arrived_at = arrived_at
        fleet.pod_link = link
        updated_shipment = []
        for s in shipment:
            s.arrived_at = arrived_at
            s.is_arrived = True
            s.pod_link = link
            updated_shipment.append(s)
        await sync_to_async(fleet.save)()
        await sync_to_async(Shipment.objects.bulk_update)(
            updated_shipment,
            ["arrived_at", "is_arrived", "pod_link"]
        )
        return await self.handle_delivery_and_pod_get(request)

    async def handle_appointment_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehosue = request.POST.get("warehouse")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number", "container_number__order__retrieval_id"
            ).filter(
                (
                    models.Q(container_number__order__retrieval_id__retrieval_destination_precise=warehosue) |
                    models.Q(container_number__order__warehouse__name=warehosue)
                ),
                shipment_batch_number__isnull=True,
            ).values(
                "destination",
                warehouse=F("container_number__order__retrieval_id__retrieval_destination_precise"),
            ).annotate(
                total_cbm=Sum("cbm", output_field=IntegerField()),
                total_pallet=Count("pallet_id", distinct=True),
            ).order_by("-total_pallet")
        )
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related(
                "container_number", "container_number__order__retrieval_id"
            ).filter(
                (
                    models.Q(container_number__order__retrieval_id__retrieval_destination_precise=warehosue) |
                    models.Q(container_number__order__warehouse__name=warehosue)
                ),
                shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
            ).values(
                "destination",
                warehouse=F("container_number__order__retrieval_id__retrieval_destination_precise"),
            ).annotate(
                total_cbm=Sum("cbm", output_field=IntegerField()),
                total_pallet=Sum("cbm", output_field=FloatField())/2,
            ).order_by("-total_pallet")
        )
        appointment = await sync_to_async(list)(
            Shipment.objects.filter(
                (models.Q(origin__isnull=True) | models.Q(origin="") | models.Q(origin=warehosue)),
                models.Q(in_use=False, is_canceled=False)
            ).order_by("shipment_appointment")
        )
        appointment_data = await sync_to_async(list)(
            Shipment.objects.filter(
                (models.Q(origin__isnull=True) | models.Q(origin="") | models.Q(origin=warehosue)),
                models.Q(in_use=False, is_canceled=False),
                shipment_appointment__gt=datetime.now(),
            ).values(
                "destination"
            ).annotate(
                n_appointment=Count("appointment_id", distinct=True)
            )
        )
        df_pallet = pd.DataFrame(pallet)
        df_packing_list = pd.DataFrame(packing_list)
        df_appointment = pd.DataFrame(appointment_data)
        df = pd.merge(df_pallet, df_packing_list, how="outer", on=["destination", "warehouse"]).fillna(0)
        df = df.merge(df_appointment, how="left", on=["destination"]).fillna(0)
        df["total_cbm"] = df["total_cbm_x"] + df["total_cbm_y"]
        df["total_pallet"] = df["total_pallet_x"] + df["total_pallet_y"]
        df = df.drop(["total_cbm_x", "total_cbm_y", "total_pallet_x", "total_pallet_y"], axis=1)
        df["n_appointment"] = df["n_appointment"].astype(int)
        df["total_pallet"] = df["total_pallet"].astype(int)
        df = df.sort_values(by="total_pallet", ascending=False)
        context = {
            "appointment": appointment,
            "po_appointment_summary": df.to_dict("records"),
            "warehouse": warehosue,
            "warehouse_options": self.warehouse_options,
            "upload_file_form": UploadFileForm(),
            "start_date": start_date,
            "end_date": end_date,
        }
        return self.template_appointment_management, context
    
    async def handle_create_empty_appointment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        appointment_id = request.POST.get("appointment_id").strip()
        appointment = await sync_to_async(list)(Shipment.objects.filter(appointment_id=appointment_id))
        if appointment:
            raise RuntimeError(f"Appointment {appointment_id} already exist!")
        await sync_to_async(Shipment.objects.create)(**{
            "appointment_id": appointment_id,
            "destination": request.POST.get("destination").upper(),
            "shipment_appointment": request.POST.get("shipment_appointment"),
            "origin": request.POST.get("origin", None),
            "in_use": False,
        })
        warehouse = request.POST.get("warehouse", "")
        if warehouse:
            return await self.handle_appointment_warehouse_search_post(request)
        else:
            return await self.handle_appointment_management_get(request)
        
    async def handle_download_empty_appointment_template_post(self) -> HttpResponse:
        file_path = Path(__file__).parent.parent.parent.parent.resolve().joinpath("templates/export_file/appointment_template.xlsx")
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, 'rb') as file:
            response = HttpResponse(file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="appointment_template.xlsx"'
            return response
        
    async def handle_upload_and_create_empty_appointment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        # raise ValueError(request.POST, request.FILES)
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            df["warehouse"] = df["warehouse"].astype(str)
            data = df.to_dict("records")
            appointment_ids = [d["appointment_id"].strip() for d in data]
            if len(appointment_ids) != len(set(appointment_ids)):
                raise RuntimeError("appointment id 重复！")
            existed_shipments = await sync_to_async(list)(Shipment.objects.filter(appointment_id__in=appointment_ids))
            if existed_shipments:
                raise ValueError(f"Appointment {existed_shipments} already created!")
            cleaned_data = [{
                "appointment_id": d["appointment_id"].strip(),
                "destination": d["destination"].upper().strip(),
                "shipment_appointment": d["scheduled_time"],
                "origin": d["warehouse"].upper().strip() if d["warehouse"] != "nan" else None,
                "in_use": False,
            } for d in data]
            await sync_to_async(Shipment.objects.bulk_create)(Shipment(**d) for d in cleaned_data)
        return await self.handle_appointment_warehouse_search_post(request)

    async def _get_packing_list(
        self, 
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        data = []
        if plt_criteria:
            pal_list = await sync_to_async(list)(
                Pallet.objects.prefetch_related(
                    "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number"
                    "container_number__order__offload_id", "container_number__order__customer_name", "container_number__order__retrieval_id"
                ).filter(
                    plt_criteria
                ).annotate(
                    schedule_status=Case(
                        When(Q(container_number__order__offload_id__offload_at__lte=datetime.now().date() + timedelta(days=-7)), then=Value("past_due")),
                        default=Value("on_time"),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                ).values(
                    'container_number__container_number',
                    'container_number__order__customer_name__zem_name',
                    'destination',
                    'address',
                    'delivery_method',
                    'container_number__order__offload_id__offload_at',
                    'schedule_status',
                    'abnormal_palletization',
                    'po_expired',
                    warehouse=F('container_number__order__retrieval_id__retrieval_destination_precise'),
                ).annotate(
                    custom_delivery_method=F('delivery_method'),
                    fba_ids=F('fba_id'),
                    ref_ids=F('ref_id'),
                    shipping_marks=F('shipping_mark'),
                    plt_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=IntegerField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=IntegerField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                ).order_by('container_number__order__offload_id__offload_at')
            )
            data += pal_list
        if pl_criteria:
            pl_list =  await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number"
                    "container_number__order__offload_id", "container_number__order__customer_name", "pallet", "container_number__order__retrieval_id"
                ).filter(pl_criteria).annotate(
                    custom_delivery_method=Case(
                        When(Q(delivery_method='暂扣留仓(HOLD)') | Q(delivery_method='暂扣留仓'), then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                        default=F('delivery_method'),
                        output_field=CharField()
                    ),
                    schedule_status=Case(
                        When(Q(container_number__order__offload_id__offload_at__lte=datetime.now().date() + timedelta(days=-7)), then=Value("past_due")),
                        default=Value("on_time"),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField())
                ).values(
                    'container_number__container_number',
                    'container_number__order__customer_name__zem_name',
                    'destination',
                    'address',
                    'custom_delivery_method',
                    'container_number__order__offload_id__offload_at',
                    'schedule_status',
                    warehouse=F('container_number__order__retrieval_id__retrieval_destination_precise'),
                ).annotate(
                    fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
                    ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
                    shipping_marks=StringAgg("str_shipping_mark", delimiter=",", distinct=True, ordering="str_shipping_mark"),
                    ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField()
                        )
                    ),
                    total_cbm=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("cbm")),
                            default=F("pallet__cbm"),
                            output_field=FloatField()
                        )
                    ),
                    total_weight_lbs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("total_weight_lbs")),
                            default=F("pallet__weight_lbs"),
                            output_field=FloatField()
                        )
                    ),
                    total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
                    label=Max(
                        Case(
                            When(pallet__isnull=True, then=Value("EST")),
                            default=Value("ACT"),
                            output_field=CharField()
                        )
                    ),
                ).distinct()
            )
            data += pl_list
        return data
    
    async def _get_sharepoint_auth(self) -> ClientContext:
        return ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))

    async def _shipment_exist(self, batch_number: str) -> bool:
        if await sync_to_async(list)(Shipment.objects.filter(shipment_batch_number=batch_number)):
            return True
        else:
            return False
        
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False