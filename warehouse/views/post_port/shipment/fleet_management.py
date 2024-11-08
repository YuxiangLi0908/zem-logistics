import ast
import uuid
import os,json
import pandas as pd
from asgiref.sync import sync_to_async
from datetime import datetime
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import CharField, Sum, FloatField, Count
from django.db.models.functions import Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template
from django.utils import timezone

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    APP_ENV,
    SP_USER,
    SP_PASS,
    SP_URL,
    SP_DOC_LIB,
    SYSTEM_FOLDER,
)


class FleetManagement(View):
    template_fleet = "post_port/shipment/03_fleet_main.html"
    template_fleet_schedule = "post_port/shipment/03_1_fleet_schedule.html"
    template_fleet_schedule_info = "post_port/shipment/03_2_fleet_schedule_info.html"
    template_outbound = "post_port/shipment/04_outbound_main.html"
    template_outbound_departure = "post_port/shipment/04_outbound_depature_confirmation.html"
    template_delivery_and_pod = "post_port/shipment/05_1_delivery_and_pod.html"
    template_pod_upload = "post_port/shipment/05_2_delivery_and_pod.html"
    template_bol = "export_file/bol_base_template.html"
    template_abnormal_fleet_warehouse_search = "post_port/shipment/abnormal/01_fleet_management_main.html"
    area_options = {"NJ": "NJ", "SAV": "SAV"}
    warehouse_options = {"": "", "NJ-07001": "NJ-07001", "NJ-08817": "NJ-08817", "SAV-31326": "SAV-31326"}
    shipment_type_options = {"":"", "FTL/LTL":"FTL/LTL", "外配/快递":"外配/快递"}
    abnormal_fleet_options = {"":"", "司机未按时提货":"司机未按时提货", "送仓被拒收":"送仓被拒收", "未送达":"未送达", "其它":"其它"}
    carrier_options = {"":"", "Arm-AMF":"Arm-AMF", "Zem-AMF":"Zem-AMF", "ASH":"ASH", "Arm":"Arm", "ZEM":"ZEM"}

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "outbound":
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
        elif step == "pod_upload":
            template, context = await self.handle_pod_upload_get(request)
            return render(request, template, context)
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_fleet_warehouse_search, context)

    async def post(self, request: HttpRequest) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "fleet_warehouse_search":
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
        elif step == "fleet_pod_search":
            mutable_get = request.GET.copy()
            mutable_get["fleet_number"] = request.POST.get("fleet_number", None)
            mutable_get["batch_number"] = request.POST.get("batch_number", None)
            request.GET = mutable_get
            template, context = await self.handle_pod_upload_get(request)
            return render(request, template, context)
        elif step == "confirm_delivery":
            template, context = await self.handle_confirm_delivery_post(request)
            return render(request, template, context)
        elif step == "pod_upload":
            template, context = await self.handle_pod_upload_post(request)
            return render(request, template, context)
        elif step == "abnormal_fleet":
            template, context = await self.handle_abnormal_fleet_post(request)
            return render(request, template, context)
        else:
            return await self.get(request)
    
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
            "carrier_options": self.carrier_options,
        })
        return self.template_fleet_schedule_info, context
    
    async def handle_fleet_depature_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        selected_fleet_number = request.GET.get("fleet_number")
        warehouse = request.GET.get("warehouse")
        selected_fleet = await sync_to_async(Fleet.objects.get)(fleet_number=selected_fleet_number)
        shipment = await sync_to_async(list)(
            Pallet.objects.select_related(
                "shipment_batch_number",
                "shipment_batch_number__fleet_number", 
                "container_number",
                "container_number__order__offload_id"
            ).filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__order__offload_id__offload_at__isnull=False
            ).values(
                "shipment_batch_number__shipment_batch_number", "container_number__container_number", 
                "destination", "shipment_batch_number__appointment_id", "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__note",
            ).annotate(
                plt_ids=StringAgg("pallet_id", delimiter=",", distinct=True, ordering="pallet_id"),
                total_weight=Sum("weight_lbs", output_field=FloatField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_pallet=Count('pallet_id', distinct=True)
            ).order_by("-shipment_batch_number__shipment_appointment")
        )
        shipment_pl = await sync_to_async(list)(
            PackingList.objects.select_related(
                "shipment_batch_number",
                "shipment_batch_number__fleet_number", 
                "container_number",
                "container_number__order__offload_id"
            ).annotate(
                str_id=Cast("id", CharField()),
            ).filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__order__offload_id__offload_at__isnull=True
            ).values(
                "shipment_batch_number__shipment_batch_number", "container_number__container_number", 
                "destination", "shipment_batch_number__appointment_id", "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__note",
            ).annotate(
                pl_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                total_weight=Sum("total_weight_lbs", output_field=FloatField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_pallet=Sum("cbm", output_field=FloatField())/2,
            ).order_by("-shipment_batch_number__shipment_appointment")
        )
        for s in shipment_pl:
            if s["total_pallet"] < 1:
                s["total_pallet"] = 1
            elif s["total_pallet"]%1 >= 0.45:
                s["total_pallet"] = int(s["total_pallet"]//1 + 1)
            else:
                s["total_pallet"] = int(s["total_pallet"]//1)
        shipment += shipment_pl
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
            arrived_at__isnull=True,
            is_canceled=False,
        )
        if fleet_number:
            criteria &= models.Q(fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment__shipment_batch_number=batch_number)
        fleet = await sync_to_async(list)(
            Fleet.objects.prefetch_related("shipment").filter(criteria).annotate(
                shipment_batch_numbers=StringAgg("shipment__shipment_batch_number", delimiter=","),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            ).order_by("departured_at")
        )
        fleet_numbers = [f.fleet_number for f in fleet]
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number").filter(fleet_number__fleet_number__in=fleet_numbers)
        )
        shipment_fleet_dict = {}
        for s in shipment:
            if s.fleet_number.fleet_number not in shipment_fleet_dict:
                shipment_fleet_dict[s.fleet_number.fleet_number] = [{
                    "shipment_batch_number": s.shipment_batch_number,
                    "appointment_id": s.appointment_id,
                    "destination": s.destination,
                    "carrier": s.carrier,
                    "shipment_appointment": s.shipment_appointment.replace(microsecond=0).isoformat(),
                    "origin": s.origin,
                }]
            else:
                shipment_fleet_dict[s.fleet_number.fleet_number].append({
                    "shipment_batch_number": s.shipment_batch_number,
                    "appointment_id": s.appointment_id,
                    "destination": s.destination,
                    "carrier": s.carrier,
                    "shipment_appointment": s.shipment_appointment.replace(microsecond=0).isoformat(),
                    "origin": s.origin,
                })
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": fleet,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "shipment": json.dumps(shipment_fleet_dict),
        }
        return self.template_delivery_and_pod, context
    
    async def handle_pod_upload_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number", "")
        batch_number = request.GET.get("batch_number", "")
        criteria = models.Q(
            models.Q(models.Q(pod_link__isnull=True) | models.Q(pod_link="")),
            shipped_at__isnull=False,
            arrived_at__isnull=False,
        )
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number").filter(criteria).order_by("shipped_at")
        )
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": shipment,
            "upload_file_form": UploadFileForm(required=True),
        }
        return self.template_pod_upload, context
    
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
                is_canceled=False,
            ).prefetch_related(
                "shipment"
            ).annotate(
                shipment_batch_numbers=StringAgg("shipment__shipment_batch_number", delimiter=","),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
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
                "carrier_options": self.carrier_options,
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
                is_canceled=False,
            ).prefetch_related(
                "shipment"
            ).annotate(
                shipment_batch_numbers=StringAgg("shipment__shipment_batch_number", delimiter=","),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
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
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__order__offload_id__offload_at__isnull=True,
                ).values(
                    "container_number__container_number", "destination", "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment"
                ).annotate(
                    total_weight=Sum("pallet__weight_lbs"),
                    total_cbm=Sum("pallet__cbm"),
                    total_n_pallet=Count("pallet__pallet_id", distinct=True),
                ).order_by("-shipment_batch_number__shipment_appointment")
            )
            packing_list += await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number", "shipment_batch_number"
                ).filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__order__offload_id__offload_at__isnull=False,
                ).values(
                    "container_number__container_number", "destination", "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment"
                ).annotate(
                    total_weight=Sum("weight_lbs"),
                    total_cbm=Sum("cbm"),
                    total_n_pallet=Count("pallet_id", distinct=True),
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
        pallet: list[Pallet] | None = None
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
                    shipment_batch_number__shipment_batch_number=batch_number,
                    container_number__order__offload_id__offload_at__isnull=True,
                )
            )
            packing_list += await sync_to_async(list)(
                Pallet.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=batch_number,
                    container_number__order__offload_id__offload_at__isnull=False,
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
        # raise ValueError(shipment_pallet)
        updated_shipment = []
        updated_pallet = []
        sub_shipment = {s.shipment_batch_number: None for s in shipment}
        fleet_shipped_weight, fleet_shipped_cbm, fleet_shipped_pallet, fleet_shipped_pcs = 0, 0, 0, 0
        for s in shipment:
            if shipment_pallet.get(s.shipment_batch_number):
                dumped_pallets = len(set([p.pallet_id for p in shipment_pallet.get(s.shipment_batch_number)]))
            else:
                dumped_pallets = 0
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
        arrived_at = request.POST.get("arrived_at")
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        fleet.arrived_at = arrived_at
        updated_shipment = []
        for s in shipment:
            s.arrived_at = arrived_at
            s.is_arrived = True
            updated_shipment.append(s)
        await sync_to_async(fleet.save)()
        await sync_to_async(Shipment.objects.bulk_update)(
            updated_shipment,
            ["arrived_at", "is_arrived"]
        )
        return await self.handle_delivery_and_pod_get(request)

    async def handle_pod_upload_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        conn = await self._get_sharepoint_auth()
        pod_form = UploadFileForm(request.POST, request.FILES)
        shipment_batch_number = request.POST.get("shipment_batch_number")
        shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
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
        await sync_to_async(shipment.save)()
        return await self.handle_pod_upload_get(request)
    
    async def handle_abnormal_fleet_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        status = request.POST.get("abnormal_status", "").strip()
        description = request.POST.get("abnormal_description", "").strip()
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        fleet.is_canceled = True
        fleet.status = "Exception"
        fleet.status_description = f"{status}-{description}"
        
        for s in shipment:
            if not s.previous_fleets:
                s.previous_fleets = fleet_number
            else:
                s.previous_fleets += f",{fleet_number}"
            s.status = "Exception"
            s.status_description = f"{status}-{description}"
            s.fleet_number = None
            await sync_to_async(s.save)()
        await sync_to_async(fleet.save)()
        return await self.handle_delivery_and_pod_get(request)
        
    # async def handle_fleet_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
    #     warehouse = request.POST.get("name")
    #     fleet = await sync_to_async(list)(
    #         Fleet.objects.select_related("shipment").filter(
    #             origin=warehouse,
    #             is_canceled=False,
    #         ).values(
    #             "fleet_number", "appointment_datetime",
    #         ).annotate(
    #             shipment_batch_number=StringAgg("shipment__shipment_batch_number", delimiter=",", distinct=True),
    #             appointment_id=StringAgg("shipment__appointment_id", delimiter=",", distinct=True),
    #             destination=StringAgg("shipment__destination", delimiter=",", distinct=True),
    #         ).order_by("appointment_datetime")
    #     )
    #     context = {
    #         "warehouse": warehouse,
    #         "fleet": fleet,
    #         "warehouse_form": ZemWarehouseForm(initial={"name": warehouse})
    #     }
    #     return self.template_fleet_warehouse_search, context

    async def _get_sharepoint_auth(self) -> ClientContext:
        return ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False