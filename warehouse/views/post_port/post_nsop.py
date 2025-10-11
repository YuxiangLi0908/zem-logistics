from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import json
import uuid
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Round, Cast, Coalesce
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    Func,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
import asyncio
from django.core.exceptions import MultipleObjectsReturned
from django.db.models.functions import Cast, Concat
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from django.utils import timezone
from datetime import timedelta
from dateutil.parser import parse

from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.fleet import Fleet
from warehouse.models.order import Order
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.shipment import Shipment
from django.contrib import messages

from warehouse.views.post_port.shipment.fleet_management import FleetManagement
from warehouse.views.post_port.shipment.shipping_management import ShippingManagement
from warehouse.utils.constants import (
    LOAD_TYPE_OPTIONS,
    amazon_fba_locations,
)

class PostNsop(View):
    template_main_dash = "post_port/new_sop/01_appointment_management.html"
    template_td_shipment = "post_port/new_sop/02_td_shipment.html"
    template_fleet_schedule = "post_port/new_sop/03_fleet_schedule.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    warehouse_options = {"":"", "NJ-07001": "NJ-07001", "SAV-31326": "SAV-31326", "LA-91761": "LA-91761"}
    account_options = {
        "": "",
        "Carrier Central1": "Carrier Central1",
        "Carrier Central2": "Carrier Central2",
        "ZEM-AMF": "ZEM-AMF",
        "ARM-AMF": "ARM-AMF",
        "walmart": "walmart",
    }
    abnormal_fleet_options = {
        "": "",
        "司机未按时提货": "司机未按时提货",
        "送仓被拒收": "送仓被拒收",
        "未送达": "未送达",
        "其它": "其它",
    }
    carrier_options = {
        "": "",
        "Arm-AMF": "Arm-AMF",
        "Zem-AMF": "Zem-AMF",
        "ASH": "ASH",
        "Arm": "Arm",
        "ZEM": "ZEM",
        "LiFeng": "LiFeng",
    }
    shipment_type_options = {
        "FTL": "FTL",
        "LTL": "LTL",
        "外配": "外配",
        "快递": "快递",
        "客户自提": "客户自提",
    }

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "appointment_management":
            template, context = await self.handle_appointment_management_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "schedule_shipment":
            template, context = await self.handle_td_shipment_get(request)
            return render(request, template, context)
        elif step == "fleet_management":
            template, context = await self.handle_fleet_management_get(request)
            return render(request, template, context)
        else:
            raise ValueError('输入错误')

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "appointment_management_warehouse":
            template, context = await self.handle_appointment_management_post(request)
            return render(request, template, context)
        elif step == "td_shipment_warehouse":
            template, context = await self.handle_td_shipment_post(request)
            return render(request, template, context)
        elif step == "fleet_schedule_warehouse":
            template, context = await self.handle_fleet_schedule_post(request)
            return render(request, template, context)
        elif step == "export_pos":
            return await self.handle_export_pos(request)
        elif step == "appointment_time_modify":
            template, context = await self.handle_appointment_time(request)
            return render(request, template, context)
        elif step == "update_fleet":
            fm = FleetManagement()
            context = await fm.handle_update_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": "更新出库成功!"}) 
            return render(request, template, context)
        elif step == "fleet_confirmation":
            template, context = await self.handle_fleet_confirmation_post(request)
            return render(request, template, context) 
        elif step == "cancel_fleet":
            fm = FleetManagement()
            context = await fm.handle_cancel_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '取消批次成功!'})  
            return render(request, template, context)
        elif step == "confirm_delivery":
            fm = FleetManagement()
            context = await fm.handle_confirm_delivery_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '确认送达成功!'})  
            return render(request, template, context)
        elif step == "abnormal_fleet":
            fm = FleetManagement()
            context = await fm.handle_abnormal_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '异常处理成功!'})  
            return render(request, template, context)
        elif step == "pod_upload":
            fm = FleetManagement()
            context = await fm.handle_pod_upload_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": 'POD上传成功!'})           
            return render(request, template, context)
        elif step == "bind_group_shipment":
            template, context = await self.handle_appointment_post(request)
            return render(request, template, context) 
        else:
            raise ValueError('输入错误',step)
    
    async def generate_unique_batch_number(self,destination):
        """生成唯一的shipment_batch_number"""
        current_time = datetime.now()
        
        for i in range(10):
            batch_id = (
                destination
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            batch_number = batch_id.replace(" ", "").replace("/", "-").upper()       
            exists = await sync_to_async(
                Shipment.objects.filter(shipment_batch_number=batch_number).exists
            )()
            
            if not exists:
                return batch_number
        raise ValueError('批次号始终重复')
    
    async def handle_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        
        destination = request.POST.get('destination')     
        shipment_batch_number = await self.generate_unique_batch_number(destination)
        ids = request.POST.get("cargo_ids")
        plt_ids = request.POST.get("plt_ids")
        selected = [int(i) for i in ids.split(",") if i]
        selected_plt = [int(i) for i in plt_ids.split(",") if i]
        
        if selected or selected_plt:
            packing_list_selected = await self._get_packing_list(
                models.Q(id__in=selected)
                & models.Q(shipment_batch_number__isnull=True),
                models.Q(id__in=selected_plt)
                & models.Q(shipment_batch_number__isnull=True),
            )
            total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
            for pl in packing_list_selected:
                total_weight += (
                    pl.get("total_weight_lbs") if pl.get("total_weight_lbs") else 0
                )
                total_cbm += pl.get("total_cbm") if pl.get("total_cbm") else 0
                total_pcs += pl.get("total_pcs") if pl.get("total_pcs") else 0
                if pl.get("label") == "ACT":
                    total_pallet += pl.get("total_n_pallet_act")
                else:
                    if pl.get("total_n_pallet_est < 1"):
                        total_pallet += 1
                    elif pl.get("total_n_pallet_est") % 1 >= 0.45:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1 + 1)
                    else:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1)
        shipment_data = {
            'shipment_batch_number': shipment_batch_number,
            'pl_ids': selected,
            'plt_ids': selected_plt,
            'destination': destination,
            'total_cbm': total_cbm,
            'total_pallet': total_pallet,
            'total_pcs': total_pcs,
            'total_pallet': total_pallet,
            'shipment_type': request.POST.get('shipment_type'),
            'shipment_account': request.POST.get('shipment_account'),
            'appointment_id': request.POST.get('appointment_id'),
            'shipment_appointment': request.POST.get('shipment_appointment'),
            'load_type': '卡板' if request.POST.get('st_type') == 'pallet' else '地板',
            'origin': request.POST.get('warehouse'),
            'note': request.POST.get('note'),
            'address': request.POST.get('address'),
        }
        request.POST = request.POST.copy()
        request.POST['shipment_data'] = str(shipment_data)
        request.POST['batch_number'] = shipment_batch_number     
        request.POST['pl_ids'] = selected
        request.POST['plt_ids'] = selected_plt
        request.POST['type'] = 'td'
        sm = ShippingManagement()
        info = await sm.handle_appointment_post(request,'post_nsop')        
        return await self.handle_td_shipment_post(request)

    async def handle_fleet_confirmation_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selected_ids_str = request.POST.get("selected_ids")
        error_message = None
        if selected_ids_str:
            try:
                selected_ids_list = json.loads(selected_ids_str)
                selected_ids = [int(id) for id in selected_ids_list]
            except (json.JSONDecodeError, ValueError) as e:
                # 处理解析错误
                error_message = f"selected_ids 参数格式错误: {e}"
                # 根据你的错误处理方式选择
                raise ValueError(error_message)
        if selected_ids:
            #先生成fleet_number
            current_time = datetime.now()
            fleet_number = (
                "F"
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            shipment_selected = await sync_to_async(list)(
                Shipment.objects.filter(id__in=selected_ids)
            )
            fleet_type = None
            shipment_types = set()
            total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
            for s in shipment_selected:
                shipment_types.add(s.shipment_type)
                total_weight += s.total_weight
                total_cbm += s.total_cbm
                total_pcs += s.total_pcs
                total_pallet += s.total_pallet
            if len(shipment_types) > 1:
                error_message = f"选中的预约批次包含不同的 shipment_type: {list(shipment_types)}"
                raise ValueError(error_message)
            else:
                fleet_type = shipment_selected[0].shipment_type if shipment_selected else None
            fleet_data_dict = {
                'fleet_number': fleet_number,
                'fleet_type': fleet_type,
                'origin': request.POST.get('warehouse'),
                'total_weight': total_weight,
                'total_cbm': total_cbm,
                'total_pallet': total_pallet,
                'total_pcs': total_pcs,
            }
            
        request.POST = request.POST.copy()
        request.POST['fleet_data'] = str(fleet_data_dict)
        request.POST['selected_ids'] = selected_ids
        fm = FleetManagement()
        info = await fm.handle_fleet_confirmation_post(request,'post_nsop')
        context = {}
        if error_message:
            context.update({"error_messages": error_message}) 
        _, context = await self.handle_fleet_schedule_post(request, context)
        context.update({"success_messages": f'排车成功!批次号是：{fleet_number}'}) 
        return await self.handle_fleet_schedule_post(request, context)

    async def handle_appointment_time(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointmentId = request.POST.get("appointmentId")
        shipment = await sync_to_async(Shipment.objects.get)(
            appointment_id=appointmentId
        )
        operation = request.POST.get("operation")
        if operation == "edit":
            appointmentTime = request.POST.get("appointmentTime")
            naive_datetime = parse(appointmentTime).replace(tzinfo=None)
            shipment.shipment_appointment = naive_datetime
            await sync_to_async(shipment.save)()
        elif operation == "delete":
            shipment.is_canceled = True
            await sync_to_async(shipment.delete)()
        return await self.handle_appointment_management_post(request)
       
    async def handle_export_pos(self, request: HttpRequest) -> HttpResponse:
        packinglist_ids = request.POST.getlist("cargo_ids")
        pallet_ids = request.POST.getlist("plt_ids")

        if packinglist_ids and packinglist_ids[0]:
            packinglist_ids = packinglist_ids[0].split(',')
        else:
            packinglist_ids = []

        if pallet_ids and pallet_ids[0]:
            pallet_ids = pallet_ids[0].split(',')
        else:
            pallet_ids = []
        all_data = []

        if packinglist_ids:
            packing_list_data = await sync_to_async(list)(
                PackingList.objects.select_related("container_number", "pallet")
                .filter(id__in=packinglist_ids)
                .values(
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "destination",
                    "delivery_method",
                    "container_number__container_number",
                    "shipping_mark",
                )
                .annotate(
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField(),
                        )
                    ),
                    total_cbm=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("cbm")),
                            default=F("pallet__cbm"),
                            output_field=FloatField(),
                        )
                    ),
                    total_weight_lbs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("total_weight_lbs")),
                            default=F("pallet__weight_lbs"),
                            output_field=FloatField(),
                        )
                    ),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                    label=Max(
                        Case(
                            When(pallet__isnull=True, then=Value("EST")),
                            default=Value("ACT"),
                            output_field=CharField(),
                        )
                    ),
                )
                .distinct()
                .order_by("destination", "container_number__container_number")
            )
            all_data += packing_list_data
        if pallet_ids:
            pallet_data = await sync_to_async(list)(
                Pallet.objects.select_related("container_number")
                .filter(id__in=pallet_ids)
                .values(
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "destination",
                    "delivery_method",
                    "container_number__container_number",
                    "shipping_mark",
                )
                .annotate(
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                )
                .distinct()
                .order_by("destination", "container_number__container_number")
            )
            all_data += pallet_data
        for p in all_data:
            try:
                pl = await sync_to_async(PoCheckEtaSeven.objects.get)(
                    container_number__container_number=p["container_number__container_number"],
                    shipping_mark=p["shipping_mark"],
                    fba_id=p["fba_id"],
                    ref_id=p["ref_id"],
                )

                if not pl.last_eta_checktime and not pl.last_retrieval_checktime:
                    p["check"] = "未校验"
                elif pl.last_retrieval_checktime and not pl.last_retrieval_status:
                    if pl.handling_method:
                        p["check"] = "失效," + str(pl.handling_method)
                    else:
                        p["check"] = "失效未处理"
                elif (
                    not pl.last_retrieval_checktime
                    and pl.last_eta_checktime
                    and not pl.last_eta_status
                ):
                    if pl.handling_method:
                        p["check"] = "失效," + str(pl.handling_method)
                    else:
                        p["check"] = "失效未处理"
                else:
                    p["check"] = "有效"
            except PoCheckEtaSeven.DoesNotExist:
                p["check"] = "未找到记录"
            except MultipleObjectsReturned:
                p["check"] = "唛头FBA_REF重复"
        data = [i for i in all_data]
        export_format = request.POST.get("export_format", "PO")
        if export_format == "PO":
            keep = [
                "fba_id",
                "container_number__container_number",
                "ref_id",
                "Pallet Count",
                "total_pcs",
                "label",
                "check",
            ]
        elif export_format == "FULL_TABLE":
            keep = [
                "container_number__container_number",
                "destination",
                "delivery_method",
                "fba_id",
                "ref_id",
                "total_cbm",
                "total_pcs",
                "total_weight_lbs",
                "Pallet Count",
                "label",
                "check",
            ]
        else:
            raise ValueError(f"unknown export_format option: {export_format}")
        df = pd.DataFrame.from_records(data)

        if "total_n_pallet_est" not in df.columns:
            df["total_n_pallet_est"] = 0
        if "total_n_pallet_act" not in df.columns:
            df["total_n_pallet_act"] = 0
        def get_est_pallet(n):
            if pd.isna(n) or n is None:
                return 0
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)

        if "total_n_pallet_est" in df.columns:
            df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
        df["est"] = df["label"] == "EST"
        df["act"] = df["label"] == "ACT"

        df["Pallet Count"] = (
            df.get("total_n_pallet_act", 0) * df["act"]
            + df.get("total_n_pallet_est", 0) * df["est"]
        )
        df = df[keep].rename(
            {
                "fba_id": "PRO",
                "container_number__container_number": "BOL",
                "ref_id": "PO List (use , as separator) *",
                "total_pcs": "Carton Count",
            },
            axis=1,
        )
        if export_format == "FULL_TABLE":
            df = df.rename(
                {
                    "total_cbm": "CBM",
                    "total_weight_lbs": "WEIGHT(LBS)",
                },
                axis=1,
            )
        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = f"attachment; filename=PO.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response

    async def handle_appointment_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_main_dash, context

    async def handle_td_shipment_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_td_shipment, context
    
    async def handle_fleet_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_fleet_schedule, context
    
    async def _fl_unscheduled_data(
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                origin=warehouse,
                fleet_number__isnull=True,
                in_use=True,
                is_canceled=False,
                shipment_type="FTL",
            ).order_by("-batch", "shipment_appointment")
        )
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(
                origin=warehouse,
                departured_at__isnull=True,
                is_canceled=False,
            )
            .prefetch_related("shipment")
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )
        context = {
            "shipment_list": shipment,
            "fleet_list": fleet,
        }
        return context

    async def _fl_delivery_get(
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:
        criteria = models.Q(
            is_arrived=False,
            is_canceled=False,
            is_shipped=True,
            origin=warehouse,
            shipment_type="FTL",
        ) & ~Q(status="Exception")
        shipments = await sync_to_async(list)(
            Shipment.objects.prefetch_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        shipment_fleet_dict = {}
        for s in shipments:
            if s.fleet_number is None:
                continue
            if s.shipment_appointment is None:
                shipment_appointment = ""
            else:
                shipment_appointment = s.shipment_appointment.replace(
                    microsecond=0
                ).isoformat()
            if s.fleet_number.fleet_number not in shipment_fleet_dict:
                shipment_fleet_dict[s.fleet_number.fleet_number] = [
                    {
                        "shipment_batch_number": s.shipment_batch_number,
                        "appointment_id": s.appointment_id,
                        "destination": s.destination,
                        "carrier": s.carrier,
                        "shipment_appointment": shipment_appointment,
                        "origin": s.origin,
                    }
                ]
            else:
                shipment_fleet_dict[s.fleet_number.fleet_number].append(
                    {
                        "shipment_batch_number": s.shipment_batch_number,
                        "appointment_id": s.appointment_id,
                        "destination": s.destination,
                        "carrier": s.carrier,
                        "shipment_appointment": shipment_appointment,
                        "origin": s.origin,
                    }
                )
        context = {
            "shipments": shipments, #待确认送达批次
            "abnormal_fleet_options": self.abnormal_fleet_options,
            #"shipment": json.dumps(shipment_fleet_dict),
        }
        return context
    
    async def _fl_pod_get(
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:

        criteria = models.Q(
            models.Q(models.Q(pod_link__isnull=True) | models.Q(pod_link="")),
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_type='FTL',
            shipment_schduled_at__gte="2024-12-01",
            origin=warehouse,
        )
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        context = {
            "fleet": shipment,
        }
        return context
    
    async def handle_fleet_schedule_post(
        self, request: HttpRequest, context: dict| None = None
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")

        # 获取三类数据：未排车、待送达、待传POD
        sp_fl = await self._fl_unscheduled_data(request, warehouse)
        delivery_data = await self._fl_delivery_get(request, warehouse)
        pod_data = await self._fl_pod_get(request, warehouse)

        summary = {
            'unscheduled_count': len(sp_fl['shipment_list']),
            'scheduled_count': len(sp_fl['fleet_list']),
            'ready_count': len(delivery_data['shipments']),
            'pod_count': len(pod_data['fleet']),
        }
        if not context:
            context = {}
        context.update({
            'shipment_list': sp_fl['shipment_list'],
            'fleet_list': sp_fl['fleet_list'],
            'delivery_shipments': delivery_data['shipments'],
            'pod_shipments': pod_data['fleet'],
            'summary': summary,        
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "warehouse": warehouse,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            'current_time': timezone.now(),
        })     
        return self.template_fleet_schedule, context
    
    async def handle_td_shipment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        st_type = request.POST.get("st_type", "pallet")
        
        # 获取三类数据：未排约、已排约、待出库
        matching_suggestions = await self.sp_unscheduled_data(warehouse, st_type)
        scheduled_data = await self.sp_scheduled_data(warehouse)
        ready_to_ship_data = await self._sp_ready_to_ship_data(warehouse)
        
        # 获取可用预约
        available_shipments = await self.sp_available_shipments(warehouse, st_type)
        
        # 计算统计数据
        summary = await self._sp_calculate_summary(matching_suggestions, scheduled_data, ready_to_ship_data)
        
        # 生成匹配建议
        max_cbm, max_pallet = await self.get_capacity_limits(st_type)
        context = {
            'warehouse': warehouse,
            'st_type': st_type,
            'matching_suggestions': matching_suggestions,
            'scheduled_data': scheduled_data,
            'ready_to_ship_data': ready_to_ship_data,
            'available_shipments': available_shipments,
            'summary': summary,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "shipment_type_options": self.shipment_type_options,
        } 
        return self.template_td_shipment, context
    
    async def sp_unscheduled_data(self, warehouse: str, st_type: str) -> list:
        """获取未排约数据"""
        unshipment_pos = await self._get_packing_list(
            models.Q(
                container_number__order__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=False,
                location=warehouse,
                delivery_type='public',
            )& ~models.Q(delivery_method__contains='暂扣'),
        )
        
        # 获取可用的shipment记录（shipment_batch_number为空的）
        shipments = await self.get_available_shipments(warehouse)
        
        # 生成智能匹配建议
        matching_suggestions = await self.generate_matching_suggestions(unshipment_pos, shipments)
        
        # 只返回匹配建议，不返回原始未排约数据
        return matching_suggestions

    async def get_available_shipments(self, warehouse: str):
        """获取可用的shipment记录"""
        now = timezone.now()
        # 这里需要根据您的实际模型调整查询条件
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(
                models.Q(shipment_batch_number__isnull=True) | models.Q(shipment_batch_number=''),
                in_use=False,
                is_canceled=False,
                shipment_appointment__gt=now  
            ).order_by('shipment_appointment') 
        )
        return shipments

    async def generate_matching_suggestions(self, unshipment_pos, shipments, max_cbm=33, max_pallet=26):
        """生成智能匹配建议 - 基于功能A的逻辑但适配shipment匹配"""
        suggestions = []

        # 第一级分组：按目的地和派送方式预分组
        pre_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = cargo.get('destination')
                delivery_method = cargo.get('custom_delivery_method')
                if not dest or not delivery_method:
                    continue
                    
                group_key = f"{dest}_{delivery_method}"
                if group_key not in pre_groups:
                    pre_groups[group_key] = {
                        'destination': dest,
                        'delivery_method': delivery_method,
                        'cargos': []
                    }
                pre_groups[group_key]['cargos'].append(cargo)
        
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__order__vessel_id__vessel_eta') or '')
            
            # 按容量限制创建大组
            primary_groups = []
            current_primary_group = {
                'destination': pre_group['destination'],
                'delivery_method': pre_group['delivery_method'],
                'cargos': [],
                'total_pallets': 0,
                'total_cbm': 0,
            }
            
            for cargo in sorted_cargos:
                cargo_pallets = 0
                if cargo.get('label') == 'ACT':
                    cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                else:
                    cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                
                cargo_cbm = cargo.get('total_cbm', 0) or 0
                
                # 检查当前大组是否还能容纳这个货物
                if (current_primary_group['total_pallets'] + cargo_pallets <= max_pallet and 
                    current_primary_group['total_cbm'] + cargo_cbm <= max_cbm):
                    # 可以加入当前大组
                    current_primary_group['cargos'].append(cargo)
                    current_primary_group['total_pallets'] += cargo_pallets
                    current_primary_group['total_cbm'] += cargo_cbm
                else:
                    # 当前大组已满，保存并创建新的大组
                    if current_primary_group['cargos']:
                        primary_groups.append(current_primary_group)
                    
                    # 创建新的大组
                    current_primary_group = {
                        'destination': pre_group['destination'],
                        'delivery_method': pre_group['delivery_method'],
                        'cargos': [cargo],
                        'total_pallets': cargo_pallets,
                        'total_cbm': cargo_cbm,
                    }
            
            # 添加最后一个大组
            if current_primary_group['cargos']:
                primary_groups.append(current_primary_group)
            
            # 为每个大组寻找匹配的shipment
            for primary_group_index, primary_group in enumerate(primary_groups):
                # 计算大组的匹配度百分比
                pallets_percentage = min(100, (primary_group['total_pallets'] / max_pallet) * 100) if max_pallet > 0 else 0
                cbm_percentage = min(100, (primary_group['total_cbm'] / max_cbm) * 100) if max_cbm > 0 else 0
                
                # 寻找匹配的shipment
                matched_shipment = await self.find_matching_shipment(primary_group, shipments)
                
                # 无论是否匹配到shipment，都创建建议分组
                suggestion = {
                    'suggestion_id': f"{group_key}_{primary_group_index}",
                    'primary_group': {
                        'destination': primary_group['destination'],
                        'delivery_method': primary_group['delivery_method'],
                        'total_pallets': primary_group['total_pallets'],
                        'total_cbm': primary_group['total_cbm'],
                        'pallets_percentage': pallets_percentage,
                        'cbm_percentage': cbm_percentage,
                        'matched_shipment': matched_shipment,  # 可能为None
                        'suggestion_id': f"{group_key}_{primary_group_index}"
                    },
                    'cargos': [{
                        'ids': cargo.get('ids', ''),
                        'plt_ids': cargo.get('plt_ids', ''),
                        'ref_ids': cargo.get('ref_ids', ''),
                        'fba_ids': cargo.get('fba_ids', ''),
                        'container_numbers': cargo.get('container_numbers', ''),
                        'cns': cargo.get('cns', ''),
                        'delivery_window_start': cargo.get('delivery_window_start'),
                        'delivery_window_end': cargo.get('delivery_window_end'),
                        'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                        'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                        'total_cbm': cargo.get('total_cbm', 0),
                        'label': cargo.get('label', ''),
                    } for cargo in primary_group['cargos']]
                }
                suggestions.append(suggestion)
        
        return suggestions

    async def find_matching_shipment(self, primary_group, shipments):
        """为货物大组寻找匹配的shipment"""
        destination = primary_group['destination']
        matched_shipments = []
        
        for shipment in shipments:
            # 检查目的地是否匹配
            if shipment.destination != destination:
                continue
            # 检查时间窗口条件
            if await self.check_time_window_match(primary_group, shipment):
                matched_shipments.append(shipment)

        # 这里简单返回第一个匹配的，您可以根据需要调整策略
        if matched_shipments:
            matched = matched_shipments[0]
            return {
                'appointment_id': matched.appointment_id,
                'shipment_appointment': matched.shipment_appointment,
                'origin': matched.origin,
                'load_type': matched.load_type,
                'shipment_account': matched.shipment_account,
                'shipment_type': matched.shipment_type,
                'address': matched.address,
                'carrier': matched.carrier,
                'note': matched.note,
                'ARM_BOL': matched.ARM_BOL,
                'ARM_PRO': matched.ARM_PRO,
                'express_number': matched.express_number,
                'address_detail': await self.get_address(destination),
            }
        return None
    
    async def get_address(self,destination):
        if destination in amazon_fba_locations:
            fba = amazon_fba_locations[destination]
            address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
            return address
        else:
            return None
    
    async def check_time_window_match(self, primary_group, shipment):
        """检查时间窗口是否匹配"""
        shipment_appointment = shipment.shipment_appointment
        if not shipment_appointment:
            return False
        
        shipment_date = shipment_appointment.date()
        # 检查小组中的每个货物
        for cargo in primary_group['cargos']:
            window_start = cargo.get('delivery_window_start')
            window_end = cargo.get('delivery_window_end')
            
            # 如果货物有时间窗口，检查shipment时间是否在窗口内
            if window_start and window_end:
                if not (window_start <= shipment_date <= window_end):
                    return False
            # 如果货物没有时间窗口，跳过时间检查（只要求目的地匹配）
        
        return True

    async def sp_scheduled_data(self, warehouse: str) -> list:
        """获取已排约数据 - 按shipment_batch_number分组"""
        # 获取有shipment_batch_number但fleet_number为空的货物
        raw_data = await self._get_packing_list(
            models.Q(
                container_number__order__warehouse__name=warehouse,
                shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=True,
                shipment_batch_number__shipment_batch_number__isnull=False,
                shipment_batch_number__fleet_number__isnull=True,
                delivery_type='public',
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=False,
                shipment_batch_number__fleet_number__isnull=True,
                location=warehouse,
                delivery_type='public',
            ),
        )
        
        # 按shipment_batch_number分组
        grouped_data = {}
        for item in raw_data:
            
            batch_number = item.get('shipment_batch_number__shipment_batch_number')
            if batch_number not in grouped_data:
                # 获取预约信息
                try:
                    shipment = await sync_to_async(Shipment.objects.get)(
                        shipment_batch_number=batch_number
                    )
                except MultipleObjectsReturned:
                    raise ValueError(f"shipment_batch_number={batch_number} 查询到多条记录，请检查数据")
                grouped_data[batch_number] = {
                    'appointment_id': shipment.appointment_id,
                    'destination': shipment.destination,
                    'shipment_appointment': shipment.shipment_appointment,
                    'load_type': shipment.load_type,
                    'cargos': []
                }
            grouped_data[batch_number]['cargos'].append(item)
        
        return list(grouped_data.values())

    async def _sp_ready_to_ship_data(self, warehouse: str) -> list:
        """获取待出库数据 - 按fleet_number分组"""
        # 获取有fleet_number的货物
        raw_data = await self._get_packing_list(
            models.Q(
                container_number__order__warehouse__name=warehouse,
                shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=True,
                shipment_batch_number__shipment_batch_number__isnull=False,
                shipment_batch_number__fleet_number__isnull=False,
                shipment_batch_number__is_shipped=False,
                delivery_type='public',
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=False,
                shipment_batch_number__fleet_number__isnull=False,
                shipment_batch_number__is_shipped=False,
                location=warehouse,
                delivery_type='public',
            ),
        )
        
        # 按fleet_number分组
        grouped_data = {}
        for item in raw_data:
            fleet_number = item.get('shipment_batch_number__fleet_number')
            if fleet_number not in grouped_data:
                grouped_data[fleet_number] = {
                    'fleet_number': fleet_number,
                    'shipments': {}
                }
            
            # 按shipment分组
            batch_number = item.get('shipment_batch_number__shipment_batch_number')
            if batch_number not in grouped_data[fleet_number]['shipments']:
                try:
                    shipment = await sync_to_async(Shipment.objects.get)(
                        shipment_batch_number=batch_number
                    )
                except MultipleObjectsReturned:
                    raise ValueError(f"shipment_batch_number={batch_number} 查询到多条记录，请检查数据")
                grouped_data[fleet_number]['shipments'][batch_number] = {
                    'appointment_id': shipment.appointment_id,
                    'destination': shipment.destination, 
                    'cargos': []
                }
            grouped_data[fleet_number]['shipments'][batch_number]['cargos'].append(item)
        
        return list(grouped_data.values())

    async def sp_available_shipments(self, warehouse: str, st_type: str) -> list:
        """获取可用预约"""
        now = timezone.now()
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(
                Q(origin__isnull=True) | Q(origin="") | Q(origin=warehouse),
                appointment_id__isnull=False,
                in_use=False,
                is_canceled=False,
                load_type=st_type.upper()
            ).order_by("shipment_appointment")
        )
        
        # 添加状态信息
        for shipment in shipments:
            is_expired = shipment.shipment_appointment_utc and shipment.shipment_appointment_utc < now
            is_urgent = (
                shipment.shipment_appointment_utc and 
                (shipment.shipment_appointment_utc - now).days < 7 and
                shipment.shipment_appointment_utc >= now and
                not is_expired
            )
            
            if is_expired:
                shipment.status = 'expired'
            elif is_urgent:
                shipment.status = 'urgent'
            else:
                shipment.status = 'available'
        
        return shipments

    def _create_primary_groups(self, cargos: list, max_cbm: float, max_pallet: int) -> list:
        """按容量限制创建大组"""
        primary_groups = []
        current_group = {
            'cargos': [],
            'total_pallets': 0,
            'total_cbm': 0,
            'destination': '',
            'delivery_method': ''
        }
        
        # 直接遍历，不排序
        for cargo in cargos:
            cargo_pallets = cargo.get('total_n_pallet_act', 0) or cargo.get('total_n_pallet_est', 0) or 0
            cargo_cbm = cargo.get('total_cbm', 0) or 0
            
            if not current_group['destination']:
                current_group['destination'] = cargo.get('destination')
                current_group['delivery_method'] = cargo.get('custom_delivery_method')
            
            # 检查容量
            if (current_group['total_pallets'] + cargo_pallets <= max_pallet and 
                current_group['total_cbm'] + cargo_cbm <= max_cbm):
                current_group['cargos'].append(cargo)
                current_group['total_pallets'] += cargo_pallets
                current_group['total_cbm'] += cargo_cbm
            else:
                if current_group['cargos']:
                    primary_groups.append(current_group.copy())
                current_group = {
                    'cargos': [cargo],
                    'total_pallets': cargo_pallets,
                    'total_cbm': cargo_cbm,
                    'destination': cargo.get('destination'),
                    'delivery_method': cargo.get('custom_delivery_method')
                }
        
        if current_group['cargos']:
            primary_groups.append(current_group)
        
        return primary_groups

    async def _check_time_window_match(self, window_start, window_end, shipment) -> bool:
        """检查时间窗口匹配"""
        # 简化实现，实际应根据业务逻辑完善
        if not window_start and not window_end:
            return True
        
        shipment_time = shipment.shipment_appointment
        
        # 如果只有开始时间，检查预约时间是否在开始时间之后
        if window_start and not window_end:
            return shipment_time >= window_start
        
        # 如果只有结束时间，检查预约时间是否在结束时间之前
        if not window_start and window_end:
            return shipment_time <= window_end
        
        # 如果既有开始时间又有结束时间，检查预约时间是否在时间窗口内
        if window_start and window_end:
            return window_start <= shipment_time <= window_end
        
        return False

    async def _sp_calculate_summary(self, unscheduled: list, scheduled: list, ready: list) -> dict:
        """计算统计数据"""
        # 计算各类数量
        unscheduled_count = len(unscheduled)
        scheduled_count = sum(len(group['cargos']) for group in scheduled)
        ready_count = sum(
            sum(len(shipment['cargos']) for shipment in fleet_group['shipments'].values())
            for fleet_group in ready
        )
        
        # 计算总板数
        total_pallets = 0
        for cargo in unscheduled:
            total_pallets += cargo.get('total_n_pallet_act', 0) or cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'unscheduled_count': unscheduled_count,
            'scheduled_count': scheduled_count,
            'ready_count': ready_count,
            'total_pallets': int(total_pallets),
        }

    async def get_capacity_limits(self, st_type: str) -> tuple:
        """获取容量限制"""
        if st_type == "pallet":
            return 72, 35
        elif st_type == "floor":
            return 80, 38
        return 72, 35

    async def handle_appointment_management_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        nowtime = timezone.now()
        two_weeks_later = nowtime + timezone.timedelta(weeks=2)
        #所有没约且两周内到港的货物
        unshipment_pos = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
                container_number__order__vessel_id__vessel_eta__lte=two_weeks_later, 
                container_number__order__retrieval_id__retrieval_destination_precise=warehouse,
                delivery_type='public',
                container_number__order__warehouse__name=warehouse,
            )& ~ models.Q(delivery_method__icontains='暂扣'),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=False,
                location=warehouse,
                delivery_type='public',
            ) & ~ models.Q(delivery_method__icontains='暂扣'),
        )
        
        #未使用的约和异常的约
        shipments = await self.get_shipments_by_warehouse(warehouse)
        
        summary = await self.calculate_summary(unshipment_pos, shipments)

        #智能匹配内容
        st_type = request.POST.get('st_type')
        if st_type == "pallet":
            max_cbm = 72
            max_pallet = 35
        elif st_type == "floor":
            max_cbm = 80
            max_pallet = 38
        matching_suggestions = await self.get_matching_suggestions(unshipment_pos, shipments,max_cbm,max_pallet)
        primary_group_keys = set()
        for suggestion in matching_suggestions:
            group_key = f"{suggestion['primary_group']['destination']}_{suggestion['primary_group']['delivery_method']}"
            primary_group_keys.add(group_key)


        auto_matches = await self.get_auto_matches(unshipment_pos, shipments)
        
        context = {
            'warehouse': warehouse,
            'warehouse_options': self.warehouse_options,
            'cargos': unshipment_pos,
            'shipments': shipments,
            'summary': summary,
            'cargo_count': len(unshipment_pos),
            'appointment_count': len(shipments),
            'matching_count': len(primary_group_keys),
            'matching_suggestions': matching_suggestions,
            'auto_matches': auto_matches,
            'st_type': st_type,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
        }
        return self.template_main_dash, context
    
    async def _get_packing_list(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        data = []
        pal_list = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
                "container_number__order__offload_id",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
                "container_number__order__vessel_id",
            )
            .filter(plt_criteria)
            .annotate(
                str_id=Cast("id", CharField()),
                str_container_number=Cast("container_number__container_number", CharField()), 
                
                # 格式化vessel_eta为月日
                formatted_offload_at=Func(
                    F('container_number__order__offload_id__offload_at'),
                    Value('MM-DD'),
                    function='to_char',
                    output_field=CharField()
                ),
                
                # 创建完整的组合字段，通过前缀区分状态
                container_with_eta_retrieval=Concat(
                    Value("[已入仓]"),
                    "container_number__container_number",
                    Value(" 入仓:"),
                    "formatted_offload_at",
                    output_field=CharField()
                ),
                data_source=Value("PALLET", output_field=CharField()),  # 添加数据源标识
            )
            .values(
                "destination",
                "delivery_method",
                "abnormal_palletization",
                "delivery_window_start",
                "delivery_window_end",
                "note",
                "po_expired",
                "shipment_batch_number__shipment_batch_number",
                "data_source",  # 包含数据源标识
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                custom_delivery_method=F("delivery_method"),
                fba_ids=F("fba_id"),
                ref_ids=F("ref_id"),
                shipping_marks=F("shipping_mark"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                container_numbers=StringAgg(  # 聚合完整的组合字段
                    "container_with_eta_retrieval", delimiter="\n", distinct=True, ordering="container_with_eta_retrieval"
                ),
                cns=StringAgg(
                    "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                ),
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet_act=Count("pallet_id", distinct=True),
                label=Value("ACT"),
            )
            .order_by("container_number__order__offload_id__offload_at")
        )
        data += pal_list
        
        # PackingList 查询 - 添加数据源标识
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "container_number__order__retrieval_id",
                    "container_number__order__vessel_id",
                )
                .filter(pl_criteria)
                .annotate(
                    custom_delivery_method=Case(
                        When(
                            Q(delivery_method="暂扣留仓(HOLD)")
                            | Q(delivery_method="暂扣留仓"),
                            then=Concat(
                                "delivery_method",
                                Value("-"),
                                "fba_id",
                                Value("-"),
                                "id",
                            ),
                        ),
                        default=F("delivery_method"),
                        output_field=CharField(),
                    ),     
                    str_container_number=Cast("container_number__container_number", CharField()),    
                    # 格式化vessel_eta为月日
                    formatted_vessel_eta=Func(
                        F('container_number__order__vessel_id__vessel_eta'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 格式化实际提柜时间为月日
                    formatted_actual_retrieval=Func(
                        F('container_number__order__retrieval_id__actual_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 格式化预计提柜时间为月日
                    formatted_target_low=Func(
                        F('container_number__order__retrieval_id__target_retrieval_timestamp_lower'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    formatted_target=Func(
                        F('container_number__order__retrieval_id__target_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 创建完整的组合字段，通过前缀区分状态
                    container_with_eta_retrieval=Case(
                        # 有实际提柜时间 - 使用前缀 [实际]
                        When(container_number__order__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Concat(
                                Value("[已提柜]"),
                                "container_number__container_number",
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                # Value(" 提柜:"),
                                # "formatted_actual_retrieval",
                                output_field=CharField()
                            )),
                        # 有预计提柜时间范围 - 使用前缀 [预计]
                        When(container_number__order__retrieval_id__target_retrieval_timestamp_lower__isnull=False,
                            then=Concat(
                                Value("[预计]"),
                                "container_number__container_number",
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                Value(" 提柜:"),
                                "formatted_target_low",
                                Value("~"),
                                Coalesce("formatted_target", "formatted_target_low"),
                                output_field=CharField()
                            )),
                        # 没有提柜计划 - 使用前缀 [未安排]
                        default=Concat(
                            Value("[未安排提柜]"),
                            "container_number__container_number",
                            Value(" ETA:"),
                            "formatted_vessel_eta",
                            output_field=CharField()
                        ),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                    data_source=Value("PACKINGLIST", output_field=CharField()),  # 添加数据源标识
                )
                .values(
                    "destination",
                    "custom_delivery_method",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "data_source",  # 包含数据源标识
                    "shipment_batch_number__shipment_batch_number",
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                )
                .annotate(
                    fba_ids=StringAgg(
                        "str_fba_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_fba_id",
                    ),
                    ref_ids=StringAgg(
                        "str_ref_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_ref_id",
                    ),
                    shipping_marks=StringAgg(
                        "str_shipping_mark",
                        delimiter=",",
                        distinct=True,
                        ordering="str_shipping_mark",
                    ),
                    ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    container_numbers=StringAgg(  # 聚合完整的组合字段
                        "container_with_eta_retrieval", delimiter="\n", distinct=True, ordering="container_with_eta_retrieval"
                    ),
                    cns=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    total_pcs=Sum("pcs", output_field=FloatField()),
                    total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                    total_n_pallet_est= Round(Sum("cbm", output_field=FloatField()) / 2, 2),
                    label=Value("EST"),
                )
                .distinct()
            )
            data += pl_list
        return data

    async def get_shipments_by_warehouse(self, warehouse):
        """异步获取指定仓库相关的预约数据"""
        appointment = await sync_to_async(list)(
            Shipment.objects.filter(
                (
                    models.Q(origin__isnull=True)
                    | models.Q(origin="")
                    | models.Q(origin=warehouse)
                ),
                models.Q(appointment_id__isnull=False),
                models.Q(in_use=False, is_canceled=False),
            ).order_by("shipment_appointment")
        )
        
        return appointment
    
    async def get_used_pallets(self, shipment):
        """异步计算预约已使用的板数"""
        
        # 异步获取相关订单
        related_orders = await sync_to_async(list)(shipment.order_set.all())
        total_pallets = 0
        
        async def process_order(order):
            order_pallets = 0
            packing_lists = await sync_to_async(list)(order.container_number.packinglist_set.all())
            for packing in packing_lists:
                order_pallets += packing.n_pallet or 0
            return order_pallets
        
        tasks = [process_order(order) for order in related_orders]
        order_pallets_list = await asyncio.gather(*tasks)
        total_pallets = sum(order_pallets_list)
        
        return total_pallets
    
    async def calculate_summary(self, unshipment_pos, shipments):
        """异步计算统计数据 - 适配新的数据结构"""
        now = timezone.now()
    
        # 计算预约状态统计
        expired_count = 0
        urgent_count = 0
        available_count = 0
        used_count = 0  # 已使用的预约数量
        
        for shipment in shipments:
            # 检查预约是否已过期
            is_expired = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc < now
            )
            
            # 检查预约是否即将过期（7天内）
            is_urgent = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc - now < timedelta(days=7) and
                not is_expired
            )
            
            # 检查预约是否已被使用（通过 PackingList 或 Pallet 绑定）
            has_packinglist = await self.has_related_packinglist(shipment)
            has_pallet = await self.has_related_pallet(shipment)
            is_used = has_packinglist or has_pallet
            
            if is_used:
                used_count += 1
            elif is_expired:
                expired_count += 1
            elif is_urgent:
                urgent_count += 1
            else:
                # 可用预约：没过期、不紧急、未被使用
                available_count += 1
        
        # 计算货物统计
        pending_cargos_count = len(unshipment_pos)
        
        # 计算总板数
        total_pallets = 0
        for cargo in unshipment_pos:
            if cargo.get('label') == 'ACT':  # 实际板数
                total_pallets += cargo.get('total_n_pallet_act', 0) or 0
            else:  # 预估板数
                total_pallets += cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'expired_count': expired_count,
            'urgent_count': urgent_count,
            'available_count': available_count,
            'used_count': used_count,  # 已使用的预约数量
            'pending_cargo_count': pending_cargos_count,
            'total_pallets': int(total_pallets),
        }

    async def has_related_packinglist(self, shipment):
        """检查预约是否有相关的 PackingList 记录"""
        
        try:
            # 使用 sync_to_async 包装数据库查询
            packinglist_exists = await sync_to_async(
                PackingList.objects.filter(shipment_batch_number=shipment).exists
            )()
            return packinglist_exists
        except Exception:
            return False

    async def has_related_pallet(self, shipment):
        """检查预约是否有相关的 Pallet 记录"""
        
        try:
            # 使用 sync_to_async 包装数据库查询
            pallet_exists = await sync_to_async(
                Pallet.objects.filter(shipment_batch_number=shipment).exists
            )()
            return pallet_exists
        except Exception:
            return False
    
    async def has_appointment(self, cargo):
        """异步判断货物是否已有预约 - 适配新的数据结构"""
        # 根据你的数据结构，判断是否有预约号
        return cargo.get('shipment_batch_number__shipment_batch_number') is not None
    
    async def get_matching_suggestions(self, unshipment_pos, shipments, max_cbm,max_pallet):
        """异步生成智能匹配建议 - 适配新的数据结构"""
        
        suggestions = []

        # 第一级分组：按目的地和派送方式预分组
        pre_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = cargo.get('destination')
                delivery_method = cargo.get('custom_delivery_method')
                if not dest or not delivery_method:
                    continue
                    
                group_key = f"{dest}_{delivery_method}"
                if group_key not in pre_groups:
                    pre_groups[group_key] = {
                        'destination': dest,
                        'delivery_method': delivery_method,
                        'cargos': []
                    }
                pre_groups[group_key]['cargos'].append(cargo)
        
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__order__vessel_id__vessel_eta') or '')
            
            # 按容量限制创建大组
            primary_groups = []
            current_primary_group = {
                'destination': pre_group['destination'],
                'delivery_method': pre_group['delivery_method'],
                'cargos': [],  # 这个大组包含的所有货物（每个货物就是一个小组）
                'total_pallets': 0,
                'total_cbm': 0,
            }
            
            for cargo in sorted_cargos:
                cargo_pallets = 0
                if cargo.get('label') == 'ACT':
                    cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                else:
                    cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                
                cargo_cbm = cargo.get('total_cbm', 0) or 0
                
                # 检查当前大组是否还能容纳这个货物
                if (current_primary_group['total_pallets'] + cargo_pallets <= max_pallet and 
                    current_primary_group['total_cbm'] + cargo_cbm <= max_cbm):
                    # 可以加入当前大组
                    current_primary_group['cargos'].append(cargo)
                    current_primary_group['total_pallets'] += cargo_pallets
                    current_primary_group['total_cbm'] += cargo_cbm
                else:
                    # 当前大组已满，保存并创建新的大组
                    if current_primary_group['cargos']:
                        primary_groups.append(current_primary_group)
                    
                    # 创建新的大组
                    current_primary_group = {
                        'destination': pre_group['destination'],
                        'delivery_method': pre_group['delivery_method'],
                        'cargos': [cargo],
                        'total_pallets': cargo_pallets,
                        'total_cbm': cargo_cbm,
                    }
            
            # 添加最后一个大组
            if current_primary_group['cargos']:
                primary_groups.append(current_primary_group)
            
            # 为每个大组创建建议，大组中的每个货物都是一个小组（一行）
            for primary_group_index, primary_group in enumerate(primary_groups):
                # 计算大组的匹配度百分比
                pallets_percentage = min(100, (primary_group['total_pallets'] / max_pallet) * 100) if max_pallet > 0 else 0
                cbm_percentage = min(100, (primary_group['total_cbm'] / max_cbm) * 100) if max_cbm > 0 else 0
                
                # 大组中的每个货物都是一个小组（一行）
                for subgroup_index, cargo in enumerate(primary_group['cargos']):
                    # 计算这个货物的板数和CBM
                    cargo_pallets = 0
                    if cargo.get('label') == 'ACT':
                        cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                    else:
                        cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                    
                    cargo_cbm = cargo.get('total_cbm', 0) or 0
                    
                    suggestion = {
                        'id': f"{group_key}_{primary_group_index}_{subgroup_index}",
                        'primary_group': {
                            'destination': primary_group['destination'],
                            'delivery_method': primary_group['delivery_method'],
                            'total_pallets': primary_group['total_pallets'],
                            'total_cbm': primary_group['total_cbm'],
                            'pallets_percentage': pallets_percentage,
                            'cbm_percentage': cbm_percentage,
                        },
                        'subgroup': {
                            'cargos': [{
                                'ids': cargo.get('ids', ''),  # 确保包含ids
                                'plt_ids': cargo.get('plt_ids', ''),  # 确保包含plt_ids
                                'container_numbers': cargo.get('container_numbers', ''),
                                'cns': cargo.get('cns', ''),
                                'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                                'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                                'total_cbm': cargo.get('total_cbm', 0),
                                'label': cargo.get('label', ''),
                            }],
                            'total_pallets': cargo_pallets,
                            'total_cbm': cargo_cbm,
                            'container_numbers': cargo.get('container_numbers', ''),
                            'cns': cargo.get('cns', ''),
                            'cargo_count': 1
                        },
                        'subgroup_index': subgroup_index + 1,
                    }
                    suggestions.append(suggestion)
        return suggestions
    
    async def is_shipment_available(self, shipment):
        """判断预约是否可用"""
        
        now = timezone.now()
        
        # 已发货的不可用
        if shipment.shipped_at:
            return False
        
        # 已过期的不可用
        if (shipment.shipment_appointment and 
            shipment.shipment_appointment < now):
            return False
        
        return True
    
    async def get_auto_matches(self, unshipment_pos, shipments):
        """异步获取自动匹配结果 - 适配新的数据结构"""
        matches = []
        
        # 按目的地分组货物
        destination_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = cargo.get('destination')
                if dest not in destination_groups:
                    destination_groups[dest] = []
                destination_groups[dest].append(cargo)
        
        # 为每个目的地生成匹配组合
        match_id = 1
        for destination, cargo_list in destination_groups.items():
            # 按板数排序，优先匹配大板数的货物
            sorted_cargos = sorted(cargo_list, 
                                 key=lambda x: x.get('total_n_pallet_act', 0) or x.get('total_n_pallet_est', 0) or 0, 
                                 reverse=True)
            
            # 生成匹配组合（尽量接近35板）
            current_group = []
            current_pallets = 0
            
            for cargo in sorted_cargos:
                cargo_pallets = (cargo.get('total_n_pallet_act', 0) or 
                               cargo.get('total_n_pallet_est', 0) or 0)
                
                if current_pallets + cargo_pallets <= 35:
                    current_group.append(cargo)
                    current_pallets += cargo_pallets
                else:
                    # 当前组已满，创建匹配
                    if current_group:
                        match_percentage = min(int((current_pallets / 35) * 100), 100)
                        
                        # 查找最佳预约
                        best_shipment = await self.find_best_shipment_for_match(destination, current_pallets, shipments)
                        
                        matches.append({
                            'id': match_id,
                            'destination': destination,
                            'cargo_count': len(current_group),
                            'total_pallets': int(current_pallets),
                            'recommended_appointment': best_shipment,
                            'match_percentage': match_percentage,
                            'cargos': current_group[:5]  # 只显示前5个货物详情
                        })
                        match_id += 1
                    
                    # 开始新组
                    current_group = [cargo]
                    current_pallets = cargo_pallets
            
            # 处理最后一组
            if current_group:
                match_percentage = min(int((current_pallets / 35) * 100), 100)
                best_shipment = await self.find_best_shipment_for_match(destination, current_pallets, shipments)
                
                matches.append({
                    'id': match_id,
                    'destination': destination,
                    'cargo_count': len(current_group),
                    'total_pallets': int(current_pallets),
                    'recommended_appointment': best_shipment,
                    'match_percentage': match_percentage,
                    'cargos': current_group[:5]
                })
                match_id += 1
        
        return matches[:10]  # 限制返回数量
    
    async def find_best_shipment_for_match(self, destination, total_pallets, shipments):
        """为匹配组合查找最佳预约"""
        best_shipment = None
        best_capacity_diff = float('inf')
        
        for shipment in shipments:
            if (shipment.destination == destination and 
                await self.is_shipment_available(shipment)):
                
                used_pallets = await self.get_used_pallets(shipment)
                remaining_capacity = 35 - used_pallets
                capacity_diff = abs(remaining_capacity - total_pallets)
                
                if capacity_diff < best_capacity_diff:
                    best_capacity_diff = capacity_diff
                    best_shipment = shipment
        
        return best_shipment

    async def get_used_pallets(self, shipment):
        """异步计算预约已使用的板数 - 适配新的数据结构"""
        # 根据你的数据结构，这里需要计算该预约已经使用的板数
        # 由于你的数据结构中没有直接关联，这里可能需要根据实际情况调整
        
        # 临时实现：假设从关联的订单中计算
        try:
            # 获取该预约关联的所有货物
            related_packing_lists = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number=shipment
                )
            )
            
            total_pallets = 0
            for pl in related_packing_lists:
                total_pallets += pl.n_pallet or 0
            
            return total_pallets
        except:
            return 0
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False