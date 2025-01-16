import ast
import uuid
import os
import json
import math
import pytz
import pandas as pd

from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from dateutil.parser import parse
from typing import List
from collections import Counter

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, Max, FloatField, IntegerField, When, Count, Q
from django.contrib.postgres.aggregates import ArrayAgg 
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.utils import timezone

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    amazon_fba_locations,
    LOAD_TYPE_OPTIONS,
    SP_USER,
    SP_PASS,
    SP_URL,
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
    template_shipment_list = "post_port/shipment/07_shipment_list.html"
    template_shipment_list_shipment_display = "post_port/shipment/07_1_shipment_list_shipment_display.html"
    template_shipment_exceptions = "post_port/shipment/exceptions/01_shipment_exceptions.html"
    template_batch_shipment = "post_port/shipment/08_batch_shipment.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA":"LA", "NJ/SAV/LA":"NJ/SAV/LA"}
    warehouse_options = {"": "", "NJ-07001": "NJ-07001", "NJ-08817": "NJ-08817", "SAV-31326": "SAV-31326","LA-91761": "LA-91761"}
    account_options = {"": "", "Carrier Central1": "Carrier Central1", "Carrier Central2": "Carrier Central2", "ZEM-AMF": "ZEM-AMF", "ARM-AMF": "ARM-AMF","walmart":"walmart"}
    shipment_type_options = {"":"", "FTL":"FTL", "LTL": "LTL", "外配/快递":"外配/快递","客户自提":"客户自提"}

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "shipment_info":
            template, context = await self.handle_shipment_info_get(request)
            return render(request, template, context)
        elif step == "shipment_list":
            template, context = await self.handle_shipment_list_get(request)
            return render(request, template, context)
        elif step == "appointment_management":
            template, context = await self.handle_appointment_management_get(request)
            return render(request, template, context)
        elif step == "shipment_detail_display":
            template, context = await self.handle_shipment_detail_display_get(request)
            return render(request, template, context)
        elif step == "shipment_exceptions":
            template, context = await self.handle_shipment_exceptions_get(request)
            return render(request, template, context)
        elif step == "batch_shipment":        
            return render(request, self.template_batch_shipment)
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
        elif step == "overshipment":
            template, context = await self.handle_over_shipment_post(request)
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
        elif step == "shipment_list_search":
            template, context = await self.handle_shipment_list_search_post(request)
            return render(request, template, context)
        elif step == "fix_shipment_exceptions":
            template, context = await self.handle_fix_shipment_exceptions_post(request)
            return render(request, template, context)
        elif step == "appointment_time_modify":
            template, context = await self.handle_appointment_time(request)
            return render(request, template, context)
        elif step == "cancel_abnormal_appointment":
            template, context = await self.handle_cancel_abnormal_appointment_post(request)
            return render(request, template, context)
        elif step == "upload_batch_shipment":
            template, context = await self.handle_batch_shipment_get(request)
            return render(request,template, context)
        else:
            return await self.get(request)
        
    async def handle_appointment_time(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        appointmentId = request.POST.get("appointmentId")
        shipment = await sync_to_async(Shipment.objects.get)(appointment_id=appointmentId)
        operation = request.POST.get("operation")
        if operation == "edit":
            appointmentTime = request.POST.get("appointmentTime")
            naive_datetime = parse(appointmentTime).replace(tzinfo=None)       
            shipment.shipment_appointment = naive_datetime
            await sync_to_async(shipment.save)()
        elif operation == "delete":
            shipment.is_canceled = True
            await sync_to_async(shipment.delete)()
        return await self.handle_appointment_warehouse_search_post(request)

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
            "account_options": self.account_options,
            "warehouse": request.GET.get("warehouse"),
            "warehouse_options": self.warehouse_options,
            "shipment_type_options": self.shipment_type_options,
        })
        return self.template_td_shipment_info, context
    
    async def handle_appointment_management_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        context = {
            "load_type_options": LOAD_TYPE_OPTIONS,
            "warehouse_options": self.warehouse_options,
            "account_options": self.account_options,
            "start_date": (datetime.now().date() + timedelta(days=-7)).strftime("%Y-%m-%d"),
            "end_date": (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d"),
        }
        return self.template_appointment_management, context

    async def handle_shipment_list_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        context = {
            "warehouse_options": self.warehouse_options,
            "start_date": (datetime.now().date() + timedelta(days=-7)).strftime("%Y-%m-%d"),
            "end_date": (datetime.now().date() + timedelta(days=14)).strftime("%Y-%m-%d"),
        }
        return self.template_shipment_list, context
    
    async def handle_shipment_detail_display_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.GET.get("warehouse")
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        batch_number = request.GET.get("batch_number")
        mutable_post = request.POST.copy()
        mutable_post["warehouse"] = warehouse
        mutable_post["start_date"] = start_date
        mutable_post["end_date"] = end_date
        request.POST = mutable_post
        _, context = await self.handle_shipment_list_search_post(request)
        shipment_selected = await sync_to_async(Shipment.objects.select_related("fleet_number").get)(shipment_batch_number=batch_number)
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
        context["shipment_selected"] = shipment_selected
        context["packing_list_selected"] = packing_list_selected
        context["shipment_type_options"] = self.shipment_type_options
        context["load_type_options"] = LOAD_TYPE_OPTIONS
        return self.template_shipment_list_shipment_display, context
    
    async def handle_shipment_exceptions_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                status="Exception",
                is_canceled=False,
            ).order_by('shipment_appointment')
        )
        shipment_data = {
            s.shipment_batch_number: {
                "origin": s.origin,
                "load_type": s.load_type,
                "note": s.note,
                "destination": s.destination,
                "address": s.address,
                "origin": s.origin,
            }
            for s in shipment
        }
        unused_appointment = await sync_to_async(list)(
            Shipment.objects.filter(
                in_use=False,
                is_canceled=False
            )
        )
        unused_appointment = {
            s.appointment_id: {
                "destination": s.destination.strip(),
                "shipment_appointment": s.shipment_appointment.replace(microsecond=0).isoformat(),
            }
            for s in unused_appointment
        }
        context = {
            "shipment": shipment,
            "shipment_type_options": self.shipment_type_options,
            "unused_appointment": json.dumps(unused_appointment),
            "shipment_data": json.dumps(shipment_data),
            "warehouse_options": self.warehouse_options,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "account_options": self.account_options,
        }
        return self.template_shipment_exceptions, context

    async def handle_over_shipment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        if request.POST.get("area"):
            area = request.POST.get("area")
        elif request.POST.get("warehouse"):
            area = request.POST.get("warehouse")[:2]
        elif request.GET.get("warehouse"):
            area = request.GET.get("warehouse")[:2]
        else:
            area = None
        if request.POST.get("area"):
            area = request.POST.get("area")       
        if area == 'NJ/SAV/LA':          
            criteria = (
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="NJ") |
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="SAV") |
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="LA") |
                models.Q(pallet__location__startswith="NJ") |
                models.Q(pallet__location__startswith="SAV") |
                models.Q(pallet__location__startswith="LA")
            )
        else:
            criteria = (
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area=area) |
                models.Q(pallet__location__startswith=area)
            )
        shipment = await sync_to_async(list)(
            Shipment.objects.prefetch_related(
                "packinglist", "packinglist__container_number", "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse", "order", "pallet", "fleet_number"
            ).filter(
                criteria &
                models.Q(
                    is_shipped=True,
                    in_use=True,
                    is_canceled=False,
                )
            ).distinct().order_by('-abnormal_palletization', 'shipment_appointment')
        )
        #ETA过滤
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        start_date = (datetime.now().date() + timedelta(days=-15)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = (datetime.now().date() + timedelta(days=15)).strftime('%Y-%m-%d') if not end_date else end_date
        
        criteria_p = models.Q(
            (models.Q(container_number__order__order_type="转运") | models.Q(container_number__order__order_type="转运组合")),
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,          
            container_number__order__created_at__gte='2024-09-01',
        ) 
        pl_criteria = criteria_p & models.Q(
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=True
        )
        plt_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=False
        )
        if area == 'NJ/SAV/LA':
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=["NJ", "SAV", "LA"]
            )
            plt_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=["NJ", "SAV", "LA"]
            )
        else:
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area=area
            )
            plt_criteria &= models.Q(location__startswith=area)

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
            "start_date": start_date,
            "end_date": end_date,
        }
        return self.template_td, context
    
    async def handle_batch_shipment_get(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            df = df.rename(columns={
                '车次编号': 'fleet_serial',
                '提货时间': 'appointment_datetime',
                'ISA': 'ISA',
                '约批次号': 'appointment_batch_number',
                '送仓时间': 'shipment_appointment',
                '柜号': 'container_number',
                '目的地': 'destination',
                '板数': 'pallets',
                'cbm': 'cbm',
                'PO_ID': 'PO_ID',
                'carrier': 'carrier',
                '备注':'note',
                '成本价': 'cost_price',
                '预约类型':'shipment_type',
                '预约账号':'shipment_account',
                '装车类型':'load_type',
                '发货仓库':'warehouse'
            })
            df['ISA'] = df['ISA'].apply(lambda x: str(int(x)) if pd.notna(x) else x)
            df = df.dropna(how='all')
            data = df.to_dict(orient='records')
            result = {}
            sum_fleet = []
            sum_ISA = []
            for item in data:
                fleet_serial = item['fleet_serial']
                if not self._verify_empty_string(fleet_serial):
                    #说明是一个新的车次，那约肯定也是新的
                    sum_fleet.append(fleet_serial)  
                    sum_ISA.append(item['ISA'])
                    #记录最近一次的约和ISA
                    last_fleet = fleet_serial
                    last_ISA = item['ISA']
                    #构建一个车次的字典
                    result[fleet_serial] = {
                        'appointment_datetime': item['appointment_datetime'],
                        'carrier': item['carrier'],
                        'origin': item['warehouse'],
                        'shipment': {
                            item['ISA']:{
                                'appointment_batch_number':item['appointment_batch_number'],
                                'shipment_appointment':item['shipment_appointment'],
                                'carrier':item['carrier'],
                                'note':item['note'],
                                'cost_price':item['cost_price'],
                                'shipment_type':item['shipment_type'],
                                'shipment_account':item['shipment_account'],
                                'load_type':item['load_type'],
                                'origin':item['warehouse'],
                                'PO':[{
                                    'PO_ID': item['PO_ID'],
                                    'container_number':item['container_number'],
                                    'destination':item['destination'],
                                    'pallets':item['pallets'],
                                    'cbm':item['cbm']
                                }],
                            }
                        }
                    }
                else:
                    #说明该行前面已建了该车次的信息                 
                    if not self._verify_empty_string(item['ISA']):
                        ISA = item['ISA']
                        #说明是一行新的预约批次
                        last_ISA = ISA
                        result[last_fleet]['shipment'][ISA]={
                            'appointment_batch_number':item['appointment_batch_number'],
                            'shipment_appointment':item['shipment_appointment'],
                            'carrier':item['carrier'],
                            'note':item['note'],
                            'cost_price':item['cost_price'],
                            'shipment_type':item['shipment_type'],
                            'shipment_account':item['shipment_account'],
                            'load_type':item['load_type'],
                            'origin':item['warehouse'],
                            'PO':[{
                                'PO_ID': item['PO_ID'],
                                'container_number':item['container_number'],
                                'destination':item['destination'],
                                'pallets':item['pallets'],
                                'cbm':item['cbm']
                            }],
                        }
                    else:
                        #说明该行只有柜号 目的地 板数cbm
                        result[last_fleet]['shipment'][last_ISA]['PO'].append({
                            'PO_ID': item['PO_ID'],
                            'container_number':item['container_number'],
                            'destination':item['destination'],
                            'pallets':item['pallets'],
                            'cbm':item['cbm'],
                        })
            #构建完数据，开始预约
            await self.handle_batch_shipment_create(result)
        context = {
            "shipment_list":[1,2,3]
        }
        return self.template_batch_shipment, context
    
    async def handle_batch_shipment_create(self, result: dict[str, Any]) -> str:
        exclude_keys = {'appointment_batch_number','shipment_appointment', 'carrier', 'note', 'cost_price'}
        repeated_isa = []#重复的约
        canceled_isa =[] #取消的约
        expired_isa = []#过期的约
        mismatched_isa = {} #系统现存的约和表格的信息不符的，键值对为：约：{不符的信息}
        equal_shipment = {}
        equal_destination = {}
        created_isa = []
        discover_isa = []
        isa_hash = []
        discover_PO = []  #excel表里，po_id在系统查不到的
        for fleet, fleet_value in result.items():
            #创建车的批次
            fleet_data = {}
            if isinstance(fleet_value, dict) and fleet_value:
                # raise ValueError(fleet_value)
                for ISA, ISA_value in fleet_value["shipment"].items():
                    # TODOs: 添加约的信息
                    created_isa.append({
                        "appointment_id": ISA,
                    })

                    PO_IDS = []
                    for po in ISA_value["PO"]:
                        PO_IDS.append(po["PO_ID"])
                    #add_po中，就是一个预约批次要加的板子
                    for ids in PO_IDS:
                        packing_list_selected = await self._get_packing_list(
                            models.Q(PO_ID=ids),
                            models.Q(PO_ID=ids),
                        )
                        
                        if len(packing_list_selected) != 0:
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
                            
                            address = amazon_fba_locations.get(destination, None) #查找亚马逊地址中是否有该地址
                            if destination in amazon_fba_locations: 
                                fba = amazon_fba_locations[destination]
                                address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
                            else:
                                address, zipcode = str(packing_list_selected[0].get("address")), str(packing_list_selected[0].get('zipcode'))
                                if zipcode.lower() not in address.lower():   
                                    address += f", {zipcode}"                              
                            shipment_data = {
                                "shipment_batch_number": ISA_value["appointment_batch_number"],
                                "destination": destination,
                                "total_weight": total_weight,
                                "total_cbm": total_cbm,
                                "total_pallet": total_pallet,
                                "total_pcs": total_pcs,
                                "PO_IDS":PO_IDS,
                                "appointment_id":ISA,
                                'shipment_account':ISA_value['shipment_account'],
                                'shipment_type':ISA_value['shipment_type'],
                                'load_type':ISA_value['load_type'],
                                'origin':ISA_value['origin'],
                                'shipment_appointment':ISA_value['shipment_appointment'],
                                'carrier':ISA_value['shipment_appointment'],
                                'note':ISA_value['shipment_appointment']
                            }
                           
                            result = await self.handle_batch_shipment_create_branch(shipment_data, repeated_isa, canceled_isa, expired_isa, 
                                                                                    mismatched_isa, discover_isa, equal_shipment, equal_destination)
                            repeat_isa = result["repeat_isa"]
                            cancel_isa = result["cancel_isa"]
                            outer_isa = result["outer_isa"]
                            discover_isa = result["discover_isa"]
                            equal_shipment = result["equal_shipment"]
                            equal_destination = result["equal_destination"]
                    else:
                        discover_PO.append(ids)
            else:
                fleet_data[fleet] = fleet_value
        return "123"
    
    async def handle_batch_shipment_create_branch(
        Self,
        shipment_data: dict,
        repeat_isa: list,
        cancel_isa: list,
        expired_isa: list,
        mismatched_isa: list,
        discover_isa: list,
        equal_shipment:dict,equal_destination:dict
    ) -> str:
        fields = [
            "shipment_type",
            "shipment_batch_number",
            "shipment_appointment",
            "appointment_id",
            "destination",
            "carrier",
            "total_weight",
            "total_cbm",
            "total_pallet",
            "total_pcs",
            "shipment_account",
            "load_type",
        ]
        #有批次号，就校验
        if shipment_data["shipment_batch_number"]:
            #校验约的基本信息
            try:
                existed_appointment = await sync_to_async(Shipment.objects.get)(appointment_id=shipment_data["appointment_id"])
                #后面的没校对
                for field in fields:
                    value_to_compare = shipment_data[field].strip()
                    appointment_value = getattr(existed_appointment, field)
                    is_equal = value_to_compare == appointment_value
                    if not is_equal:
                        if equal_shipment[shipment_data["appointment_id"]]:
                            equal_shipment[shipment_data["appointment_id"]].append([value_to_compare,appointment_value])
                        else:
                            equal_shipment[shipment_data["appointment_id"]] = [[value_to_compare,appointment_value]]
                #校验板子的信息
                #本次上传的板子信息  PO_IDS
                #查找该约在系统的板子信息
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related(
                        "container_number", "shipment_batch_number", "pallet"
                    ).filter(
                        shipment_batch_number__appointment_id=shipment_data["appointment_id"]
                    ).values('PO_ID').annotate(
                        pl_id=Count('id'),
                        cbms=Sum('cbm'),
                        destinations=ArrayAgg('destination')
                    )
                    )
                packing_list += await sync_to_async(list)(
                    Pallet.objects.select_related(
                        "container_number", "shipment_batch_number"
                    ).filter(
                        shipment_batch_number__appointment_id=shipment_data["appointment_id"]
                    ).values('PO_ID').annotate(
                        pl_id=Count('id'),
                        cbms=Sum('cbm'),
                        destinations=ArrayAgg('destination')
                    ))
                sys_PO_ID = []
                #先判断约里目的地是不是有不同
                for pl in packing_list:
                    sys_PO_ID.append(pl["PO_ID"])
                    des = set(pl["destinations"])
                    if len(des) > 1:
                        equal_destination[shipment_data["appointment_id"]] = des
                #然后判断PO_ID两方是否相等,这里如果不相等，就是改约了吧？那板子怎么比较？改约了本来也不相等
                are_equal = Counter(sys_PO_ID) == Counter(shipment_data["PO_IDS"])
            except Shipment.DoesNotExist:
                discover_isa.append(shipment_data["appointment_id"])
        else:
            #没有批次号就创建，先看下ISA有没有用过
            try:
                existed_appointment = await sync_to_async(Shipment.objects.get)(appointment_id=shipment_data["appointment_id"])
            except:
                existed_appointment = None
            if existed_appointment:
                if existed_appointment.in_use:
                    repeat_isa.append(shipment_data["appointment_id"])
                elif existed_appointment.is_canceled:
                    cancel_isa.append(shipment_data["appointment_id"])
                elif existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC) < timezone.now():
                    outer_isa.append(shipment_data["appointment_id"])
            else:#开始创建
                current_time = datetime.now()
                batch_id = shipment_data["destination"] + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper()
                batch_id = batch_id.replace(" ", "").upper()
                shipment = Shipment(**shipment_data)
                await sync_to_async(shipment.save)()
        result = [repeat_isa,cancel_isa,outer_isa,discover_isa,equal_shipment,equal_destination]
        return self.template_batch_shipment, result

    async def handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        if request.POST.get("area"):
            area = request.POST.get("area")
        elif request.POST.get("warehouse"):
            area = request.POST.get("warehouse")[:2]
        elif request.GET.get("warehouse"):
            area = request.GET.get("warehouse")[:2]
        else:
            area = None
        if request.POST.get("area"):
            area = request.POST.get("area")       
        if area == 'NJ/SAV/LA':          
            criteria = (
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="NJ") |
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="SAV") |
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area="LA") |
                models.Q(pallet__location__startswith="NJ") |
                models.Q(pallet__location__startswith="SAV") |
                models.Q(pallet__location__startswith="LA")
            )
        else:
            criteria = (
                models.Q(packinglist__container_number__order__retrieval_id__retrieval_destination_area=area) |
                models.Q(pallet__location__startswith=area)
            )
        shipment = await sync_to_async(list)(
            Shipment.objects.prefetch_related(
                "packinglist", "packinglist__container_number", "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse", "order", "pallet", "fleet_number"
            ).filter(
                criteria &
                models.Q(
                    is_shipped=False,
                    in_use=True,
                    is_canceled=False,
                )
            ).distinct().order_by('-abnormal_palletization', 'shipment_appointment')
        )
        #ETA过滤
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        start_date = (datetime.now().date() + timedelta(days=-15)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = (datetime.now().date() + timedelta(days=15)).strftime('%Y-%m-%d') if not end_date else end_date
        
        criteria_p = models.Q(
            (models.Q(container_number__order__order_type="转运") | models.Q(container_number__order__order_type="转运组合")),
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,          
            container_number__order__created_at__gte='2024-09-01',
        ) 
        pl_criteria = criteria_p & models.Q(
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=True
        )
        plt_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=False
        )
        if area == 'NJ/SAV/LA':
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=["NJ", "SAV", "LA"]
            )
            plt_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=["NJ", "SAV", "LA"]
            )
        else:
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area=area
            )
            plt_criteria &= models.Q(location__startswith=area)

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
            "start_date": start_date,
            "end_date": end_date,
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
            unused_appointment = await sync_to_async(list)(
                Shipment.objects.filter(
                    in_use=False,
                    is_canceled=False
                )
            )
            unused_appointment = {
                s.appointment_id: {
                    "destination": s.destination.strip(),
                    "shipment_appointment": s.shipment_appointment.replace(microsecond=0).isoformat(),
                }
                for s in unused_appointment
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
                "unused_appointment": json.dumps(unused_appointment),
                "start_date": request.POST.get("start_date"),
                "end_date": request.POST.get("end_date"),
                "account_options": self.account_options,
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
                    raise RuntimeError(f"ISA {appointment_id} 已经登记过了!")
                elif existed_appointment.is_canceled:
                    raise RuntimeError(f"ISA {appointment_id} already exists and is canceled!")
                elif existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC) < timezone.now():
                    raise RuntimeError(f"ISA {appointment_id} 预约时间小于当前时间，已过期!")
                elif existed_appointment.destination.replace("Walmart", "").replace("WALMART","").replace("-", "") != request.POST.get("destination", None).replace("Walmart", "").replace("WALMART","").replace("-", ""):
                    raise ValueError(f"ISA {appointment_id} 登记的目的地是 {existed_appointment.destination} ，此次登记的目的地是 {request.POST.get('destination', None)}!")
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
                    shipment.destination = request.POST.get("destination", None).replace("WALMART","Walmart")
                    shipment.address = request.POST.get("address", None)
                    shipment.shipment_account = request.POST.get("shipment_account", "").strip()
                    shipmentappointment = request.POST.get("shipment_appointment", None)
                    shipment.shipment_appointment = shipmentappointment
                    #LTL的需要存ARM-BOL和ARM-PRO
                    shipment.ARM_BOL = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
                    shipment.ARM_PRO = request.POST.get("arm_pro") if request.POST.get("arm_bol") else ""
                    try:
                        shipment.third_party_address = shipment_data["third_party_address"].strip()
                    except:
                        pass
            else:
                if await self._shipment_exist(shipment_data["shipment_batch_number"]):
                    raise ValueError(f"约批次 {shipment_data['shipment_batch_number']} 已经存在!")
                shipment_data["appointment_id"] = request.POST.get("appointment_id", None)
                try:
                    shipment_data["third_party_address"] = shipment_data["third_party_address"].strip()
                except:
                    pass
                if shipment_type == "外配/快递":
                    shipmentappointment = request.POST.get("shipment_est_arrival", None)
                else:
                    shipmentappointment = request.POST.get("shipment_appointment", None)
                shipment_data["shipment_type"] = shipment_type
                shipment_data["load_type"] = request.POST.get("load_type", None)
                shipment_data["note"] = request.POST.get("note", "")
                shipment_data["shipment_schduled_at"] = current_time
                shipment_data["is_shipment_schduled"] = True
                shipment_data["destination"] = request.POST.get("destination", None)
                shipment_data["address"] = request.POST.get("address", None)
                shipment_data["origin"] = request.POST.get("origin", "")
                shipment_data["shipment_account"] = request.POST.get("shipment_account", "").strip()
                shipment_data["shipment_appointment"] = shipmentappointment  #FTL和外配快递的scheduled time表示预计到仓时间，LTL和客户自提的提货时间
                if shipment_type != "FTL":                 
                    fleet = Fleet(**{
                        "carrier": request.POST.get("carrier"),
                        "fleet_type": shipment_type,
                        "appointment_datetime": request.POST.get("shipment_appointment", None), #车次的提货时间
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
                    #LTL的需要存ARM-BOL和ARM-PRO
                    shipment_data["ARM_BOL"] = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
                    shipment_data["ARM_PRO"] = request.POST.get("arm_pro") if request.POST.get("arm_bol") else ""
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
                pallet_container_number = [p.container_number.container_number for p in pallet]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("shipment_batch_number").filter(
                        container_number__container_number__in=pallet_container_number
                    )
                )
                pallet_shipping_marks, pallet_fba_ids, pallet_ref_ids = [], [], []
                updated_pl = []
                for p in pallet:
                    p.shipment_batch_number = shipment
                    if p.shipping_mark:
                        pallet_shipping_marks += p.shipping_mark.split(",")
                    if p.fba_id:
                        pallet_fba_ids += p.fba_id.split(",")
                    if p.ref_id:
                        pallet_ref_ids += p.ref_id.split(",")
                for pl in packing_list:
                    if pl.shipping_mark and pl.shipping_mark in pallet_shipping_marks:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                    elif pl.fba_id and pl.fba_id in pallet_fba_ids:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                    elif pl.ref_id and pl.ref_id in pallet_ref_ids:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])  
                await sync_to_async(PackingList.objects.bulk_update)(updated_pl, ["shipment_batch_number"])    
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
            shipment.shipment_appointment = parse(shipment_appointment).replace(tzinfo=None)
            shipment.note = note
            shipment.is_shipment_schduled = True
            shipment.shipment_schduled_at = current_time
            #LTL的需要存ARM-BOL和ARM-PRO
            shipment.ARM_BOL = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
            shipment.ARM_PRO = request.POST.get("arm_pro") if request.POST.get("arm_bol") else ""
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
                pallet_container_number = [p.container_number.container_number for p in pallet]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("shipment_batch_number").filter(
                        container_number__container_number__in=pallet_container_number
                    )
                )
                pallet_shipping_marks, pallet_fba_ids, pallet_ref_ids = [], [], []
                updated_pl = []
                for p in pallet:
                    p.shipment_batch_number = shipment
                    shipment.total_weight += p.weight_lbs
                    shipment.total_pcs += p.pcs
                    shipment.total_cbm += p.cbm
                    if p.shipping_mark:
                        pallet_shipping_marks += p.shipping_mark.split(",")
                    if p.fba_id:
                        pallet_fba_ids += p.fba_id.split(",")
                    if p.ref_id:
                        pallet_ref_ids += p.ref_id.split(",")
                for pl in packing_list:
                    if pl.shipping_mark and pl.shipping_mark in pallet_shipping_marks:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                    elif pl.fba_id and pl.fba_id in pallet_fba_ids:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                    elif pl.ref_id and pl.ref_id in pallet_ref_ids:
                        pl.shipment_batch_number = shipment
                        updated_pl.append(pl)
                shipment.total_pallet += len(set([p.pallet_id for p in pallet]))
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])
                await sync_to_async(PackingList.objects.bulk_update)(updated_pl, ["shipment_batch_number"])    
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
                pallet_container_number = [p.container_number.container_number for p in pallet]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("shipment_batch_number").filter(
                        container_number__container_number__in=pallet_container_number
                    )
                )
                pallet_shipping_marks, pallet_fba_ids, pallet_ref_ids = [], [], []
                updated_pl = []
                for p in pallet:
                    p.shipment_batch_number = None
                    shipment.total_weight -= p.weight_lbs
                    shipment.total_pcs -= p.pcs
                    shipment.total_cbm -= p.cbm
                    if p.shipping_mark:
                        pallet_shipping_marks += p.shipping_mark.split(",")
                    if p.fba_id:
                        pallet_fba_ids += p.fba_id.split(",")
                    if p.ref_id:
                        pallet_ref_ids += p.ref_id.split(",")
                for pl in packing_list:
                    if pl.shipping_mark and pl.shipping_mark in pallet_shipping_marks:
                        pl.shipment_batch_number = None
                        updated_pl.append(pl)
                    elif pl.fba_id and pl.fba_id in pallet_fba_ids:
                        pl.shipment_batch_number = None
                        updated_pl.append(pl)
                    elif pl.ref_id and pl.ref_id in pallet_ref_ids:
                        pl.shipment_batch_number = None
                        updated_pl.append(pl)
                shipment.total_pallet -= len(set([p.pallet_id for p in pallet]))
                await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])
                await sync_to_async(PackingList.objects.bulk_update)(updated_pl, ["shipment_batch_number"]) 
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
        shipment_appointment = request.POST.get("shipment_appointment")
        if not shipment_appointment:
            shipment_appointment = None
        if shipment_type == shipment.shipment_type:
            if shipment_type == "FTL":
                shipment.appointment_id = request.POST.get("appointment_id")
                shipment.shipment_account = request.POST.get("shipment_account")
                shipment.origin = request.POST.get("origin")
                shipment.carrier = request.POST.get("carrier")
                shipment.third_party_address = request.POST.get("third_party_address")
                shipment.load_type = request.POST.get("load_type")
                shipment.shipment_schduled_at = timezone.now()
                shipment.shipment_appointment = shipment_appointment #界面的schedule_time
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace("WALMART","Walmart")
                shipment.address = request.POST.get("address")
            elif shipment_type != "FTL":
                shipment.appointment_id = request.POST.get("appointment_id", "")
                shipment.shipment_account = request.POST.get("shipment_account", "")
                shipment.origin = request.POST.get("origin")
                shipment.shipment_schduled_at = timezone.now()
                shipment.shipment_appointment = shipment_appointment
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace("WALMART","Walmart")
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                #测试发现甩板的约没有车次
                if fleet:
                    fleet.carrier = request.POST.get("carrier")
                    fleet.appointment_datetime = shipment_appointment
                    await sync_to_async(fleet.save)()
                else:
                    current_time = datetime.now()
                    fleet = Fleet(**{
                        "carrier": request.POST.get("carrier"),
                        "fleet_type": shipment_type,
                        "appointment_datetime": shipment_appointment,
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
                shipment.ARM_BOL = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ''
                shipment.ARM_PRO = request.POST.get("arm_pro") if request.POST.get("arm_pro") else ''
        else:
            if shipment_type == "FTL":
                shipment.shipment_type = shipment_type
                shipment.appointment_id = request.POST.get("appointment_id")
                shipment.shipment_account = request.POST.get("shipment_account")
                shipment.origin = request.POST.get("origin")
                shipment.carrier = request.POST.get("carrier")
                shipment.third_party_address = request.POST.get("third_party_address")
                shipment.load_type = request.POST.get("load_type")
                shipment.shipment_schduled_at = timezone.now()
                shipment.shipment_appointment = shipment_appointment
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace("WALMART","Walmart")
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                shipment.fleet_number = None
                shipment.ARM_BOL = None
                shipment.ARM_PRO = None
                await sync_to_async(fleet.delete)()
            elif shipment_type != "FTL":
                shipment.shipment_type = shipment_type
                shipment.shipment_account = request.POST.get("shipment_account")
                shipment.origin = request.POST.get("origin")
                shipment.shipment_appointment = shipment_appointment
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace("WALMART","Walmart")
                shipment.address = request.POST.get("address")
                shipment.appointment_id = request.POST.get("appointment_id", "")
                shipment.load_type = ""
                shipment.third_party_address = ""
                shipment.ARM_BOL = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ''
                shipment.ARM_PRO = request.POST.get("arm_pro") if request.POST.get("arm_pro") else ''
                current_time = datetime.now()
                fleet = Fleet(**{
                    "carrier": request.POST.get("carrier"),
                    "fleet_type": shipment_type,
                    "appointment_datetime": shipment_appointment,
                    "fleet_number": "FO" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper(),
                    "scheduled_at": current_time,
                    "total_weight": shipment.total_weight,
                    "total_cbm": shipment.total_cbm,
                    "total_pallet": shipment.total_pallet,
                    "total_pcs": shipment.total_pcs,
                    "origin": shipment.origin,
                })
                await sync_to_async(fleet.save)()
                if shipment.fleet_number:
                    await sync_to_async(shipment.fleet_number.delete)()
                shipment.fleet_number = fleet
        await sync_to_async(shipment.save)()
        mutable_get = request.GET.copy()
        mutable_get['warehouse'] = request.POST.get("warehouse")
        mutable_get['batch_number'] = batch_number
        request.GET = mutable_get
        return await self.handle_shipment_info_get(request)

    async def handle_appointment_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number", "container_number__order","container_number__order__retrieval_id"
            ).filter(
                location=warehouse,
                container_number__order__created_at__gte = '2024-09-01',
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
                "container_number", "container_number__order__retrieval_id","container_number__order__vessel_id"
            ).filter(
                (
                    models.Q(container_number__order__retrieval_id__retrieval_destination_precise=warehouse) |
                    models.Q(container_number__order__warehouse__name=warehouse)
                       
                ),
                container_number__order__created_at__gte = '2024-09-01',
                container_number__order__vessel_id__vessel_eta__gte=start_date,
                container_number__order__vessel_id__vessel_eta__lte=end_date,
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
                (models.Q(origin__isnull=True) | models.Q(origin="") | models.Q(origin=warehouse)),
                models.Q(in_use=False, is_canceled=False)
            ).order_by("shipment_appointment")
        )
        appointment_data = await sync_to_async(list)(
            Shipment.objects.filter(
                (models.Q(origin__isnull=True) | models.Q(origin="") | models.Q(origin=warehouse)),
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
        try:
            df = pd.merge(df_pallet, df_packing_list, how="outer", on=["destination", "warehouse"]).fillna(0)
            df = df.merge(df_appointment, how="left", on=["destination"]).fillna(0)
            df["total_cbm"] = df["total_cbm_x"] + df["total_cbm_y"]
            df["total_pallet"] = df["total_pallet_x"] + df["total_pallet_y"]
            df = df.drop(["total_cbm_x", "total_cbm_y", "total_pallet_x", "total_pallet_y"], axis=1)
        except:
            df = df_pallet
            try:
                df = df.merge(df_appointment, how="left", on=["destination"]).fillna(0)
                df["n_appointment"] = df["n_appointment"].astype(int)
                df["total_pallet"] = df["total_pallet"].astype(int)
            except:
                df["n_appointment"] = 0
                df["total_pallet"] = 0
        df = df.sort_values(by="total_pallet", ascending=False)
        context = {
            "appointment": appointment,
            "po_appointment_summary": df.to_dict("records"),
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options,
            "upload_file_form": UploadFileForm(),
            "start_date": start_date,
            "end_date": end_date,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "account_options": self.account_options
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
            "load_type": request.POST.get("load_type"),
            "origin": request.POST.get("origin", None),
            "shipment_account":request.POST.get("shipment_account",None),
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
                "load_type": d["load_type"].strip(),
                "shipment_account": d["shipment_account"].strip()
            } for d in data]
            await sync_to_async(Shipment.objects.bulk_create)(Shipment(**d) for d in cleaned_data)
        return await self.handle_appointment_warehouse_search_post(request)
    
    async def handle_shipment_list_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        appointmnet_start_date = request.POST.get("start_date")
        appointment_end_date = request.POST.get("end_date")
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number").filter(
                origin=warehouse,
                shipment_appointment__gte=appointmnet_start_date,
                shipment_appointment__lte=appointment_end_date,
                in_use=True,
            ).order_by("shipment_appointment")
        )
        context = {
            "warehouse": warehouse,
            "start_date": appointmnet_start_date,
            "end_date": appointment_end_date,
            "warehouse_options": self.warehouse_options,
            "shipment": shipment,
        }
        return self.template_shipment_list, context
    
    async def handle_fix_shipment_exceptions_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        solution = request.POST.get("solution")
        shipment_batch_number = request.POST.get("shipment_batch_number")
        if solution == "keep_old":
            shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
            shipment.status = ""
            shipment.priority = "P0"
            shipment.is_shipped = False
            shipment.shipped_at = None
            await sync_to_async(shipment.save)()
        else:
            old_shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
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
                    shipment.shipment_batch_number = request.POST.get("shipment_batch_number").strip().split('_')[0] + f"_{shipment.batch + 1}"
                    shipment.in_use = True
                    shipment.origin = request.POST.get("origin", "").strip()
                    shipment.shipment_type = shipment_type
                    shipment.load_type = request.POST.get("load_type", "").strip()
                    shipment.note = request.POST.get("note", "").strip()
                    shipment.shipment_schduled_at = timezone.now()
                    shipment.is_shipment_schduled = True
                    shipment.destination = request.POST.get("destination", "").replace("WALMART","Walmart").strip()
                    shipment.address = request.POST.get("address", "").strip()
                    shipment.master_batch_number = old_shipment.shipment_batch_number
                    shipment.total_weight = old_shipment.total_weight
                    shipment.total_cbm = old_shipment.total_cbm
                    shipment.total_pallet = old_shipment.total_pallet
                    shipment.total_pcs = old_shipment.total_pcs
                    shipment.shipped_weight = old_shipment.shipped_weight
                    shipment.shipped_cbm = old_shipment.shipped_cbm
                    shipment.shipped_pallet = old_shipment.shipped_pallet
                    shipment.shipped_pcs = old_shipment.shipped_pcs
                    shipment.pallet_dumpped = old_shipment.pallet_dumpped
                    shipment.previous_fleets = old_shipment.previous_fleets
                    if shipment_type != "FTL":
                        shipmentappointment = request.POST.get("shipment_appointment")
                        shipment_appointment = parse(shipmentappointment).replace(tzinfo=None)
                        fleet = Fleet(**{
                            "carrier": request.POST.get("carrier"),
                            "fleet_type": shipment_type,
                            "appointment_datetime": shipment_appointment, #车次的提货时间=约的提货时间
                            "fleet_number": "FO" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper(),
                            "scheduled_at": current_time,
                            "total_weight": shipment_data["total_weight"],
                            "total_cbm": shipment_data["total_cbm"],
                            "total_pallet": shipment_data["total_pallet"],
                            "total_pcs": shipment_data["total_pcs"],
                            "origin": shipment_data["origin"]
                        })
                        await sync_to_async(fleet.save)()
                        shipment.fleet_number = fleet
                        #LTL的需要存ARM-BOL和ARM-PRO
                        if shipment_type in ["LTL", "外配/快递"]:
                            shipment.ARM_BOL = request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
                            shipment.ARM_PRO = request.POST.get("arm_pro") if request.POST.get("arm_bol") else ""
                    try:
                        shipment.third_party_address = request.POST.get("third_party_address").strip()
                    except:
                        pass
            else:
                current_time = timezone.now()
                shipmentappointment = request.POST.get("shipment_appointment")
                shipment_appointment = parse(shipmentappointment).replace(tzinfo=None)
                shipment_data = {}
                shipment_data["appointment_id"] = request.POST.get("appointment_id", "").strip()
                shipment_data["third_party_address"] = request.POST.get("third_party_address", "").strip()
                shipment_data["shipment_type"] = shipment_type
                shipment_data["load_type"] = request.POST.get("load_type", "").strip()
                shipment_data["shipment_account"] = request.POST.get("shipment_account", "").strip()
                shipment_data["note"] = request.POST.get("note", "").strip()
                if shipment_type == "外配/快递":
                    shipment_data["shipment_appointment"] = request.POST.get("shipment_est_arrival", None)
                else:
                    shipment_data["shipment_appointment"] = request.POST.get("shipment_appointment", None)
                shipment_data["shipment_schduled_at"] = current_time
                shipment_data["is_shipment_schduled"] = True
                shipment_data["destination"] = request.POST.get("destination", "").strip()
                shipment_data["address"] = request.POST.get("address", "").strip()
                shipment_data["origin"] = request.POST.get("origin", "").strip()
                shipment_data["master_batch_number"] = old_shipment.shipment_batch_number
                shipment_data["shipment_batch_number"] = request.POST.get("shipment_batch_number").strip().split('_')[0] + f"_{old_shipment.batch + 1}"
                shipment_data["total_weight"] = old_shipment.total_weight
                shipment_data["total_cbm"] = old_shipment.total_cbm
                shipment_data["total_pallet"] = old_shipment.total_pallet
                shipment_data["total_pcs"] = old_shipment.total_pcs
                shipment_data["shipped_weight"] = old_shipment.shipped_weight
                shipment_data["shipped_cbm"] = old_shipment.shipped_cbm
                shipment_data["shipped_pallet"] = old_shipment.shipped_pallet
                shipment_data["shipped_pcs"] = old_shipment.shipped_pcs
                shipment_data["pallet_dumpped"] = old_shipment.pallet_dumpped
                shipment_data["previous_fleets"] = old_shipment.previous_fleets
                if shipment_type in ["LTL", "外配/快递"]:
                    shipment_data["ARM_BOL"] = request.POST.get("arm_bol", "").strip()
                    shipment_data["ARM_PRO"] = request.POST.get("arm_pro", "").strip()
                    fleet = Fleet(**{
                        "carrier": request.POST.get("carrier"),
                        "fleet_type": shipment_type,
                        "appointment_datetime": shipment_appointment,
                        "fleet_number": "FO" + current_time.strftime("%m%d%H%M%S") + str(uuid.uuid4())[:2].upper(),
                        "scheduled_at": current_time,
                        "total_weight": old_shipment.shipped_weight,
                        "total_cbm": old_shipment.shipped_cbm,
                        "total_pallet": old_shipment.shipped_pallet,
                        "total_pcs": old_shipment.shipped_pcs,
                        "origin": request.POST.get("origin", "").strip()
                    })
                    await sync_to_async(fleet.save)()
                    shipment_data["fleet_number"] = fleet
            if not existed_appointment:
                shipment = Shipment(**shipment_data)
            await sync_to_async(shipment.save)()
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("shipment_batch_number").filter(shipment_batch_number__shipment_batch_number=shipment_batch_number)
            )
            pallet = await sync_to_async(list)(
                Pallet.objects.select_related("shipment_batch_number").filter(shipment_batch_number__shipment_batch_number=shipment_batch_number)
            )
            for pl in packing_list:
                pl.shipment_batch_number = shipment
            for p in pallet:
                p.shipment_batch_number = shipment
            await sync_to_async(PackingList.objects.bulk_update)(packing_list, ["shipment_batch_number"])
            await sync_to_async(Pallet.objects.bulk_update)(pallet, ["shipment_batch_number"])
            old_shipment.is_canceled = True
            await sync_to_async(old_shipment.save)()
        return await self.handle_shipment_exceptions_get(request)
    
    async def handle_cancel_abnormal_appointment_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        shipment_batch_number = request.POST.get("batch_number")
        shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
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
        return await self.handle_shipment_exceptions_get(request)

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
                    "container_number__order__offload_id", "container_number__order__customer_name", "container_number__order__retrieval_id",
                    "container_number__order__vessel_id"
                ).filter(
                    plt_criteria
                ).annotate(
                    schedule_status=Case(
                        When(Q(container_number__order__offload_id__offload_at__lte=datetime.now().date() + timedelta(days=-7)), then=Value("past_due")),
                        default=Value("on_time"),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                    str_length=Cast("length", CharField()),
                    str_width=Cast("width", CharField()),
                    str_height=Cast("height", CharField()),
                    str_pcs=Cast("pcs",CharField())
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
                    'container_number__order__vessel_id__vessel_eta',
                    'sequence_number', 
                    target_retrieval_timestamp=F('container_number__order__retrieval_id__target_retrieval_timestamp'),
                    target_retrieval_timestamp_lower=F('container_number__order__retrieval_id__target_retrieval_timestamp_lower'),
                    temp_t49_pickup=F('container_number__order__retrieval_id__temp_t49_available_for_pickup'),
                    warehouse=F('container_number__order__retrieval_id__retrieval_destination_precise'),
                ).annotate(
                    eta=F('container_number__order__vessel_id__vessel_eta'),
                    custom_delivery_method=F('delivery_method'),
                    fba_ids=F('fba_id'),
                    ref_ids=F('ref_id'),
                    shipping_marks=F('shipping_mark'),
                    plt_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                    length=StringAgg("str_length", delimiter=",", ordering="str_length"),
                    width=StringAgg("str_width", delimiter=",", ordering="str_width"),
                    height=StringAgg("str_height", delimiter=",", ordering="str_height"),
                    n_pcs=StringAgg("str_pcs", delimiter=",", ordering="str_pcs"),
                ).order_by('container_number__order__offload_id__offload_at')
                .order_by('sequence_number')
            )
            data += pal_list
        if pl_criteria:
            pl_list =  await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number"
                    "container_number__order__offload_id", "container_number__order__customer_name", "pallet", "container_number__order__retrieval_id",
                    "container_number__order__vessel_id"
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
                    'container_number__order__vessel_id__vessel_eta',
                    target_retrieval_timestamp=F('container_number__order__retrieval_id__target_retrieval_timestamp'),
                    target_retrieval_timestamp_lower=F('container_number__order__retrieval_id__target_retrieval_timestamp_lower'),
                    warehouse=F('container_number__order__retrieval_id__retrieval_destination_precise'),
                    temp_t49_pickup=F('container_number__order__retrieval_id__temp_t49_available_for_pickup'),
                ).annotate(
                    eta=F('container_number__order__vessel_id__vessel_eta'),
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
    
    def _verify_empty_string(sefl, string_value: str) -> bool:
        if string_value is None:
            return True
        if isinstance(string_value, float) and math.isnan(string_value):
            return True
        if isinstance(string_value, str) and string_value.strip() == "":
            return True
        return False
    
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