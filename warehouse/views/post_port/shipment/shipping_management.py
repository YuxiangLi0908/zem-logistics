import ast
import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from dateutil.parser import parse
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Concat
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.fleet import Fleet
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.container import Container
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.transfer_location import TransferLocation
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    LOAD_TYPE_OPTIONS,
    SP_CLIENT_ID,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    amazon_fba_locations,
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
    template_outbound_departure = (
        "post_port/shipment/04_outbound_depature_confirmation.html"
    )
    template_delivery_and_pod = "post_port/shipment/05_delivery_and_pod.html"
    template_bol = "export_file/bol_base_template.html"
    template_appointment_management = (
        "post_port/shipment/06_appointment_management.html"
    )
    template_shipment_list = "post_port/shipment/07_shipment_list.html"
    template_shipment_list_shipment_display = (
        "post_port/shipment/07_1_shipment_list_shipment_display.html"
    )
    template_shipment_exceptions = (
        "post_port/shipment/exceptions/01_shipment_exceptions.html"
    )
    template_batch_shipment = "post_port/shipment/08_batch_shipment.html"
    area_options = {
        "NJ": "NJ",
        "SAV": "SAV",
        "LA": "LA",
        "NJ/SAV/LA": "NJ/SAV/LA",
        "MO": "MO",
        "TX": "TX",
    }
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }
    account_options = {
        "": "",
        "Carrier Central1": "Carrier Central1",
        "Carrier Central2": "Carrier Central2",
        "ZEM-AMF": "ZEM-AMF",
        "ARM-AMF": "ARM-AMF",
        "walmart": "walmart",
    }
    shipment_type_options = {
        "": "",
        "FTL": "FTL",
        "LTL": "LTL",
        "外配": "外配",
        "快递": "快递",
        "客户自提": "客户自提",
    }

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
        # raise ValueError(request.POST)
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
            template, context = await self.handle_appointment_warehouse_search_post(
                request
            )
            return render(request, template, context)
        elif step == "create_empty_appointment":
            template, context = await self.handle_create_empty_appointment_post(request)
            return render(request, template, context)
        elif step == "download_empty_appointment_template":
            return await self.handle_download_empty_appointment_template_post()
        elif step == "upload_and_create_empty_appointment":
            template, context = (
                await self.handle_upload_and_create_empty_appointment_post(request)
            )
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
            template, context = await self.handle_cancel_abnormal_appointment_post(
                request
            )
            return render(request, template, context)
        elif step == "upload_batch_shipment":
            template, context = await self.handle_batch_shipment_post(request)
            return render(request, template, context)
        else:
            return await self.get(request)

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
        return await self.handle_appointment_warehouse_search_post(request)

    async def handle_shipment_info_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        batch_number = request.GET.get("batch_number")
        mutable_post = request.POST.copy()
        mutable_post["area"] = request.GET.get("area")
        request.POST = mutable_post
        _, context = await self.handle_warehouse_post(request)
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        packing_list_selected = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=True,
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=False,
            ),
        )
        try:
            note = packing_list_selected[0]["note"]
        except Exception as e:
            note = ""
        context.update(
            {
                "shipment": shipment,
                "packing_list_selected": packing_list_selected,
                "load_type_options": LOAD_TYPE_OPTIONS,
                "account_options": self.account_options,
                "warehouse": request.GET.get("warehouse"),
                "warehouse_options": self.warehouse_options,
                "shipment_type_options": self.shipment_type_options,
                "start_date": request.GET.get("start_date"),
                "end_date": request.GET.get("end_date"),
                "express_number": note,
            }
        )
        return self.template_td_shipment_info, context

    async def handle_appointment_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {
            "load_type_options": LOAD_TYPE_OPTIONS,
            "warehouse_options": self.warehouse_options,
            "account_options": self.account_options,
            "start_date": (datetime.now().date() + timedelta(days=-7)).strftime(
                "%Y-%m-%d"
            ),
            "end_date": (datetime.now().date() + timedelta(days=7)).strftime(
                "%Y-%m-%d"
            ),
        }
        return self.template_appointment_management, context

    async def handle_shipment_list_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {
            "warehouse_options": self.warehouse_options,
            "start_date": (datetime.now().date() + timedelta(days=-7)).strftime(
                "%Y-%m-%d"
            ),
            "end_date": (datetime.now().date() + timedelta(days=14)).strftime(
                "%Y-%m-%d"
            ),
        }
        return self.template_shipment_list, context

    async def handle_shipment_detail_display_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
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
        shipment_selected = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        packing_list_selected = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=True,
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number=batch_number,
                container_number__order__offload_id__offload_at__isnull=False,
            ),
        )
        context["shipment_selected"] = shipment_selected
        context["packing_list_selected"] = packing_list_selected
        context["shipment_type_options"] = self.shipment_type_options
        context["load_type_options"] = LOAD_TYPE_OPTIONS
        return self.template_shipment_list_shipment_display, context

    async def handle_shipment_exceptions_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                status="Exception",
                is_canceled=False,
            ).order_by("shipment_appointment")
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
            Shipment.objects.filter(in_use=False, is_canceled=False)
        )
        unused_appointment = {
            s.appointment_id: {
                "destination": s.destination.strip(),
                "shipment_appointment": s.shipment_appointment.replace(
                    microsecond=0
                ).isoformat(),
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

    async def handle_over_shipment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
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
        # ETA过滤
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        start_date = (
            (datetime.now().date() + timedelta(days=-15)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            (datetime.now().date() + timedelta(days=15)).strftime("%Y-%m-%d")
            if not end_date
            else end_date
        )

        # year_2025 = datetime(2025, 1, 1)
        shipment = await sync_to_async(list)(
            Shipment.objects.prefetch_related(
                "packinglist",
                "packinglist__container_number",
                "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse",
                "order",
                "pallet",
                "fleet_number",
            )
            .filter(
                models.Q(
                    is_shipped=True,
                    in_use=True,
                    is_canceled=False,
                    shipment_appointment__gt=start_date,
                    shipment_appointment__lt=end_date,
                )
            )
            .distinct()
            .order_by("-abnormal_palletization", "shipment_appointment")
        )

        criteria_p = models.Q(
            (
                models.Q(container_number__order__order_type="转运")
                | models.Q(container_number__order__order_type="转运组合")
            ),
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,
            container_number__order__created_at__gte="2025-01-01",
            container_number__order__vessel_id__vessel_eta__gte="2025-01-01",
            container_number__order__vessel_id__vessel_etd__gte="2025-01-01",
        )
        pl_criteria = criteria_p & models.Q(
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=True,
        )
        plt_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=False,
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
        )
        if area == "NJ/SAV/LA":
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=[
                    "NJ",
                    "SAV",
                    "LA",
                ]
            )
            plt_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=[
                    "NJ",
                    "SAV",
                    "LA",
                ]
            )
        else:
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area=area
            )
            plt_criteria &= models.Q(location__startswith=area)

        packing_list_not_scheduled = await self._get_packing_list(
            pl_criteria, plt_criteria
        )
        cbm_act, cbm_est, pallet_act, pallet_est = 0, 0, 0, 0
        for pl in packing_list_not_scheduled:
            if pl.get("label") == "ACT":
                cbm_act += pl.get("total_cbm")
                pallet_act += pl.get("total_n_pallet_act")
            else:
                cbm_est += pl.get("total_cbm")
                if pl.get("total_n_pallet_est") < 1:
                    pallet_est += 1
                elif pl.get("total_n_pallet_est") % 1 >= 0.45:
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
            "modify_shipped_shipment": await sync_to_async(
                request.user.groups.filter(name="shipment_leader").exists
            )(),
            "shipped": True,
        }
        return self.template_td, context

    async def handle_batch_shipment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            df = df.rename(
                columns={
                    "车次编号": "fleet_zem_serial",
                    "提货时间": "appointment_datetime",
                    "ISA": "ISA",
                    "约批次号": "shipment_batch_number",
                    "送仓时间": "shipment_appointment",
                    "柜号": "container_number",
                    "目的地": "destination",
                    "板数": "pallets",
                    "cbm": "cbm",
                    "PO_ID": "PO_ID",
                    "carrier": "carrier",
                    "备注": "note",
                    "成本价": "cost_price",
                    "预约类型": "shipment_type",
                    "预约账号": "shipment_account",
                    "装车类型": "load_type",
                    "发货仓库": "origin",
                    "ARM_BOL": "ARM_BOL",
                    "ARM_PRO": "ARM_PRO",
                }
            )
            df["ISA"] = df["ISA"].apply(lambda x: str(int(x)) if pd.notna(x) else x)
            columns_to_clean = [
                "fleet_zem_serial",
                "ISA",
                "shipment_batch_number",
                "container_number",
                "destination",
                "PO_ID",
                "carrier",
                "note",
                "shipment_type",
                "shipment_account",
                "load_type",
                "origin",
                "AMF_ID",
                "ARM_BOL",
                "ARM_PRO",
            ]
            df[columns_to_clean] = df[columns_to_clean].applymap(
                lambda x: x.strip() if isinstance(x, str) else x
            )
            df = df.dropna(how="all")
            duplicated_isa = (
                df[df.duplicated(subset=["ISA"], keep=False)]["ISA"].dropna().unique()
            )
            duplicated_fleet = (
                df[df.duplicated(subset=["fleet_zem_serial"], keep=False)][
                    "fleet_zem_serial"
                ]
                .dropna()
                .unique()
            )
            upload_data = df.to_dict(orient="records")
            data = {}
            isa_hash = []
            fleet_hash = []
            if duplicated_isa:
                raise ValueError(f"以下ISA重复出现: {', '.join(duplicated_isa)}")
            if duplicated_fleet:
                raise ValueError(f"以下车次重复出现: {', '.join(duplicated_fleet)}")
            for item in upload_data:
                if not self._verify_empty_string(item["ISA"]):
                    if self._verify_empty_string(item["shipment_appointment"]):
                        raise RuntimeError(f"ISA {item['ISA']} 没有填写送仓时间!")
                    if self._verify_empty_string(item["shipment_type"]):
                        raise RuntimeError(f"ISA {item['ISA']} 没有填写预约类型!")
                    if self._verify_empty_string(item["shipment_account"]):
                        raise RuntimeError(f"ISA {item['ISA']} 没有填写预约账号!")
                    if self._verify_empty_string(item["load_type"]):
                        raise RuntimeError(f"ISA {item['ISA']} 没有填写装车类型!")
                    if self._verify_empty_string(item["origin"]):
                        raise RuntimeError(f"ISA {item['ISA']} 没有填写发货仓库!")
                fleet_serial = item["fleet_zem_serial"]
                if not self._verify_empty_string(fleet_serial):
                    if self._verify_empty_string(item["appointment_datetime"]):
                        raise RuntimeError(f"车次 {fleet_serial} 没有填写提货时间!")
                    # 说明是一个新的车次，那约肯定也是新的
                    # 记录最近一次的约和ISA
                    last_fleet = fleet_serial
                    last_fleet = last_fleet.strip()
                    fleet_hash.append(last_fleet)
                    last_ISA = item["ISA"]
                    if self._verify_empty_string(last_ISA):
                        raise ValueError(f"车次({last_fleet})缺少ISA!")
                    last_ISA = last_ISA.strip()
                    isa_hash.append(last_ISA)
                    appointment_datetime, appointment_datetime_tz = (
                        self._parse_datetime(item["appointment_datetime"])
                    )
                    shipment_appointment, shipment_appointment_tz = (
                        self._parse_datetime(item["shipment_appointment"])
                    )
                    # 构建一个车次的字典
                    data[last_fleet] = {
                        "appointment_datetime": appointment_datetime,
                        "appointment_datetime_tz": appointment_datetime_tz,
                        "carrier": item["carrier"].strip(),
                        "origin": item["origin"],
                        "cost_price": item["cost_price"],
                        "amf_id": item["AMF_ID"],
                        "fleet_type": item["shipment_type"],
                        "shipment": {
                            last_ISA: {
                                "shipment_batch_number": item["shipment_batch_number"],
                                "shipment_appointment": shipment_appointment,
                                "shipment_appointment_tz": shipment_appointment_tz,
                                "carrier": item["carrier"].strip(),
                                "note": item["note"],
                                "shipment_type": item["shipment_type"],
                                "shipment_account": item["shipment_account"],
                                "load_type": item["load_type"],
                                "origin": item["origin"],
                                "ARM_BOL": item["ARM_BOL"],
                                "ARM_PRO": item["ARM_PRO"],
                                "PO": [
                                    {
                                        "PO_ID": item["PO_ID"],
                                        "container_number": item["container_number"],
                                        "destination": item["destination"],
                                        "pallets": item["pallets"],
                                        "cbm": item["cbm"],
                                    }
                                ],
                            }
                        },
                    }
                else:
                    # 说明该行前面已建了该车次的信息
                    if not self._verify_empty_string(item["ISA"]):
                        ISA = item["ISA"]
                        # 说明是一行新的预约批次
                        last_ISA = ISA
                        last_ISA = last_ISA.strip()
                        isa_hash.append(last_ISA)
                        shipment_appointment, shipment_appointment_tz = (
                            self._parse_datetime(item["shipment_appointment"])
                        )
                        data[last_fleet]["shipment"][last_ISA] = {
                            "shipment_appointment": shipment_appointment,
                            "shipment_appointment_tz": shipment_appointment_tz,
                            "carrier": item["carrier"].strip(),
                            "note": item["note"],
                            "shipment_type": item["shipment_type"],
                            "shipment_account": item["shipment_account"],
                            "load_type": item["load_type"],
                            "origin": item["origin"],
                            "ARM_BOL": item["ARM_BOL"],
                            "ARM_PRO": item["ARM_PRO"],
                            "PO": [
                                {
                                    "PO_ID": item["PO_ID"],
                                    "container_number": item["container_number"],
                                    "destination": item["destination"],
                                    "pallets": item["pallets"],
                                    "cbm": item["cbm"],
                                }
                            ],
                        }
                    else:
                        # 说明该行只有柜号 目的地 板数 cbm
                        data[last_fleet]["shipment"][last_ISA]["PO"].append(
                            {
                                "PO_ID": item["PO_ID"],
                                "container_number": item["container_number"],
                                "destination": item["destination"],
                                "pallets": item["pallets"],
                                "cbm": item["cbm"],
                            }
                        )
            # 构建完数据，开始预约
            created_isa, created_fleet, failed_fleet = (
                await self._batch_process_fleet_isa_data(data, isa_hash, fleet_hash)
            )
            context = {
                "created_isa": created_isa,
                "created_fleet": created_fleet,
                "failed_fleet": failed_fleet,
            }
        return self.template_batch_shipment, context

    async def _batch_process_fleet_isa_data(
        self,
        data: dict[str, Any],
        isa_list: list[str],
        fleet_list: list[str],
    ) -> tuple[list[Any]]:
        db_shipment = await sync_to_async(list)(
            Shipment.objects.filter(appointment_id__in=isa_list)
        )
        db_shipment = {s.appointment_id: s for s in db_shipment}
        db_fleet = await sync_to_async(list)(
            Fleet.objects.filter(fleet_zem_serial__in=fleet_list)
        )
        db_fleet = {f.fleet_zem_serial: f for f in db_fleet}
        created_isa = []
        created_fleet = []
        failed_fleet = []
        current_time = datetime.now()
        for fleet_serial, fleet_data in data.items():
            # 判断车次是否可以创建
            # 1. 车次号不能出现在数据库, fleet_data不是空值
            # 2. ISA不能过期、删除、已在使用中
            # 3. PO_ID全部出现在数据库
            failed = False
            if fleet_serial in db_fleet:
                failed_fleet.append(
                    {
                        "fleet_serial": fleet_serial,
                        "ISA": ", ".join(fleet_data["shipment"].keys()),
                        "reason": f"系统已存在车次 {fleet_serial}",
                    }
                )
                failed = True
            elif not isinstance(fleet_data, dict) or not fleet_data:
                failed_fleet.append(
                    {
                        "fleet_serial": fleet_serial,
                        "ISA": ", ".join(fleet_data["shipment"].keys()),
                        "reason": f"车次 {fleet_serial} 缺少数据",
                    }
                )
                failed = True
            else:
                for ISA, ISA_data in fleet_data["shipment"].items():
                    if ISA in db_shipment:
                        if db_shipment[ISA].in_use:
                            failed_fleet.append(
                                {
                                    "fleet_serial": fleet_serial,
                                    "ISA": ", ".join(fleet_data["shipment"].keys()),
                                    "reason": f"ISA {ISA} 已创建",
                                }
                            )
                            failed = True
                            break
                        elif db_shipment[ISA].is_canceled:
                            failed_fleet.append(
                                {
                                    "fleet_serial": fleet_serial,
                                    "ISA": ", ".join(fleet_data["shipment"].keys()),
                                    "reason": f"ISA {ISA} 已删约",
                                }
                            )
                            failed = True
                            break
                        elif (
                            db_shipment[ISA].shipment_appointment.replace(
                                tzinfo=pytz.UTC
                            )
                            < timezone.now()
                        ):
                            failed_fleet.append(
                                {
                                    "fleet_serial": fleet_serial,
                                    "ISA": ", ".join(fleet_data["shipment"].keys()),
                                    "reason": f"ISA {ISA} 已过期",
                                }
                            )
                            failed = True
                            break
                    po_list = ISA_data["PO"]
                    failed = False
                    for p in po_list:
                        packing_lists, pallets = await self._get_packing_list_non_agg(
                            models.Q(
                                PO_ID=p["PO_ID"], shipment_batch_number__isnull=True
                            ),
                            models.Q(
                                PO_ID=p["PO_ID"], shipment_batch_number__isnull=True
                            ),
                        )
                        for po in ISA_data["PO"]:
                            if po["PO_ID"] == p["PO_ID"]:
                                if packing_lists:
                                    # 比较pl的cbm
                                    total_cbm = sum(
                                        item.get("cbm", 0) for item in packing_lists
                                    )
                                    if abs(total_cbm - po["cbm"]) > 0.1:
                                        failed_fleet.append(
                                            {
                                                "fleet_serial": fleet_serial,
                                                "ISA": ", ".join(
                                                    fleet_data["shipment"].keys()
                                                ),
                                                "reason": f"PO_ID {po['PO_ID']} cbm不对",
                                            }
                                        )
                                        failed = True
                                if pallets:
                                    # 比较plt的板数
                                    if len(pallets) != int(po["pallets"]):
                                        failed_fleet.append(
                                            {
                                                "fleet_serial": fleet_serial,
                                                "ISA": ", ".join(
                                                    fleet_data["shipment"].keys()
                                                ),
                                                "reason": f"PO_ID {po['PO_ID']} 数量不对",
                                            }
                                        )
                                        failed = True
                    po_id_list = [p["PO_ID"] for p in po_list]
                    packing_lists, pallets = await self._get_packing_list_non_agg(
                        models.Q(
                            PO_ID__in=po_id_list, shipment_batch_number__isnull=True
                        ),
                        models.Q(
                            PO_ID__in=po_id_list, shipment_batch_number__isnull=True
                        ),
                    )
                    db_po_id = set([p.PO_ID for p in packing_lists + pallets])
                    po_id_set = set(po_id_list)
                    if db_po_id != po_id_set:
                        diff = db_po_id.symmetric_difference(po_id_set)
                        failed_fleet.append(
                            {
                                "fleet_serial": fleet_serial,
                                "ISA": ", ".join(fleet_data["shipment"].keys()),
                                "reason": f"PO_ID不存在: {', '.join(diff)}",
                            }
                        )
                        failed = True
            if failed:
                continue

            fleet_weight, fleet_cbm, fleet_pallet, fleet_pcs = 0, 0, 0, 0
            fleet_isa = []
            n = 0
            for ISA, ISA_data in fleet_data["shipment"].items():
                shipment_weight, shipment_cbm, shipment_pallet, shipment_pcs = (
                    0,
                    0,
                    0,
                    0,
                )
                # PO
                po_list = ISA_data["PO"]
                po_id_list = [p["PO_ID"] for p in po_list]
                packing_lists, pallets = await self._get_packing_list_non_agg(
                    models.Q(PO_ID__in=po_id_list),
                    models.Q(PO_ID__in=po_id_list),
                )
                for pl in packing_lists:
                    shipment_weight += pl.total_weight_lbs
                    shipment_cbm += pl.cbm
                    shipment_pcs += pl.pcs
                shipment_pallet += len(set([p.pallet_id for p in pallets]))
                fleet_weight += shipment_weight
                fleet_cbm += shipment_cbm
                fleet_pallet += shipment_pallet
                fleet_pcs += shipment_pcs
                # Shipment
                batch_id = (
                    ISA_data["PO"][0]["destination"]
                    + current_time.strftime("%m%d%H%M%S")
                    + str(uuid.uuid4())[:2].upper()
                )
                batch_id = batch_id.replace(" ", "").replace("/", "-").upper()
                if ISA in db_shipment:
                    shipment = db_shipment[ISA]
                    destination = (
                        shipment.destination
                        if shipment.destination
                        else packing_lists[0].destination
                    )
                    if destination in amazon_fba_locations:
                        fba = amazon_fba_locations[destination]
                        address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
                    else:
                        address, zipcode = str(packing_lists[0].get("address")), str(
                            packing_lists[0].get("zipcode")
                        )
                        if zipcode.lower() not in address.lower():
                            address += f", {zipcode}"
                    shipment.shipment_batch_number = batch_id
                    shipment.in_use = True
                    shipment.origin = ISA_data["origin"]
                    shipment.shipment_type = ISA_data["shipment_type"]
                    shipment.note = ISA_data["note"]
                    shipment.shipment_account = ISA_data["shipment_account"]
                    shipment.load_type = ISA_data["load_type"]
                    shipment.shipment_appointment = ISA_data["shipment_appointment"]
                    shipment.shipment_appointment_tz = ISA_data[
                        "shipment_appointment_tz"
                    ]
                    shipment.carrier = ISA_data["carrier"]
                    shipment.address = address
                    shipment.total_weight = shipment_weight
                    shipment.total_cbm = shipment_cbm
                    shipment.total_pallet = shipment_pallet
                    shipment.total_cbm = shipment_cbm
                    shipment.is_shipment_schduled = True
                    shipment.shipment_schduled_at = current_time
                else:
                    destination = packing_lists[0].destination
                    if destination in amazon_fba_locations:
                        fba = amazon_fba_locations[destination]
                        address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
                    else:
                        address, zipcode = str(packing_lists[0].get("address")), str(
                            packing_lists[0].get("zipcode")
                        )
                        if zipcode.lower() not in address.lower():
                            address += f", {zipcode}"
                    shipment = Shipment(
                        **{
                            "shipment_batch_number": batch_id,
                            "destination": destination,
                            "address": address,
                            "total_weight": shipment_weight,
                            "total_cbm": shipment_cbm,
                            "total_pallet": shipment_pallet,
                            "total_pcs": shipment_cbm,
                            "appointment_id": ISA,
                            "shipment_account": ISA_data["shipment_account"],
                            "shipment_type": ISA_data["shipment_type"],
                            "load_type": ISA_data["load_type"],
                            "origin": ISA_data["origin"],
                            "shipment_appointment": ISA_data["shipment_appointment"],
                            "shipment_appointment_tz": ISA_data[
                                "shipment_appointment_tz"
                            ],
                            "carrier": ISA_data["carrier"],
                            "note": ISA_data["note"],
                            "is_shipment_schduled": True,
                            "shipment_schduled_at": current_time,
                        }
                    )
                fleet_isa.append(
                    {
                        "shipment": shipment,
                        "packing_list": packing_lists,
                        "pallet": pallets,
                    }
                )
                n += 1
            # 创建车次
            fleet = Fleet(
                **{
                    "fleet_number": "FO"
                    + current_time.strftime("%m%d%H%M%S")
                    + str(uuid.uuid4())[:2].upper(),
                    "fleet_zem_serial": fleet_serial,
                    "carrier": fleet_data["carrier"],
                    "appointment_datetime": fleet_data["appointment_datetime"],
                    "appointment_datetime_tz": fleet_data["appointment_datetime_tz"],
                    "fleet_type": fleet_data["fleet_type"],
                    "scheduled_at": current_time,
                    "total_weight": fleet_weight,
                    "total_cbm": fleet_cbm,
                    "total_pallet": fleet_pallet,
                    "total_pcs": fleet_pcs,
                    "origin": fleet_data["origin"],
                    "amf_id": fleet_data["amf_id"],
                    "multipule_destination": n > 1,
                }
            )
            await sync_to_async(fleet.save)()
            created_fleet.append(fleet)
            for d in fleet_isa:
                shipment, packing_lists, pallets = (
                    d["shipment"],
                    d["packing_list"],
                    d["pallet"],
                )
                shipment.fleet_number = fleet
                await sync_to_async(shipment.save)()
                created_isa.append(shipment)
                for pl in packing_lists:
                    pl.shipment_batch_number = shipment
                for p in pallets:
                    p.shipment_batch_number = shipment
                await sync_to_async(bulk_update_with_history)(
                    packing_lists,
                    PackingList,
                    fields=["shipment_batch_number"],
                )
                await sync_to_async(bulk_update_with_history)(
                    pallets,
                    Pallet,
                    fields=["shipment_batch_number"],
                )
        return created_isa, created_fleet, failed_fleet

    async def handle_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
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
        if area == "NJ/SAV/LA":
            criteria = (
                models.Q(
                    packinglist__container_number__order__retrieval_id__retrieval_destination_area="NJ"
                )
                | models.Q(
                    packinglist__container_number__order__retrieval_id__retrieval_destination_area="SAV"
                )
                | models.Q(
                    packinglist__container_number__order__retrieval_id__retrieval_destination_area="LA"
                )
                | models.Q(pallet__location__startswith="NJ")
                | models.Q(pallet__location__startswith="SAV")
                | models.Q(pallet__location__startswith="LA")
            )
        else:
            criteria = models.Q(
                packinglist__container_number__order__retrieval_id__retrieval_destination_area=area
            ) | models.Q(pallet__location__startswith=area)
        year_2025 = datetime(2025, 4, 1)
        shipment = await sync_to_async(list)(
            Shipment.objects.prefetch_related(
                "packinglist",
                "packinglist__container_number",
                "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse",
                "order",
                "pallet",
                "fleet_number",
            )
            .filter(
                criteria
                & models.Q(
                    is_shipped=False,
                    in_use=True,
                    is_canceled=False,
                    shipment_appointment__isnull=False,
                    shipment_appointment__gt=year_2025,
                )
            )
            .distinct()
            .order_by("-abnormal_palletization", "shipment_appointment")
        )
        # ETA过滤
        start_date = (
            request.POST.get("start_date")
            if "start_date" in request.POST
            else request.GET.get("start_date")
        )
        end_date = (
            request.POST.get("end_date")
            if "end_date" in request.POST
            else request.GET.get("end_date")
        )
        start_date = (
            (datetime.now().date() + timedelta(days=-15)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            (datetime.now().date() + timedelta(days=15)).strftime("%Y-%m-%d")
            if not end_date
            else end_date
        )
        criteria_p = models.Q(
            (
                models.Q(container_number__order__order_type="转运")
                | models.Q(container_number__order__order_type="转运组合")
            ),
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,
            # container_number__order__created_at__gte="2024-09-01",
        )
        pl_criteria = criteria_p & models.Q(
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=True,
        )
        plt_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=False,
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
        )
        if area == "NJ/SAV/LA":
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=[
                    "NJ",
                    "SAV",
                    "LA",
                ]
            )
            plt_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area__in=[
                    "NJ",
                    "SAV",
                    "LA",
                ]
            )
        else:
            pl_criteria &= models.Q(
                container_number__order__retrieval_id__retrieval_destination_area=area
            )
            plt_criteria &= models.Q(location__startswith=area)
        packing_list_not_scheduled = await self._get_packing_list(
            pl_criteria, plt_criteria
        )
        # 与转仓表进行比较，将转仓表中记录的，所有转仓的柜子，ETA和入仓时间按照转仓来显示
        trans = await sync_to_async(list)(
            TransferLocation.objects.all().values("ETA", "arrival_time", "plt_ids")
        )
        for pl in packing_list_not_scheduled:
            if "plt_ids" in pl:  # 只有打板后的才可能有转仓记录
                pl_id = pl.get("plt_ids")
                if "," in pl_id:
                    id_l = pl_id.split(",")
                    pl_id = id_l[0]
                for t in trans:
                    if pl_id in t.get("plt_ids"):
                        pl["eta"] = t.get("ETA")
                        pl["container_number__order__offload_id__offload_at"] = t.get(
                            "arrival_time"
                        )
        cbm_act, cbm_est, pallet_act, pallet_est = 0, 0, 0, 0
        for pl in packing_list_not_scheduled:
            if pl.get("label") == "ACT":
                cbm_act += pl.get("total_cbm")
                pallet_act += pl.get("total_n_pallet_act")
            else:
                cbm_est += pl.get("total_cbm")
                if pl.get("total_n_pallet_est") < 1:
                    pallet_est += 1
                elif pl.get("total_n_pallet_est") % 1 >= 0.45:
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
            "modify_shipped_shipment": await sync_to_async(
                request.user.groups.filter(name="shipment_leader").exists
            )(),
        }
        return self.template_td, context

    async def handle_selection_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
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
            destination = packing_list_selected[0].get("destination", "RDM")
            batch_id = (
                destination
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            disallowed_chars = "#%*：:<>?/|"  # 不允许的字符
            for char in disallowed_chars:
                batch_id = batch_id.replace(char, "-")
            batch_id = batch_id.replace(" ", "").upper()
            address = amazon_fba_locations.get(
                destination, None
            )  # 查找亚马逊地址中是否有该地址
            if destination in amazon_fba_locations:
                fba = amazon_fba_locations[destination]
                address = (
                    f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
                )
            else:
                address, zipcode = str(packing_list_selected[0].get("address")), str(
                    packing_list_selected[0].get("zipcode")
                )
                if (
                    zipcode.lower() not in address.lower()
                ):  # 如果不在亚马逊地址中，就从packing_list_selected的第一个元素获取地址和编码，转为字符串类型
                    address += f", {zipcode}"  # 如果编码不在地址字符串内，将邮编添加到字符串后面
            shipment_data = {
                "shipment_batch_number": str(batch_id),
                "destination": destination,
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": total_pallet,
                "total_pcs": total_pcs,
            }
            unused_appointment = await sync_to_async(list)(
                Shipment.objects.filter(in_use=False, is_canceled=False)
            )
            unused_appointment = {
                s.appointment_id: {
                    "destination": s.destination.strip(),
                    "shipment_appointment": s.shipment_appointment.replace(
                        microsecond=0
                    ).isoformat(),
                }
                for s in unused_appointment
            }
            note = packing_list_selected[0]["note"]
            context.update(
                {
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
                    "express_number": note,
                }
            )
            # 更新完约的信息后，加一个功能，把预约出库时选中的pl对应的order表等级改为P3
            container_numbers = await sync_to_async(
                lambda: list(
                    Order.objects.filter(
                        models.Q(container_number__packinglist__id__in=selected) |
                        models.Q(container_number__pallet__id__in=selected_plt)
                    )
                    .values_list("container_number__container_number", flat=True)
                    .distinct()
                )
            )()
            for cn in container_numbers:
                await self._update_container_unpacking_priority(cn)

            return self.template_td_schedule, context
        else:
            return await self.handle_warehouse_post(request)

    async def _update_container_unpacking_priority(
        self, container_number:str
    ) -> None:
        # 把所有有快递的，都直接优先级定位P1
        has_ups_fedex = await sync_to_async(
            lambda: (
                PackingList.objects.filter(
                    container_number__container_number=container_number,
                    delivery_method__in=["UPS", "FEDEX"]
                ).exists()
                or
                Pallet.objects.filter(
                    container_number__container_number=container_number,
                    delivery_method__in=["UPS", "FEDEX"]
                ).exists()
            )
        )()
        if has_ups_fedex:
            priority = "P1"
        else:
            is_expiry_guaranteed = await sync_to_async(
                lambda: Container.objects.filter(
                    container_number=container_number,
                    is_expiry_guaranteed=True
                ).exists()
            )()
            if is_expiry_guaranteed:
                priority = "P2"
            else:
                has_shipment = await sync_to_async(
                    lambda: (
                        PackingList.objects.filter(
                            container_number__container_number=container_number,
                            shipment_batch_number__isnull=False
                        ).exists()
                        or
                        Pallet.objects.filter(
                            container_number__container_number=container_number,
                            shipment_batch_number__isnull=False
                        ).exists()
                    )
                )()
                priority = "P3" if has_shipment else "P4"
        await sync_to_async(
            lambda: Order.objects.filter(
                container_number__container_number=container_number
            ).update(unpacking_priority=priority)
        )()

    async def handle_appointment_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        area = request.POST.get("area")
        current_time = datetime.now()
        appointment_type = request.POST.get("type")
        if appointment_type == "td":  # 首次预约、更新预约、取消预约都是这个类型
            shipment_data = ast.literal_eval(request.POST.get("shipment_data"))
            if 'shipment_schduled_at' not in shipment_data or shipment_data['shipment_schduled_at'] is None:
                #港后新sop传过来的没有当前操作时间
                shipment_data['shipment_schduled_at'] = datetime.now(timezone.utc)
            shipment_type = request.POST.get("shipment_type")
            appointment_id = request.POST.get("appointment_id", None)
            appointment_id = appointment_id.strip() if appointment_id else None
            try:
                existed_appointment = await sync_to_async(Shipment.objects.get)(
                    appointment_id=appointment_id
                )
            except:
                existed_appointment = None
            if existed_appointment:
                if existed_appointment.in_use:
                    raise RuntimeError(f"ISA {appointment_id} 已经登记过了!")
                elif existed_appointment.is_canceled:
                    raise RuntimeError(
                        f"ISA {appointment_id} already exists and is canceled!"
                    )
                elif (
                    existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC)
                    < timezone.now()
                ):
                    raise RuntimeError(
                        f"ISA {appointment_id} 预约时间小于当前时间，已过期!"
                    )
                elif existed_appointment.destination.replace("Walmart", "").replace(
                    "WALMART", ""
                ).replace("-", "") != request.POST.get("destination", None).replace(
                    "Walmart", ""
                ).replace(
                    "WALMART", ""
                ).replace(
                    "-", ""
                ):
                    raise ValueError(
                        f"ISA {appointment_id} 登记的目的地是 {existed_appointment.destination} ，此次登记的目的地是 {request.POST.get('destination', None)}!"
                    )
                else:  # 没有特殊情况就更新约的信息
                    tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
                    shipment = existed_appointment
                    shipment.shipment_batch_number = shipment_data[
                        "shipment_batch_number"
                    ]
                    shipment.in_use = True
                    shipment.origin = request.POST.get("origin", "")
                    shipment.shipment_type = shipment_type
                    shipment.load_type = request.POST.get("load_type", None)
                    shipment.note = request.POST.get("note", "")
                    shipment.shipment_schduled_at = timezone.now()
                    shipment.is_shipment_schduled = True
                    shipment.destination = request.POST.get(
                        "destination", None
                    ).replace("WALMART", "Walmart")
                    shipment.address = request.POST.get("address", None)
                    shipment.shipment_account = request.POST.get(
                        "shipment_account", ""
                    ).strip()
                    shipmentappointment = request.POST.get("shipment_appointment", None)
                    shipment.shipment_appointment = shipmentappointment
                    shipment.shipment_appointment_utc = self._parse_ts(
                        shipmentappointment, tzinfo
                    )
                    # LTL的需要存ARM-BOL和ARM-PRO
                    shipment.ARM_BOL = (
                        request.POST.get("arm_bol")
                        if request.POST.get("arm_bol")
                        else ""
                    )
                    shipment.ARM_PRO = (
                        request.POST.get("arm_pro")
                        if request.POST.get("arm_bol")
                        else ""
                    )
                    try:
                        shipment.third_party_address = shipment_data[
                            "third_party_address"
                        ].strip()
                    except:
                        pass
            else:
                if await self._shipment_exist(shipment_data["shipment_batch_number"]):
                    raise ValueError(
                        f"约批次 {shipment_data['shipment_batch_number']} 已经存在!"
                    )
                shipment_data["appointment_id"] = request.POST.get(
                    "appointment_id", None
                )
                try:
                    shipment_data["third_party_address"] = shipment_data[
                        "third_party_address"
                    ].strip()
                except:
                    pass
                if shipment_type == "快递":
                    shipmentappointment = request.POST.get("shipment_est_arrival", None)
                    tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
                    if shipmentappointment == "":
                        shipmentappointment = current_time
                        shipmentappointment_utc = current_time
                    else:
                        shipmentappointment_utc = self._parse_ts(
                            shipmentappointment, tzinfo
                        )
                    # if "NJ" in str(
                    #     request.POST.get("origin", "")
                    # ):  # NJ仓的，UPS预约完就结束，POD都不用传，现在三个仓库都不用传了，这段就注释掉了
                    shipment_data["express_number"] = (
                        request.POST.get("express_number")
                        if request.POST.get("express_number")
                        else ""
                    )
                    shipment_data["is_shipped"] = True
                    shipment_data["shipped_at"] = shipmentappointment
                    shipment_data["shipped_at_utc"] = shipmentappointment_utc
                    shipment_data["is_arrived"] = True
                    shipment_data["arrived_at"] = shipmentappointment
                    shipment_data["arrived_at_utc"] = shipmentappointment_utc
                    shipment_data["pod_link"] = "Without"
                    shipment_data["pod_uploaded_at"] = timezone.now()
                elif shipment_type == "外配":
                    shipmentappointment = request.POST.get("shipment_est_arrival", None)
                    tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
                    if shipmentappointment == "":
                        shipmentappointment = current_time
                        shipmentappointment_utc = current_time
                    else:
                        shipmentappointment_utc = self._parse_ts(
                            shipmentappointment, tzinfo
                        )
                    shipment_data["express_number"] = (
                        request.POST.get("express_number")
                        if request.POST.get("express_number")
                        else ""
                    )
                    shipment_data["is_shipped"] = True
                    shipment_data["shipped_at"] = shipmentappointment
                    shipment_data["shipped_at_utc"] = shipmentappointment_utc
                    shipment_data["is_arrived"] = True
                    shipment_data["arrived_at"] = shipmentappointment
                    shipment_data["arrived_at_utc"] = shipmentappointment_utc
                else:
                    shipmentappointment = request.POST.get("shipment_appointment", None)
                    tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
                    shipmentappointment_utc = self._parse_ts(
                        shipmentappointment, tzinfo
                    )
                    if shipment_type == "客户自提" and "NJ" in str(
                        request.POST.get("origin", "")
                    ):  # 客户自提的预约完要直接跳到POD上传,时间按预计提货时间
                        shipment_data["is_shipped"] = True
                        shipment_data["shipped_at"] = shipmentappointment
                        shipment_data["shipped_at_utc"] = shipmentappointment_utc
                        shipment_data["is_arrived"] = True
                        shipment_data["arrived_at"] = shipmentappointment
                        shipment_data["arrived_at_utc"] = shipmentappointment_utc
                shipment_data["shipment_type"] = shipment_type
                shipment_data["load_type"] = request.POST.get("load_type", None)
                shipment_data["note"] = request.POST.get("note", "")
                shipment_data["shipment_schduled_at"] = current_time
                shipment_data["is_shipment_schduled"] = True
                shipment_data["destination"] = request.POST.get("destination", None)
                shipment_data["address"] = request.POST.get("address", None)
                shipment_data["origin"] = request.POST.get("origin", "")
                shipment_data["shipment_account"] = request.POST.get(
                    "shipment_account", ""
                ).strip()
                shipment_data["shipment_appointment"] = (
                    shipmentappointment  # FTL和外配快递的scheduled time表示预计到仓时间，LTL和客户自提的提货时间
                )
                shipment_data["shipment_appointment_utc"] = shipmentappointment_utc
                if shipment_type != "FTL":
                    appointment_datetime = request.POST.get(
                        "shipment_appointment", None
                    )
                    if appointment_datetime == "":
                        appointment_datetime = current_time
                    if not appointment_datetime:
                        appointment_datetime = None
                    # 自动生成pickupNumber
                    wh = request.POST.get("origin", "").split("-")[1]
                    ca = request.POST.get("carrier").strip()
                    if isinstance(appointment_datetime, str):
                        dt = datetime.fromisoformat(
                            appointment_datetime.replace("Z", "+00:00")
                        )
                    else:
                        dt = appointment_datetime
                    month_day = dt.strftime("%m%d")
                    pickupNumber = "ZEM" + "-" + wh + "-" + "" + month_day + ca
                    fleet = Fleet(
                        **{
                            "carrier": request.POST.get("carrier").strip(),
                            "fleet_type": shipment_type,
                            "pickup_number": pickupNumber,
                            "appointment_datetime": appointment_datetime,  # 车次的提货时间
                            "fleet_number": "FO"
                            + current_time.strftime("%m%d%H%M%S")
                            + str(uuid.uuid4())[:2].upper(),
                            "scheduled_at": current_time,
                            "total_weight": shipment_data["total_weight"],
                            "total_cbm": shipment_data["total_cbm"],
                            "total_pallet": shipment_data["total_pallet"],
                            "total_pcs": shipment_data["total_pcs"],
                            "origin": shipment_data["origin"],
                        }
                    )
                    # NJ仓的客户自提和UPS，都不需要确认出库和确认到达，客户自提需要POD上传
                    if (
                        shipment_type == "客户自提"
                        or shipment_type == "外配"
                    ) and "NJ" in str(request.POST.get("origin", "")):
                        fleet.departured_at = shipmentappointment
                        fleet.arrived_at = shipmentappointment
                    if shipment_type == "快递":
                        fleet.departured_at = shipmentappointment
                        fleet.arrived_at = shipmentappointment
                    await sync_to_async(fleet.save)()
                    shipment_data["fleet_number"] = fleet
                    # LTL的需要存ARM-BOL和ARM-PRO
                    shipment_data["ARM_BOL"] = (
                        request.POST.get("arm_bol")
                        if request.POST.get("arm_bol")
                        else ""
                    )
                    shipment_data["ARM_PRO"] = (
                        request.POST.get("arm_pro")
                        if request.POST.get("arm_bol")
                        else ""
                    )
            print('期望的参数是',shipment_data)
            if not existed_appointment:
                shipment = Shipment(**shipment_data)
            await sync_to_async(shipment.save)()
            # 上面更新完约的信息，下面要更新packinglist绑定的约,这是未打板的，所有不用管板子
            container_number = set()
            if name != "post_nsop":
                pl_ids = request.POST.get("pl_ids").strip("][").split(", ")
            else:
                pl_ids = request.POST.get("pl_ids")
            try:
                if name != "post_nsop":
                    pl_ids = [int(i) for i in pl_ids]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("container_number").filter(
                        id__in=pl_ids
                    )
                )
                pl_po_ids = set()
                for pl in packing_list:
                    pl.shipment_batch_number = shipment

                    pl_master_shipment = await self.get_master_shipment(pl)
                    if pl_master_shipment is None:
                        # 因为一个仓点的主约，每次都是一起更新，所以如果同PO_ID的任意一个没有主约，这个PO_ID下的pl都要更新主约
                        pl_po_ids.add(pl.PO_ID)
                        pl.master_shipment_batch_number = shipment
                    container_number.add(pl.container_number.container_number)

                if pl_po_ids:
                    await sync_to_async(
                        PackingList.objects.filter(PO_ID__in=pl_po_ids).update
                    )(master_shipment_batch_number=shipment)

                await sync_to_async(bulk_update_with_history)(
                    packing_list,
                    PackingList,
                    fields=["shipment_batch_number", "master_shipment_batch_number"],
                )
            except:
                pass

            # 再更新pl绑定的约
            plt_master_po_ids = set()  # 需要改主约的
            plt_shipment_po_ids = set()  # 需要改实际约的

            if name != "post_nsop":
                plt_ids = request.POST.get("plt_ids").strip("][").split(", ")
            else:
                plt_ids = request.POST.get("plt_ids")
            try:  # 如果没有板子，就会报错，不用管
                if name != "post_nsop":
                    plt_ids = [int(i) for i in plt_ids]
                pallet = await sync_to_async(list)(
                    Pallet.objects.select_related("container_number").filter(
                        id__in=plt_ids
                    )
                )
                for p in pallet:
                    p.shipment_batch_number = shipment
                    plt_shipment_po_ids.add(p.PO_ID)
                    p_master_shipment = await self.get_master_shipment(p)
                    if p_master_shipment is None:
                        plt_master_po_ids.add(p.PO_ID)
                        p.master_shipment_batch_number = shipment
                await sync_to_async(bulk_update_with_history)(
                    pallet,
                    Pallet,
                    fields=["shipment_batch_number", "master_shipment_batch_number"],
                )
            except Exception as e:
                print(f"Global error: {str(e)}")
            # 改同一PO_ID的板子的主约
            if plt_master_po_ids:
                await sync_to_async(
                    Pallet.objects.filter(PO_ID__in=plt_master_po_ids).update
                )(master_shipment_batch_number=shipment)

            # 因为pl和plt的PO_ID相同，所以根据PO_ID去找pl
            # 改同一PO_ID的pl的主约
            if plt_master_po_ids:
                await sync_to_async(
                    PackingList.objects.filter(PO_ID__in=plt_master_po_ids).update
                )(master_shipment_batch_number=shipment)
            # 改plt对应的pl的实际约
            if plt_shipment_po_ids:
                await sync_to_async(
                    PackingList.objects.filter(PO_ID__in=plt_shipment_po_ids).update
                )(shipment_batch_number=shipment)

            # except:
            #     pass
            order = await sync_to_async(list)(
                Order.objects.select_related(
                    "retrieval_id", "warehouse", "container_number"
                ).filter(container_number__container_number__in=container_number)
            )
            assigned_warehouse = request.POST.get("origin", "")
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(
                name=assigned_warehouse
            )
            updated_order, updated_retrieval = [], []
            for o in order:
                if not o.warehouse or not o.retrieval_id.retrieval_destination_precise:
                    o.warehouse = warehouse
                    o.retrieval_id.retrieval_destination_precise = assigned_warehouse
                    o.retrieval_id.assigned_by_appt = True
                    updated_order.append(o)
                    updated_retrieval.append(o.retrieval_id)
            await sync_to_async(bulk_update_with_history)(
                updated_order,
                Order,
                fields=["warehouse"],
            )
            await sync_to_async(bulk_update_with_history)(
                updated_retrieval,
                Retrieval,
                fields=["retrieval_destination_precise", "assigned_by_appt"],
            )
            # 如果是客户自提且NJ，那么FleetShipmentPallet表也应该增加记录
            if shipment_type == "客户自提":
                shipment_ava = await sync_to_async(self.sync_query_and_create)(shipment, fleet)
            # 历史FleetShipmentPallet 客户自提费用改为0,后续不调用
            # await self.history_fleet_shipment_pallet()
            mutable_post = request.POST.copy()
            mutable_post["area"] = area
            request.POST = mutable_post
        else:
            print('不是首次')
            batch_number = request.POST.get("batch_number")
            warehouse = request.POST.get("warehouse")
            shipment_appointment = request.POST.get("shipment_appointment")
            tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
            shipment_appointment_utc = self._parse_ts(shipment_appointment, tzinfo)
            note = request.POST.get("note")
            print('batch_number',batch_number)
            shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=batch_number)
            shipment.shipment_appointment = parse(shipment_appointment).replace(
                tzinfo=None
            )
            shipment.shipment_appointment_utc = shipment_appointment_utc
            shipment.note = note
            shipment.is_shipment_schduled = True
            shipment.shipment_schduled_at = current_time
            # LTL的需要存ARM-BOL和ARM-PRO
            shipment.ARM_BOL = (
                request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
            )
            shipment.ARM_PRO = (
                request.POST.get("arm_pro") if request.POST.get("arm_bol") else ""
            )
            shipment.save()
            mutable_post = request.POST.copy()
            mutable_post["area"] = warehouse
            request.POST = mutable_post
        if name == "post_nsop":
            return True
        return await self.handle_warehouse_post(request)

    def sync_query_and_create(self, shipment, fleet):
        list_data = list(
            Pallet.objects.filter(
                shipment_batch_number__shipment_batch_number=shipment.shipment_batch_number
            ).values(
                'PO_ID', 'container_number'
            ).annotate(
                pallet_count=models.Count('id')
            )
        )
        if not list_data:
            list_data = list(
                PackingList.objects.filter(
                    shipment_batch_number__shipment_batch_number=shipment.shipment_batch_number
                ).values(
                    'PO_ID', 'container_number'
                ).annotate(
                    pallet_count=models.Count('id')
                )
            )

        container_identifiers = [item['container_number'] for item in list_data]
        containers = Container.objects.filter(id__in=container_identifiers)
        container_map = {str(container.id): container for container in containers}
        records = []
        for item in list_data:
            container = container_map.get(str(item['container_number']))
            records.append(
                FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number=shipment,
                    PO_ID=item['PO_ID'],
                    total_pallet=item['pallet_count'],
                    container_number=container,
                    expense=0
                )
            )
        if records:
            FleetShipmentPallet.objects.bulk_create(records)

        return list_data

    #历史FleetShipmentPallet 客户自提的费用改为0,后续不调用
    def history_fleet_shipment_pallet(self):
        def sync_update():
            queryset = FleetShipmentPallet.objects.filter(
                shipment_batch_number__shipment_type="客户自提"
            ).select_related(
                "shipment_batch_number", "container_number"
            )
            updated_count = queryset.update(expense=0)
            return updated_count
        return sync_to_async(sync_update)()



    async def get_master_shipment(self, pallet_obj):
        return await sync_to_async(lambda: pallet_obj.master_shipment_batch_number)()

    async def handle_alter_po_shipment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        shipment_batch_number = request.POST.get("shipment_batch_number")
        alter_type = request.POST.get("alter_type")
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=shipment_batch_number)
        if alter_type == "add":
            container_number = set()
            selections = request.POST.getlist("is_shipment_added")
            # 添加PO，更新pl
            try:
                pl_ids = request.POST.getlist("added_pl_ids")
                pl_ids = [id for s, id in zip(selections, pl_ids) if s == "on"]
                pl_ids = [int(i) for id in pl_ids for i in id.split(",") if i]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("container_number").filter(
                        id__in=pl_ids
                    )
                )
                pl_master_po_ids = set()
                for pl in packing_list:
                    pl.shipment_batch_number = shipment
                    pl_master_shipment = await self.get_master_shipment(pl)
                    if pl_master_shipment is None:  # 添加PO时，主约为空就赋值给主约
                        pl_master_po_ids.add(pl.PO_ID)
                        pl.master_shipment_batch_number = shipment
                    shipment.total_weight += pl.total_weight_lbs
                    shipment.total_pcs += pl.pcs
                    shipment.total_cbm += pl.cbm
                    shipment.total_pallet += int(pl.cbm / 2)
                    container_number.add(pl.container_number.container_number)
                await sync_to_async(bulk_update_with_history)(
                    packing_list,
                    PackingList,
                    fields=["shipment_batch_number", "master_shipment_batch_number"],
                )

                if pl_master_po_ids:
                    await sync_to_async(
                        PackingList.objects.filter(PO_ID__in=pl_master_po_ids).update
                    )(master_shipment_batch_number=shipment)
                # 把添加的pl对应的order表等级改为P3
                container_numbers = await sync_to_async(
                    lambda: list(
                        Order.objects.filter(container_number__packinglist__id__in=pl_ids)
                        .values_list("container_number__container_number", flat=True)
                        .distinct()
                    )
                )()
                for cn in container_numbers:
                    await self._update_container_unpacking_priority(cn)
            except:
                pass

            # 添加PO，是直接添加的板子
            try:
                plt_ids = request.POST.getlist("added_plt_ids")
                plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
                plt_ids = [int(i) for id in plt_ids for i in id.split(",") if i]
                pallet = await sync_to_async(list)(
                    Pallet.objects.select_related("container_number").filter(
                        id__in=plt_ids
                    )
                )
                p_master_po_ids = set()  # 需要改主约的
                p_shipment_po_ids = set()  # 需要改实际约的
                for p in pallet:
                    p.shipment_batch_number = shipment
                    p_shipment_po_ids.add(p.PO_ID)

                    p_master_shipment = await self.get_master_shipment(p)
                    if p_master_shipment is None:
                        p_master_po_ids.add(p.PO_ID)
                        p.master_shipment_batch_number = shipment
                await sync_to_async(bulk_update_with_history)(
                    pallet,
                    Pallet,
                    fields=["shipment_batch_number", "master_shipment_batch_number"],
                )
                # 改同一PO_ID的板子的主约
                if p_master_po_ids:
                    await sync_to_async(
                        Pallet.objects.filter(PO_ID__in=p_master_po_ids).update
                    )(master_shipment_batch_number=shipment)

                # 因为pl和板子的PO_ID相同，所以根据PO_ID去找pl
                # 改同一PO_ID的pl的主约
                if p_master_po_ids:
                    await sync_to_async(
                        PackingList.objects.filter(PO_ID__in=p_master_po_ids).update
                    )(master_shipment_batch_number=shipment)
                # 改板子对应的pl的实际约
                if p_shipment_po_ids:
                    await sync_to_async(
                        PackingList.objects.filter(PO_ID__in=p_shipment_po_ids).update
                    )(shipment_batch_number=shipment)
                shipment.total_pallet += len(set([p.pallet_id for p in pallet]))

            except:
                pass
            order = await sync_to_async(list)(
                Order.objects.select_related(
                    "retrieval_id", "warehouse", "container_number"
                ).filter(container_number__container_number__in=container_number)
            )
            assigned_warehouse = shipment.origin
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(
                name=assigned_warehouse
            )
            updated_order, updated_retrieval = [], []
            for o in order:
                if not o.warehouse or not o.retrieval_id.retrieval_destination_precise:
                    o.warehouse = warehouse
                    o.retrieval_id.retrieval_destination_precise = assigned_warehouse
                    o.retrieval_id.assigned_by_appt = True
                    updated_order.append(o)
                    updated_retrieval.append(o.retrieval_id)
            shipment.in_use = True
            await sync_to_async(bulk_update_with_history)(
                updated_order,
                Order,
                fields=["warehouse"],
            )
            await sync_to_async(bulk_update_with_history)(
                updated_retrieval,
                Retrieval,
                fields=["retrieval_destination_precise", "assigned_by_appt"],
            )
        elif alter_type == "remove":  # 删除PO
            selections = request.POST.getlist("is_shipment_removed")
            # 记录要删除PO的PO_ID,因为pl和plt的相同，所以不用区分
            fleet_po_ids = set()
            # 未打板的
            try:
                pl_ids = request.POST.getlist("removed_pl_ids")
                pl_ids = [id for s, id in zip(selections, pl_ids) if s == "on"]
                pl_ids = [int(i) for id in pl_ids for i in id.split(",") if i]
                packing_list = await sync_to_async(list)(
                    PackingList.objects.select_related("container_number").filter(
                        id__in=pl_ids
                    )
                )
                pl_master_po_ids = set()  # 要移除主约的PO_ID
                for pl in packing_list:
                    fleet_po_ids.add(pl.PO_ID)
                    shipment.total_weight -= pl.total_weight_lbs
                    shipment.total_pcs -= pl.pcs
                    shipment.total_cbm -= pl.cbm
                    shipment.total_pallet -= int(pl.cbm / 2)
                    pl_master_shipment = await sync_to_async(
                        lambda: pl.master_shipment_batch_number
                    )()
                    pl_shipment = await sync_to_async(
                        lambda: pl.shipment_batch_number
                    )()
                    if pl_shipment == pl_master_shipment:
                        pl_master_po_ids.add(pl.PO_ID)
                        await sync_to_async(
                            lambda: setattr(pl, "master_shipment_batch_number", None)
                        )()
                    await sync_to_async(
                        lambda: setattr(pl, "shipment_batch_number", None)
                    )()
                await sync_to_async(bulk_update_with_history)(
                    packing_list,
                    PackingList,
                    fields=["shipment_batch_number", "master_shipment_batch_number"],
                )
                if pl_master_po_ids:
                    await sync_to_async(
                        PackingList.objects.filter(PO_ID__in=pl_master_po_ids).update
                    )(master_shipment_batch_number=None)
            except:
                pass
            # 打板的
            plt_ids = request.POST.getlist("removed_plt_ids")
            plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
            plt_ids = [int(i) for id in plt_ids for i in id.split(",") if i]
            pallet = await sync_to_async(list)(
                Pallet.objects.select_related("container_number").filter(id__in=plt_ids)
            )
            plt_master_po_ids = set()  # 需要改主约的
            plt_shipment_po_ids = set()  # 需要改实际约的
            for plt in pallet:
                fleet_po_ids.add(plt.PO_ID)
                plt_shipment_po_ids.add(plt.PO_ID)

                plt_master_shipment = await sync_to_async(
                    lambda: plt.master_shipment_batch_number
                )()
                plt_shipment = await sync_to_async(lambda: plt.shipment_batch_number)()
                if plt_shipment == plt_master_shipment:
                    plt_master_po_ids.add(plt.PO_ID)
                    await sync_to_async(
                        lambda: setattr(plt, "master_shipment_batch_number", None)
                    )()
                await sync_to_async(
                    lambda: setattr(plt, "shipment_batch_number", None)
                )()

            await sync_to_async(bulk_update_with_history)(
                pallet,
                Pallet,
                fields=["shipment_batch_number", "master_shipment_batch_number"],
            )

            # 把fleet_shipment_pallet表里，以上PO_ID相同并且shipment相同的记录都删除
            if fleet_po_ids:
                deleted_count, _ = await sync_to_async(
                    FleetShipmentPallet.objects.filter(
                        PO_ID__in=fleet_po_ids,
                        shipment_batch_number=shipment,
                        fleet_number=shipment.fleet_number,
                    ).delete
                )()
            # 改同一PO_ID的板子的主约
            if plt_master_po_ids:
                await sync_to_async(
                    Pallet.objects.filter(PO_ID__in=plt_master_po_ids).update
                )(master_shipment_batch_number=None)

            # 因为pl和板子的PO_ID相同，所以根据PO_ID去找pl
            # 改同一PO_ID的pl的主约f
            if plt_master_po_ids:
                await sync_to_async(
                    PackingList.objects.filter(PO_ID__in=plt_master_po_ids).update
                )(master_shipment_batch_number=None)
            # 改板子对应的pl的实际约
            if plt_shipment_po_ids:
                await sync_to_async(
                    PackingList.objects.filter(PO_ID__in=plt_shipment_po_ids).update
                )(shipment_batch_number=None)
            shipment.total_pallet -= len(set([p.pallet_id for p in pallet]))
            # except:
            #     pass
            # 删除完PO之后，查找这个约是不是空了，如果空了，就令约的in_use为no
            pls = await sync_to_async(list)(
                PackingList.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            plts = await sync_to_async(list)(
                Pallet.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            if len(pls) == 0 and len(plts) == 0:
                shipment = await sync_to_async(Shipment.objects.get)(
                    shipment_batch_number=shipment_batch_number
                )
                shipment.in_use = False
                shipment.total_weight = 0
                shipment.total_pcs = 0
                shipment.total_cbm = 0
                shipment.total_pallet = 0
        else:
            raise ValueError(f"Unknown shipment alter type: {alter_type}")
        await sync_to_async(shipment.save)()
        mutable_get = request.GET.copy()
        mutable_get["batch_number"] = shipment_batch_number
        mutable_get["area"] = request.POST.get("area")
        request.GET = mutable_get
        return await self.handle_shipment_info_get(request)

    async def handle_cancel_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        appointment_type = request.POST.get("type")
        if appointment_type == "td":  # 撤销预约是这种类型
            shipment_batch_number = request.POST.get("shipment_batch_number")
            shipment = await sync_to_async(Shipment.objects.get)(
                shipment_batch_number=shipment_batch_number
            )
            if shipment.is_shipped:
                if name == "post_nsop":
                    return {'error_messages':f"{shipment}车次已发货，不可取消！"}
                raise RuntimeError(
                    f"Shipment with batch number {shipment} has been shipped!"
                )
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            pallet = await sync_to_async(list)(
                Pallet.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            pl_master_po_ids = set()
            for pl in packing_list:
                pl_master_shipment = await sync_to_async(
                    lambda: pl.master_shipment_batch_number
                )()
                shipment_batch_number = await sync_to_async(
                    lambda: pl.shipment_batch_number
                )()
                if shipment_batch_number == pl_master_shipment:
                    pl_master_po_ids.add(pl.PO_ID)
                    await sync_to_async(
                        lambda: setattr(pl, "master_shipment_batch_number", None)
                    )()
                await sync_to_async(
                    lambda: setattr(pl, "shipment_batch_number", None)
                )()

            if pl_master_po_ids:
                await sync_to_async(
                    PackingList.objects.filter(PO_ID__in=pl_master_po_ids).update
                )(master_shipment_batch_number=None)

            p_master_po_ids = set()
            for p in pallet:
                p_master_shipment = await sync_to_async(
                    lambda: p.master_shipment_batch_number
                )()
                shipment_batch_number = await sync_to_async(
                    lambda: p.shipment_batch_number
                )()
                if shipment_batch_number == p_master_shipment:
                    p_master_po_ids.add(p.PO_ID)
                    await sync_to_async(
                        lambda: setattr(p, "master_shipment_batch_number", None)
                    )()
                await sync_to_async(lambda: setattr(p, "shipment_batch_number", None))()
            if p_master_po_ids:
                await sync_to_async(
                    Pallet.objects.filter(PO_ID__in=p_master_po_ids).update
                )(master_shipment_batch_number=None)

            await sync_to_async(bulk_update_with_history)(
                packing_list,
                PackingList,
                fields=["shipment_batch_number"],
            )
            await sync_to_async(bulk_update_with_history)(
                pallet,
                Pallet,
                fields=["shipment_batch_number"],
            )
            shipment.is_canceled = True
            await sync_to_async(shipment.save)()
        else:
            shipment_batch_number = request.POST.get("batch_number")
            shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
            if shipment.is_shipped:
                raise RuntimeError(
                    f"Shipment with batch number {shipment} has been shipped!"
                )
            shipment.is_shipment_schduled = False
            shipment.shipment_appointment = None
            shipment.shipment_appointment_utc = None
            shipment.note = None
            shipment.shipment_schduled_at = None
            shipment.save()
        warehouse = request.POST.get("warehouse")
        mutable_post = request.POST.copy()
        mutable_post["name"] = warehouse
        request.POST = mutable_post
        if name == "post_nsop":
            return {'success_messages':f"{shipment}批次已发货，不可取消！"}
        return await self.handle_warehouse_post(request)

    async def handle_update_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        batch_number = request.POST.get("batch_number")
        shipment_type = request.POST.get("shipment_type")
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        shipment_appointment = request.POST.get("shipment_appointment")
        if not shipment_appointment:
            shipment_appointment = None
        # 如果这个ISA备约已经登记过了，就把原记录删除
        appointment_id = request.POST.get("appointment_id")
        if (
            shipment.appointment_id != appointment_id
        ):  # 更改ISA的时候，才判断ISA是否已存在，改ISA跟主约没有关系
            if appointment_id:
                any_existing = await sync_to_async(
                    Shipment.objects.filter(appointment_id=appointment_id).exists
                )()
                if any_existing:
                    existing_with_null_batch = await sync_to_async(
                        Shipment.objects.filter(
                            appointment_id=appointment_id,
                            shipment_batch_number__isnull=True,
                        ).first
                    )()
                    if existing_with_null_batch:  # 删除备约的记录
                        await sync_to_async(existing_with_null_batch.delete)()
                    else:  # 如果ISA已经有预约批次，就报错
                        raise ValueError("ISA已预约")
        tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
        if shipment_type == shipment.shipment_type:
            if shipment_type == "FTL":
                shipment.appointment_id = request.POST.get("appointment_id")
                shipment.shipment_account = request.POST.get("shipment_account")
                shipment.origin = request.POST.get("origin")
                shipment.carrier = request.POST.get("carrier")
                shipment.third_party_address = request.POST.get("third_party_address")
                shipment.load_type = request.POST.get("load_type")
                shipment.shipment_schduled_at = timezone.now()
                shipment.shipment_appointment = (
                    shipment_appointment  # 界面的schedule_time
                )
                shipment.shipment_appointment_utc = (
                    self._parse_ts(shipment_appointment, tzinfo)
                    if not shipment_appointment
                    else None
                )
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace(
                    "WALMART", "Walmart"
                )
                shipment.address = request.POST.get("address")
            elif shipment_type != "FTL":
                shipment.appointment_id = request.POST.get("appointment_id", "")
                shipment.shipment_account = request.POST.get("shipment_account", "")
                shipment.origin = request.POST.get("origin")
                shipment.shipment_schduled_at = timezone.now()
                shipment.shipment_appointment = shipment_appointment
                shipment.shipment_appointment_utc = (
                    self._parse_ts(shipment_appointment, tzinfo)
                    if not shipment_appointment
                    else None
                )
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace(
                    "WALMART", "Walmart"
                )
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                # 测试发现甩板的约没有车次
                if fleet:
                    fleet.carrier = request.POST.get("carrier").strip()
                    fleet.appointment_datetime = shipment_appointment
                    await sync_to_async(fleet.save)()
                else:
                    current_time = datetime.now()
                    # 给非FTL排车时，加上pickupNumber
                    wh = request.POST.get("origin", "").split("-")[1]
                    ca = request.POST.get("carrier").strip()
                    dt = datetime.fromisoformat(
                        shipment_appointment.replace("Z", "+00:00")
                    )
                    month_day = dt.strftime("%m%d")
                    pickupNumber = "ZEM" + "-" + wh + "-" + "" + month_day + ca
                    fleet = Fleet(
                        **{
                            "carrier": request.POST.get("carrier").strip(),
                            "fleet_type": shipment_type,
                            "pickup_number": pickupNumber,
                            "appointment_datetime": shipment_appointment,
                            "fleet_number": "FO"
                            + current_time.strftime("%m%d%H%M%S")
                            + str(uuid.uuid4())[:2].upper(),
                            "scheduled_at": current_time,
                            "total_weight": shipment.total_weight,
                            "total_cbm": shipment.total_cbm,
                            "total_pallet": shipment.total_pallet,
                            "total_pcs": shipment.total_pcs,
                            "origin": shipment.origin,
                        }
                    )
                    await sync_to_async(fleet.save)()
                    shipment.fleet_number = fleet
                shipment.ARM_BOL = (
                    request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
                )
                shipment.ARM_PRO = (
                    request.POST.get("arm_pro") if request.POST.get("arm_pro") else ""
                )
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
                shipment.shipment_appointment_utc = self._parse_ts(
                    shipment_appointment, tzinfo
                )
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace(
                    "WALMART", "Walmart"
                )
                shipment.address = request.POST.get("address")
                fleet = shipment.fleet_number
                shipment.fleet_number = None
                shipment.ARM_BOL = None
                shipment.ARM_PRO = None
                if fleet:
                    await sync_to_async(fleet.delete)()
            elif shipment_type != "FTL":
                shipment.shipment_type = shipment_type
                shipment.shipment_account = request.POST.get("shipment_account")
                shipment.origin = request.POST.get("origin")
                shipment.shipment_appointment = shipment_appointment
                shipment.shipment_appointment_utc = self._parse_ts(
                    shipment_appointment, tzinfo
                )
                shipment.note = request.POST.get("note")
                shipment.destination = request.POST.get("destination").replace(
                    "WALMART", "Walmart"
                )
                shipment.address = request.POST.get("address")
                shipment.appointment_id = request.POST.get("appointment_id", "")
                shipment.load_type = ""
                shipment.third_party_address = ""
                shipment.ARM_BOL = (
                    request.POST.get("arm_bol") if request.POST.get("arm_bol") else ""
                )
                shipment.ARM_PRO = (
                    request.POST.get("arm_pro") if request.POST.get("arm_pro") else ""
                )
                current_time = datetime.now()
                # 给非FTL的车，加上pickupNumber
                wh = request.POST.get("origin", "").split("-")[1]
                ca = request.POST.get("carrier").strip()
                dt = datetime.fromisoformat(shipment_appointment.replace("Z", "+00:00"))
                month_day = dt.strftime("%m%d")
                pickupNumber = "ZEM" + "-" + wh + "-" + "" + month_day + ca
                fleet = Fleet(
                    **{
                        "carrier": request.POST.get("carrier").strip(),
                        "fleet_type": shipment_type,
                        "pickup_number": pickupNumber,
                        "appointment_datetime": shipment_appointment,
                        "fleet_number": "FO"
                        + current_time.strftime("%m%d%H%M%S")
                        + str(uuid.uuid4())[:2].upper(),
                        "scheduled_at": current_time,
                        "total_weight": shipment.total_weight,
                        "total_cbm": shipment.total_cbm,
                        "total_pallet": shipment.total_pallet,
                        "total_pcs": shipment.total_pcs,
                        "origin": shipment.origin,
                    }
                )
                if (
                    shipment_type == "客户自提"
                    or shipment_type == "外配"
                ) and "NJ" in str(request.POST.get("origin")):
                    shipment.is_shipped = True
                    shipment.shipped_at = shipment_appointment
                    shipment.shipped_at_utc = self._parse_ts(
                        shipment_appointment, tzinfo
                    )
                    shipment.is_arrived = True
                    shipment.arrived_at = shipment_appointment
                    shipment.arrived_at_utc = self._parse_ts(
                        shipment_appointment, tzinfo
                    )
                    fleet.departured_at = shipment_appointment
                    fleet.arrived_at = shipment_appointment
                if shipment_type == "快递":  # UPS的比客户自提的，系统上还少一步POD上传
                    shipment.express_number = (
                        request.POST.get("express_number")
                        if request.POST.get("express_number")
                        else ""
                    )
                    shipment.is_shipped = True
                    shipment.shipped_at = shipment_appointment
                    shipment.shipped_at_utc = self._parse_ts(
                        shipment_appointment, tzinfo
                    )
                    shipment.is_arrived = True
                    shipment.arrived_at = shipment_appointment
                    shipment.arrived_at_utc = self._parse_ts(
                        shipment_appointment, tzinfo
                    )
                    fleet.departured_at = shipment_appointment
                    fleet.arrived_at = shipment_appointment
                    shipment.pod_link = "Without"
                    shipment.pod_uploaded_at = timezone.now()

                if shipment_type == "外配":  # UPS的比客户自提的，系统上还少一步POD上传
                    shipment.express_number = (
                        request.POST.get("express_number")
                        if request.POST.get("express_number")
                        else ""
                    )

                await sync_to_async(fleet.save)()
                if shipment.fleet_number:
                    await sync_to_async(shipment.fleet_number.delete)()
                shipment.fleet_number = fleet
        shipment.status = ""
        await sync_to_async(shipment.save)()
        mutable_get = request.GET.copy()
        mutable_get["warehouse"] = request.POST.get("warehouse")
        mutable_get["batch_number"] = batch_number
        request.GET = mutable_get
        return await self.handle_shipment_info_get(request)

    async def handle_appointment_warehouse_search_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number",
                "container_number__order",
                "container_number__order__retrieval_id",
            )
            .filter(
                location=warehouse,
                container_number__order__created_at__gte="2024-09-01",
                shipment_batch_number__isnull=True,
            )
            .values(
                "destination",
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                total_cbm=Sum("cbm", output_field=IntegerField()),
                total_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-total_pallet")
        )
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related(
                "container_number",
                "container_number__order__retrieval_id",
                "container_number__order__vessel_id",
            )
            .filter(
                (
                    models.Q(
                        container_number__order__retrieval_id__retrieval_destination_precise=warehouse
                    )
                    | models.Q(container_number__order__warehouse__name=warehouse)
                ),
                container_number__order__created_at__gte="2024-09-01",
                container_number__order__vessel_id__vessel_eta__gte=start_date,
                container_number__order__vessel_id__vessel_eta__lte=end_date,
                shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
            )
            .values(
                "destination",
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                total_cbm=Sum("cbm", output_field=IntegerField()),
                total_pallet=Sum("cbm", output_field=FloatField()) / 2,
            )
            .order_by("-total_pallet")
        )
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
        appointment_data = await sync_to_async(list)(
            Shipment.objects.filter(
                (
                    models.Q(origin__isnull=True)
                    | models.Q(origin="")
                    | models.Q(origin=warehouse)
                ),
                models.Q(in_use=False, is_canceled=False),
                shipment_appointment__gt=datetime.now(),
            )
            .values("destination")
            .annotate(n_appointment=Count("appointment_id", distinct=True))
        )
        df_pallet = pd.DataFrame(pallet)
        df_packing_list = pd.DataFrame(packing_list)
        df_appointment = pd.DataFrame(appointment_data)
        try:
            df = pd.merge(
                df_pallet, df_packing_list, how="outer", on=["destination", "warehouse"]
            ).fillna(0)
            df = df.merge(df_appointment, how="left", on=["destination"]).fillna(0)
            df["total_cbm"] = df["total_cbm_x"] + df["total_cbm_y"]
            df["total_pallet"] = df["total_pallet_x"] + df["total_pallet_y"]
            df = df.drop(
                ["total_cbm_x", "total_cbm_y", "total_pallet_x", "total_pallet_y"],
                axis=1,
            )
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
            "account_options": self.account_options,
        }
        return self.template_appointment_management, context

    async def handle_create_empty_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointment_id = request.POST.get("appointment_id").strip()
        appointment = await sync_to_async(list)(
            Shipment.objects.filter(appointment_id=appointment_id)
        )
        if appointment:
            raise RuntimeError(f"Appointment {appointment_id} already exist!")
        await sync_to_async(Shipment.objects.create)(
            **{
                "appointment_id": appointment_id,
                "destination": request.POST.get("destination").upper(),
                "shipment_appointment": request.POST.get("shipment_appointment"),
                "load_type": request.POST.get("load_type"),
                "origin": request.POST.get("origin", None),
                "shipment_account": request.POST.get("shipment_account", None),
                "in_use": False,
            }
        )
        warehouse = request.POST.get("warehouse", "")
        if warehouse:
            return await self.handle_appointment_warehouse_search_post(request)
        else:
            return await self.handle_appointment_management_get(request)

    async def handle_download_empty_appointment_template_post(self) -> HttpResponse:
        file_path = (
            Path(__file__)
            .parent.parent.parent.parent.resolve()
            .joinpath("templates/export_file/appointment_template.xlsx")
        )
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = (
                f'attachment; filename="appointment_template.xlsx"'
            )
            return response

    async def handle_upload_and_create_empty_appointment_post(
            self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            df["warehouse"] = df["warehouse"].astype(str)
            # 遍历每行数据进行校验
            for idx, row in df.iterrows():
                c_id = row["c_id"]
                destination = row["destination"]
                scheduled_time = row["scheduled_time"]
                warehouse = row["warehouse"]
                load_type = row["load_type"]
                shipment_account = row["shipment_account"]
                row_num = idx + 2  # Excel 行号从 1 开始，表头占 1 行

                # 1. 校验不为空
                if pd.isna(c_id) or str(c_id).strip() == "":
                    raise ValueError(f"第 {row_num} 行的预约号（c_id）不能为空")
                if pd.isna(destination) or str(destination).strip() == "":
                    raise ValueError(f"第 {row_num} 行的目的地（destination）不能为空")
                if pd.isna(scheduled_time) or str(scheduled_time).strip() == "":
                    raise ValueError(f"第 {row_num} 行的（scheduled_time）不能为空")
                if pd.isna(warehouse) or str(warehouse).strip() == "":
                    raise ValueError(f"第 {row_num} 行的发货仓库（warehouse）不能为空")
                if pd.isna(load_type) or str(load_type).strip() == "":
                    raise ValueError(f"第 {row_num} 行的装车类型（load_type）不能为空")
                if pd.isna(shipment_account) or str(shipment_account).strip() == "":
                    raise ValueError(f"第 {row_num} 行的预约账号（shipment_account）不能为空")
                # 2. 转换为字符串并去除两端空格
                c_id_str = str(c_id).strip()

                # 3. 校验仅为数字（不包含任何标点符号或字母）
                if not c_id_str.isdigit():
                    raise ValueError(
                        f"第 {row_num} 行的预约号（c_id）'{c_id_str}' 无效，"
                        "仅允许数字，不允许包含标点符号、字母或其他特殊字符"
                    )

            # 校验通过后继续处理数据
            data = df.to_dict("records")
            c_id = [str(d["c_id"]).strip() for d in data]

            # 校验重复
            if len(c_id) != len(set(c_id)):
                raise RuntimeError("预约号（c_id）存在重复值！")

            existed_shipments = await sync_to_async(list)(
                Shipment.objects.filter(appointment_id__in=c_id)
            )
            if existed_shipments:
                existing_ids = [ship.appointment_id for ship in existed_shipments]
                raise ValueError(f"以下预约号已存在：{existing_ids}")

            cleaned_data = [
                {
                    "appointment_id": str(d["c_id"]).strip(),
                    "destination": d["destination"].upper().strip(),
                    "shipment_appointment": d["scheduled_time"],
                    "origin": (
                        d["warehouse"].upper().strip()
                        if d["warehouse"] != "nan"
                        else None
                    ),
                    "in_use": False,
                    "load_type": d["load_type"].strip(),
                    "shipment_account": d["shipment_account"].strip(),
                }
                for d in data
            ]

            instances = [Shipment(**d) for d in cleaned_data]
            await sync_to_async(bulk_create_with_history)(instances, Shipment)

        return await self.handle_appointment_warehouse_search_post(request)

    async def handle_shipment_list_search_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        appointmnet_start_date = request.POST.get("start_date")
        appointment_end_date = request.POST.get("end_date")
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(
                origin=warehouse,
                shipment_appointment__gte=appointmnet_start_date,
                shipment_appointment__lte=appointment_end_date,
                in_use=True,
            )
            .order_by("shipment_appointment")
        )
        context = {
            "warehouse": warehouse,
            "start_date": appointmnet_start_date,
            "end_date": appointment_end_date,
            "warehouse_options": self.warehouse_options,
            "shipment": shipment,
        }
        return self.template_shipment_list, context

    async def handle_fix_shipment_exceptions_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        solution = request.POST.get("solution")
        shipment_batch_number = request.POST.get("shipment_batch_number")
        if solution == "keep_old":
            shipment = await sync_to_async(Shipment.objects.get)(
                shipment_batch_number=shipment_batch_number
            )
            shipment.status = ""
            shipment.priority = "P0"
            shipment.is_shipped = False
            shipment.shipped_at = None
            await sync_to_async(shipment.save)()
        else:  # 换约是这个选项
            old_shipment = await sync_to_async(Shipment.objects.get)(
                shipment_batch_number=shipment_batch_number
            )
            shipment_type = request.POST.get("shipment_type")
            appointment_id = request.POST.get("appointment_id", None)
            appointment_id = appointment_id.strip() if appointment_id else None
            try:
                existed_appointment = await sync_to_async(Shipment.objects.get)(
                    appointment_id=appointment_id
                )
            except:
                existed_appointment = None
            if existed_appointment:
                if existed_appointment.in_use:
                    raise RuntimeError(
                        f"Appointment {existed_appointment} already used by other shipment!"
                    )
                elif existed_appointment.is_canceled:
                    raise RuntimeError(
                        f"Appointment {existed_appointment} already exists and is canceled!"
                    )
                elif (
                    existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC)
                    < timezone.now()
                ):
                    raise RuntimeError(
                        f"Appointment {existed_appointment} already exists and expired!"
                    )
                elif existed_appointment.destination != request.POST.get(
                    "destination", None
                ):
                    raise ValueError(
                        f"Appointment {existed_appointment} has a different destination {existed_appointment.destination} - {request.POST.get('destination', None)}!"
                    )
                else:
                    shipment = existed_appointment
                    shipment.shipment_batch_number = (
                        request.POST.get("shipment_batch_number").strip().split("_")[0]
                        + f"_{shipment.batch + 1}"
                    )
                    shipment.in_use = True
                    shipment.origin = request.POST.get("origin", "").strip()
                    shipment.shipment_type = shipment_type
                    shipment.load_type = request.POST.get("load_type", "").strip()
                    shipment.note = request.POST.get("note", "").strip()
                    shipment.shipment_schduled_at = timezone.now()
                    shipment.is_shipment_schduled = True
                    shipment.destination = (
                        request.POST.get("destination", "")
                        .replace("WALMART", "Walmart")
                        .strip()
                    )
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
                        shipment_appointment = parse(shipmentappointment).replace(
                            tzinfo=None
                        )
                        fleet = Fleet(
                            **{
                                "carrier": request.POST.get("carrier").strip(),
                                "fleet_type": shipment_type,
                                "appointment_datetime": shipment_appointment,  # 车次的提货时间=约的提货时间
                                "fleet_number": "FO"
                                + current_time.strftime("%m%d%H%M%S")
                                + str(uuid.uuid4())[:2].upper(),
                                "scheduled_at": current_time,
                                "total_weight": shipment_data["total_weight"],
                                "total_cbm": shipment_data["total_cbm"],
                                "total_pallet": shipment_data["total_pallet"],
                                "total_pcs": shipment_data["total_pcs"],
                                "origin": shipment_data["origin"],
                            }
                        )
                        await sync_to_async(fleet.save)()
                        shipment.fleet_number = fleet
                        # LTL的需要存ARM-BOL和ARM-PRO
                        if shipment_type in ["LTL", "外配", "快递"]:
                            shipment.ARM_BOL = (
                                request.POST.get("arm_bol")
                                if request.POST.get("arm_bol")
                                else ""
                            )
                            shipment.ARM_PRO = (
                                request.POST.get("arm_pro")
                                if request.POST.get("arm_bol")
                                else ""
                            )
                    try:
                        shipment.third_party_address = request.POST.get(
                            "third_party_address"
                        ).strip()
                    except:
                        pass
            else:
                current_time = timezone.now()
                tzinfo = self._parse_tzinfo(request.POST.get("origin", ""))
                shipmentappointment = request.POST.get("shipment_appointment")
                shipment_appointment = parse(shipmentappointment).replace(tzinfo=None)
                shipment_data = {}
                shipment_data["appointment_id"] = request.POST.get(
                    "appointment_id", ""
                ).strip()
                shipment_data["third_party_address"] = request.POST.get(
                    "third_party_address", ""
                ).strip()
                shipment_data["shipment_type"] = shipment_type
                shipment_data["load_type"] = request.POST.get("load_type", "").strip()
                shipment_data["shipment_account"] = request.POST.get(
                    "shipment_account", ""
                ).strip()
                shipment_data["note"] = request.POST.get("note", "").strip()
                if shipment_type == "外配":
                    shipment_data["shipment_appointment"] = request.POST.get(
                        "shipment_est_arrival", None
                    )
                elif shipment_type == "快递":
                    shipment_data["shipment_appointment"] = request.POST.get(
                        "shipment_est_arrival", None
                    )
                else:
                    shipment_data["shipment_appointment"] = request.POST.get(
                        "shipment_appointment", None
                    )
                if shipment_data["shipment_appointment"]:
                    shipment_data["shipment_appointment_utc"] = self._parse_ts(
                        shipment_data["shipment_appointment"], tzinfo
                    )
                else:
                    shipment_data["shipment_appointment_utc"] = None
                shipment_data["shipment_schduled_at"] = current_time
                shipment_data["is_shipment_schduled"] = True
                shipment_data["destination"] = request.POST.get(
                    "destination", ""
                ).strip()
                shipment_data["address"] = request.POST.get("address", "").strip()
                shipment_data["origin"] = request.POST.get("origin", "").strip()
                shipment_data["master_batch_number"] = (
                    old_shipment.shipment_batch_number
                )
                shipment_data["shipment_batch_number"] = (
                    request.POST.get("shipment_batch_number").strip().split("_")[0]
                    + f"_{old_shipment.batch + 1}"
                )
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
                if shipment_type in ["LTL", "外配", "快递"]:
                    shipment_data["ARM_BOL"] = request.POST.get("arm_bol", "").strip()
                    shipment_data["ARM_PRO"] = request.POST.get("arm_pro", "").strip()
                    fleet = Fleet(
                        **{
                            "carrier": request.POST.get("carrier").strip(),
                            "fleet_type": shipment_type,
                            "appointment_datetime": shipment_appointment,
                            "fleet_number": "FO"
                            + current_time.strftime("%m%d%H%M%S")
                            + str(uuid.uuid4())[:2].upper(),
                            "scheduled_at": current_time,
                            "total_weight": old_shipment.shipped_weight,
                            "total_cbm": old_shipment.shipped_cbm,
                            "total_pallet": old_shipment.shipped_pallet,
                            "total_pcs": old_shipment.shipped_pcs,
                            "origin": request.POST.get("origin", "").strip(),
                        }
                    )
                    await sync_to_async(fleet.save)()
                    shipment_data["fleet_number"] = fleet
            if not existed_appointment:
                shipment = Shipment(**shipment_data)
            await sync_to_async(shipment.save)()
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            pallet = await sync_to_async(list)(
                Pallet.objects.select_related("shipment_batch_number").filter(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            )
            # 把旧约改成新约
            pl_master_po_ids = set()  # 需要改主约的pl
            for pl in packing_list:
                pl_master_shipment = await sync_to_async(
                    lambda: pl.master_shipment_batch_number
                )()
                pl_shipment = await sync_to_async(lambda: pl.shipment_batch_number)()
                if pl_master_shipment == pl_shipment:
                    pl_master_po_ids.add(pl.PO_ID)
                    await sync_to_async(
                        lambda: setattr(pl, "master_shipment_batch_number", shipment)
                    )()

                await sync_to_async(
                    lambda: setattr(pl, "shipment_batch_number", shipment)
                )()

            packing_lists_to_update = []
            if pl_master_po_ids:
                master_pls = await sync_to_async(list)(
                    PackingList.objects.filter(PO_ID__in=pl_master_po_ids)
                )
                for plm in master_pls:
                    plm.master_shipment_batch_number = shipment
                    packing_lists_to_update.append(plm)
            await sync_to_async(bulk_update_with_history)(
                packing_lists_to_update,
                PackingList,
                fields=["master_shipment_batch_number"],
            )

            plt_master_po_ids = set()  # 需要改主约的plt
            for plt in pallet:
                plt_master_shipment = await sync_to_async(
                    lambda: plt.master_shipment_batch_number
                )()
                plt_shipment = await sync_to_async(lambda: plt.shipment_batch_number)()
                if plt_master_shipment == plt_shipment:
                    plt_master_po_ids.add(plt.PO_ID)
                    await sync_to_async(
                        lambda: setattr(plt, "master_shipment_batch_number", shipment)
                    )()

                await sync_to_async(
                    lambda: setattr(plt, "shipment_batch_number", shipment)
                )()

            pallet_lists_to_update = []
            if plt_master_po_ids:
                master_plts = await sync_to_async(list)(
                    Pallet.objects.filter(PO_ID__in=plt_master_po_ids)
                )
                for pltm in master_plts:
                    pltm.master_shipment_batch_number = shipment
                    pallet_lists_to_update.append(pltm)
            await sync_to_async(bulk_update_with_history)(
                pallet_lists_to_update,
                Pallet,
                fields=["master_shipment_batch_number"],
            )
            await sync_to_async(bulk_update_with_history)(
                packing_list,
                PackingList,
                fields=["shipment_batch_number", "master_shipment_batch_number"],
            )
            await sync_to_async(bulk_update_with_history)(
                pallet,
                Pallet,
                fields=["shipment_batch_number", "master_shipment_batch_number"],
            )
            old_shipment.is_canceled = True
            await sync_to_async(old_shipment.save)()
        return await self.handle_shipment_exceptions_get(request)

    async def handle_cancel_abnormal_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        shipment_batch_number = request.POST.get("batch_number")
        shipment = await sync_to_async(Shipment.objects.get)(
            shipment_batch_number=shipment_batch_number
        )
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related("shipment_batch_number").filter(
                shipment_batch_number__shipment_batch_number=shipment_batch_number
            )
        )

        pl_master_po_ids = set()  # 需要更改主约的pl的PO_ID
        for pl in packing_list:
            pl_master_shipment = await sync_to_async(
                lambda: pl.master_shipment_batch_number
            )()
            pl_shipment = await sync_to_async(lambda: pl.shipment_batch_number)()
            if pl_shipment == pl_master_shipment:
                pl_master_po_ids.add(pl.PO_ID)
                await sync_to_async(
                    lambda: setattr(pl, "master_shipment_batch_number", None)
                )()

            await sync_to_async(lambda: setattr(pl, "shipment_batch_number", None))()

        packing_lists_to_update = []
        if pl_master_po_ids:
            master_pls = await sync_to_async(list)(
                PackingList.objects.filter(PO_ID__in=pl_master_po_ids)
            )
            for plm in master_pls:
                plm.master_shipment_batch_number = None
                packing_lists_to_update.append(plm)
        await sync_to_async(bulk_update_with_history)(
            packing_lists_to_update,
            PackingList,
            fields=["master_shipment_batch_number"],
        )

        pallet = await sync_to_async(list)(
            Pallet.objects.select_related("shipment_batch_number").filter(
                shipment_batch_number__shipment_batch_number=shipment_batch_number
            )
        )
        plt_master_po_ids = set()  # 需要更改主约的plt的PO_ID
        for plt in pallet:
            plt_master_shipment = await sync_to_async(
                lambda: plt.master_shipment_batch_number
            )()
            plt_shipment = await sync_to_async(lambda: plt.shipment_batch_number)()
            if plt_master_shipment == plt_shipment:
                plt_master_po_ids.add(plt.PO_ID)
                await sync_to_async(
                    lambda: setattr(plt, "master_shipment_batch_number", None)
                )()

            await sync_to_async(lambda: setattr(plt, "shipment_batch_number", None))()

        pallet_lists_to_update = []
        if plt_master_po_ids:
            master_plts = await sync_to_async(list)(
                Pallet.objects.filter(PO_ID__in=plt_master_po_ids)
            )
            for pltm in master_plts:
                pltm.master_shipment_batch_number = None
                pallet_lists_to_update.append(pltm)
        await sync_to_async(bulk_update_with_history)(
            pallet_lists_to_update,
            Pallet,
            fields=["master_shipment_batch_number"],
        )

        await sync_to_async(bulk_update_with_history)(
            packing_list,
            PackingList,
            fields=["shipment_batch_number", "master_shipment_batch_number"],
        )
        await sync_to_async(bulk_update_with_history)(
            pallet,
            Pallet,
            fields=["shipment_batch_number", "master_shipment_batch_number"],
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
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number" "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "container_number__order__retrieval_id",
                    "container_number__order__vessel_id",
                )
                .filter(plt_criteria)
                .annotate(
                    schedule_status=Case(
                        When(
                            Q(
                                container_number__order__offload_id__offload_at__lte=datetime.now().date()
                                + timedelta(days=-7)
                            ),
                            then=Value("past_due"),
                        ),
                        default=Value("on_time"),
                        output_field=CharField(),
                    ),
                    str_id=Cast("id", CharField()),
                    str_length=Cast("length", CharField()),
                    str_width=Cast("width", CharField()),
                    str_height=Cast("height", CharField()),
                    str_pcs=Cast("pcs", CharField()),
                )
                .values(
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "destination",
                    "address",
                    "delivery_method",
                    "container_number__order__offload_id__offload_at",
                    "schedule_status",
                    "abnormal_palletization",
                    "po_expired",
                    "container_number__order__vessel_id__vessel_eta",
                    "sequence_number",
                    "PO_ID",
                    "note",
                    target_retrieval_timestamp=F(
                        "container_number__order__retrieval_id__target_retrieval_timestamp"
                    ),
                    target_retrieval_timestamp_lower=F(
                        "container_number__order__retrieval_id__target_retrieval_timestamp_lower"
                    ),
                    temp_t49_pickup=F(
                        "container_number__order__retrieval_id__temp_t49_available_for_pickup"
                    ),
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                )
                .annotate(
                    eta=F("container_number__order__vessel_id__vessel_eta"),
                    custom_delivery_method=F("delivery_method"),
                    fba_ids=F("fba_id"),
                    ref_ids=F("ref_id"),
                    shipping_marks=F("shipping_mark"),
                    plt_ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                    length=StringAgg(
                        "str_length", delimiter=",", ordering="str_length"
                    ),
                    width=StringAgg("str_width", delimiter=",", ordering="str_width"),
                    height=StringAgg(
                        "str_height", delimiter=",", ordering="str_height"
                    ),
                    n_pcs=StringAgg("str_pcs", delimiter=",", ordering="str_pcs"),
                )
                .order_by("container_number__order__offload_id__offload_at")
                .order_by("sequence_number")
            )
            data += pal_list
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number" "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "pallet",
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
                    schedule_status=Case(
                        When(
                            Q(
                                container_number__order__offload_id__offload_at__lte=datetime.now().date()
                                + timedelta(days=-7)
                            ),
                            then=Value("past_due"),
                        ),
                        default=Value("on_time"),
                        output_field=CharField(),
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                )
                .values(
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "destination",
                    "address",
                    "custom_delivery_method",
                    "container_number__order__offload_id__offload_at",
                    "schedule_status",
                    "container_number__order__vessel_id__vessel_eta",
                    "PO_ID",
                    "note",
                    target_retrieval_timestamp=F(
                        "container_number__order__retrieval_id__target_retrieval_timestamp"
                    ),
                    target_retrieval_timestamp_lower=F(
                        "container_number__order__retrieval_id__target_retrieval_timestamp_lower"
                    ),
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                    temp_t49_pickup=F(
                        "container_number__order__retrieval_id__temp_t49_available_for_pickup"
                    ),
                )
                .annotate(
                    eta=F("container_number__order__vessel_id__vessel_eta"),
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
                    total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
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
            )
            data += pl_list
        return data

    async def _get_packing_list_non_agg(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> tuple[list[PackingList], list[Pallet]]:
        if pl_criteria:
            pal_list = await sync_to_async(list)(
                PackingList.objects.filter(pl_criteria)
            )
        else:
            pal_list = []
        if plt_criteria:
            plt_list = await sync_to_async(list)(Pallet.objects.filter(plt_criteria))
        else:
            plt_list = []
        return pal_list, plt_list

    async def _get_sharepoint_auth(self) -> ClientContext:
        ctx = ClientContext(SP_URL).with_client_certificate(
            SP_TENANT,
            SP_CLIENT_ID,
            SP_THUMBPRINT,
            private_key=SP_PRIVATE_KEY,
            scopes=[SP_SCOPE],
        )
        return ctx

    async def _shipment_exist(self, batch_number: str) -> bool:
        if await sync_to_async(list)(
            Shipment.objects.filter(shipment_batch_number=batch_number)
        ):
            return True
        else:
            return False

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    def _verify_empty_string(sefl, string_value: str) -> bool:
        if string_value is None:
            return True
        if isinstance(string_value, float) and math.isnan(string_value):
            return True
        if isinstance(string_value, str) and string_value.strip() == "":
            return True
        return False

    def _parse_datetime(self, datetime_string: str) -> tuple[str, str]:
        datetime_pattern = re.compile(
            r"""
            (?P<month>\d{1,2})          # Month (1 or 2 digits)
            [/\-]                       # Separator (/, -)
            (?P<day>\d{1,2})            # Day (1 or 2 digits)
            [/\-]                       # Separator (/, -)
            (?P<year>\d{4})             # Year (4 digits)
            [, ]*                       # Optional comma or space
            (?P<hour>\d{1,2})           # Hour (1 or 2 digits)
            [:]                         # Colon
            (?P<minute>\d{2})           # Minute (2 digits)
            [ ]*                        # Optional space
            (?P<period>AM|PM)           # AM or PM
            [ ,]*                       # Optional comma or space
            (?P<timezone>[A-Z+\-:\d]*)? # Optional timezone
            """,
            re.VERBOSE | re.IGNORECASE,
        )
        match = datetime_pattern.search(datetime_string)
        if not match:
            raise ValueError(f"Invalid datetime format: {datetime_string}")

        parts = match.groupdict()
        month, day, year = int(parts["month"]), int(parts["day"]), int(parts["year"])
        hour, minute = int(parts["hour"]), int(parts["minute"])
        period = parts["period"].upper()
        timezone = parts["timezone"].strip() if parts["timezone"] else ""

        if period == "PM" and hour != 12:
            hour += 12
        elif period == "AM" and hour == 12:
            hour = 0

        formatted_datetime = datetime(year, month, day, hour, minute).strftime(
            "%Y-%m-%d %H:%M"
        )
        return formatted_datetime, timezone

    def _parse_tzinfo(self, s: str) -> str:
        if "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: str, tzinfo: str) -> str:
        if ts:
            if isinstance(ts, str):
                ts_naive = datetime.fromisoformat(ts)
            else:
                ts_naive = ts.replace(tzinfo=None)
            tz = pytz.timezone(tzinfo)
            ts = tz.localize(ts_naive).astimezone(timezone.utc)
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
