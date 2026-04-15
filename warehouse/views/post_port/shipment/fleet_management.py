import ast
import base64
import copy
import csv
import io
import json
import os
import platform
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Tuple
import os
import logging
from django.core.exceptions import ObjectDoesNotExist
from numpy.core.records import record
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import barcode
import chardet
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import openpyxl
import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from barcode.writer import ImageWriter
from django.contrib.postgres.aggregates import StringAgg
from django.db import models, transaction
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Q,
    Sum,
    Value,
    When, Prefetch, Max,
)
from django.db.models.functions import Cast, Round
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.template.loader import get_template
from django.utils import timezone
from django.views import View
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from PIL import Image
from PyPDF2 import PdfMerger, PdfReader
from simple_history.utils import bulk_update_with_history
from xhtml2pdf import pisa

from warehouse.forms.upload_file import UploadFileForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.fleet import Fleet
from warehouse.models.fleet_dose_not import FleetDoseNot
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoicev2 import Invoicev2
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    APP_ENV,
    DELIVERY_METHOD_OPTIONS,
    SP_CLIENT_ID,
    SP_DOC_LIB,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    SYSTEM_FOLDER,
)
from warehouse.views.post_port.shipment.shipping_management import ShippingManagement
from warehouse.views.export_file import link_callback

matplotlib.use("Agg")
matplotlib.rcParams["font.size"] = 100
matplotlib.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
matplotlib.rcParams["axes.unicode_minus"] = False
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei"]


class FleetManagement(View):
    template_fleet = "post_port/shipment/03_fleet_main.html"
    template_fleet_schedule = "post_port/shipment/03_1_fleet_schedule.html"
    template_fleet_schedule_info = "post_port/shipment/03_2_fleet_schedule_info.html"
    template_outbound = "post_port/shipment/04_outbound_main.html"
    template_outbound_departure = (
        "post_port/shipment/04_outbound_depature_confirmation.html"
    )
    template_delivery_and_pod = "post_port/shipment/05_1_delivery_and_pod.html"
    template_pod_upload = "post_port/shipment/05_2_delivery_and_pod.html"
    template_fleet_cost_record = "post_port/shipment/fleet_cost_record.html"
    template_fleet_cost_record_ltl = "post_port/shipment/fleet_cost_record_ltl.html"
    template_bol = "export_file/bol_base_template.html"
    template_bol_pickup = "export_file/bol_template.html"
    template_bol_pickup_alone = "export_file/bol_template_alone.html"
    template_la_bol_pickup = "export_file/LA_bol_template.html"
    template_la_bol_pickup_alone = "export_file/LA_bol_template_alone.html"
    template_ltl_label = "export_file/ltl_label.html"
    template_ltl_bol = "export_file/ltl_bol.html"
    template_abnormal_fleet_warehouse_search = (
        "post_port/shipment/abnormal/01_fleet_management_main.html"
    )
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "LA-91748": "LA-91748",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
        "LA-91789": "LA-91789",
        "LA-91766": "LA-91766",
    }
    shipment_type_options = {
        "": "",
        "FTL/LTL": "FTL/LTL",
        "外配": "外配",
        "快递": "快递",
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
        "大森林": "大森林",
        "Arm-AMF": "Arm-AMF",
        "Zem-AMF": "Zem-AMF",
        "ASH": "ASH",
        "Arm": "Arm",
        "ZEM": "ZEM",
        "LiFeng": "LiFeng",
        "FWT": "FWT",
        "SunZong": "SunZong",
    }

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
        elif step == "fleet_cost_record":
            template, context = await self.handle_fleet_cost_record_get(request)
            return render(request, template, context)
        elif step == "fleet_cost_record_ltl":
            template, context = await self.handle_fleet_cost_record_get_ltl(request)
            return render(request, template, context)
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_fleet_warehouse_search, context)

    async def post(self, request: HttpRequest) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        print('step',step)
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
            template, context = await self.handle_outbound_warehouse_search_post(
                request
            )
            return render(request, template, context)
        elif step == "export_packing_list":
            return await self.handle_export_packing_list_post(request)
        elif step == "export_bol":
            return await self.handle_export_bol_post(request)
        elif step == "update_fleet_note":
            template, context = await self.handle_update_fleet_note(request)
            return render(request, template, context)
        elif step == "add_pallet":
            template, context = await self.handle_add_pallet(request)
            return render(request, template, context)
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
        elif step == "upload_SELF_BOL":
            return await self.handle_bol_upload_post(request)
        elif step == "download_LABEL":
            return await self._export_ltl_label(request)
        elif step == "download_LTL_BOL":
            return await self._export_ltl_bol(request)
        elif step == "fleet_cost_record":
            template, context = await self.handle_fleet_cost_record_get(request)
            return render(request, template, context)
        elif step == "fleet_cost_record_ltl":
            template, context = await self.handle_fleet_cost_record_get_ltl(request)
            return render(request, template, context)
        elif step == "bind_multi_unload":
            template, context = await self.handle_bind_multi_unload(request)
            return render(request, template, context)
        elif step == "unbind_multi_unload":
            template, context = await self.handle_unbind_multi_unload(request)
            return render(request, template, context)

        elif step == "batch_allocate_ltl_cost":
            template, context = await self.handle_batch_allocate_ltl_cost(request)
            return render(request, template, context)
        elif step == "fleet_cost_confirm":
            template, context = await self.handle_fleet_cost_confirm_get(request)
            return render(request, template, context)
        elif step == "fleet_cost_confirm_ltl":
            template, context = await self.handle_fleet_cost_confirm_get_ltl(request)
            return render(request, template, context)
        elif step == "fleet_cost_confirm_back":
            template, context = await self.handle_fleet_cost_confirm_get_back(request)
            return render(request, template, context)
        elif step == "fleet_cost_confirm_back_ltl":
            template, context = await self.handle_fleet_cost_confirm_get_back_ltl(request)
            return render(request, template, context)
        elif step == "upload_fleet_cost":
            template, context = await self.handle_upload_fleet_cost_get(request)
            return render(request, template, context)
        elif step == "download_fleet_cost_template":
            return await self.handle_fleet_cost_export(request)
        elif step == "download_ltl_cost_template":
            return await self.handle_download_ltl_template(request)
        elif step == "download_recorded_fleet_cost":
            return await self.handle_download_recorded_fleet_cost(request)
        elif step == "download_ltl_table":
            return await self.handle_download_ltl_table(request)
        elif step == "upload_ltl_cost":
            template, context = await self.handle_upload_ltl_cost(request)
            return render(request, template, context)
        elif step == "update_fleet_verify_status":
            template, context = await self.handle_update_fleet_verify_status(request)
            return render(request, template, context)
        elif step == "rollback_fleet_status":
            template, context = await self.handle_rollback_fleet_status(request)
            return render(request, template, context)
        elif step == "batch_confirm_verify":
            template, context = await self.handle_batch_confirm_verify(request)
            return render(request, template, context)
        elif step == "batch_cancel_verify":
            template, context = await self.handle_batch_cancel_verify(request)
            return render(request, template, context)
        elif step == "batch_confirm_ltl_price":
            template, context = await self.handle_batch_confirm_ltl_price(request)
            return render(request, template, context)
        elif step == "batch_confirm_ltl_note":
            template, context = await self.handle_batch_confirm_ltl_note(request)
            return render(request, template, context)
        elif step == "batch_confirm_ltl_supplier":
            template, context = await self.handle_batch_confirm_ltl_supplier(request)
            return render(request, template, context)
        elif step == "update_fleet_supplier":
            template, context = await self.handle_update_fleet_supplier(request)
            return render(request, template, context)

        else:
            return await self.get(request)

    async def handle_add_pallet(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selections = request.POST.getlist("is_plt_added")
        on_positions = [i for i, v in enumerate(selections) if v == "on"]
        pallet_add = request.POST.getlist("pallet_add")  # 实际加的板数
        pallet_adds = [id for s, id in zip(selections, pallet_add) if s == "on"]
        pallet_adds = [int(pallet) for pallet in pallet_adds]
        total_pallet = sum(pallet_adds)
        destinations = request.POST.getlist("destination")
        pallets = request.POST.getlist("pallets")  # 该仓点下原本没有约的板数
        container_numbers = request.POST.getlist("container")
        plt_id = request.POST.getlist("added_plt_ids")
        po_id = request.POST.getlist("added_po_id")
        results = []
        des = set()
        # 按顺序记录选中的每一行加塞的信息
        for p in on_positions:
            result = {}
            result["destination"] = destinations[p]
            des.add(result["destination"])
            result["pallet_add"] = int(pallet_add[p])
            result["container_number"] = container_numbers[p]
            result["pallets"] = int(pallets[p])
            pid_list = [int(i) for i in plt_id[p].split(",") if i]
            result["ids"] = pid_list
            po_list = po_id[p]
            result["po_id"] = po_list
            results.append(result)
        total_weight, total_cbm, total_pcs = 0.0, 0.0, 0
        plt_ids = [id for s, id in zip(selections, plt_id) if s == "on"]
        plt_ids = [int(i) for id in plt_ids for i in id.split(",") if i]
        Utilized_pallet_ids = []
        # 记录加塞的plt_id
        for r in range(len(results)):
            results[r][
                "has_master_shipment"
            ] = True  # 默认有主约，因为只有没有主约的时候才需要处理主约
            results[r]["is_fully_add"] = False  # 默认当前不是一次全部加塞
            if results[r]["pallet_add"] < results[r]["pallets"]:
                Utilized_pallet_ids += results[r]["ids"][: results[r]["pallet_add"]]
                Utilized_pallet_ids = [int(i) for i in Utilized_pallet_ids]
                results[r]["ids"] = Utilized_pallet_ids
            elif results[r]["pallet_add"] == results[r]["pallets"]:
                # 如果加塞了当前剩余的全部板子，还要看该PO_ID下有没有主约，没有主约再加主约
                # 这里包括两种情况：1、这是第一次被加塞，2、这不是第一次被加塞，不管是第几次只要没有剩余板子了，都需要确认主约
                has_master_shipment = await sync_to_async(
                    Pallet.objects.filter(PO_ID=results[r]["po_id"])
                    .exclude(master_shipment_batch_number__isnull=True)
                    .exists
                )()
                results[r]["has_master_shipment"] = has_master_shipment

        pallet = await sync_to_async(list)(
            Pallet.objects.select_related("container_number").filter(
                id__in=Utilized_pallet_ids
            )
        )
        for plt in pallet:
            total_weight += plt.weight_lbs
            total_pcs += plt.pcs
            total_cbm += plt.cbm
        # 查找该出库批次,将重量等信息加到出库批次上
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        fleet.total_weight += total_weight
        fleet.total_pcs += total_pcs
        fleet.total_cbm += total_cbm
        fleet.total_pallet += total_pallet
        await sync_to_async(fleet.save)()
        # 查找该出库批次下的约，把加塞的柜子板数加到同一个目的地的约
        shipments = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number").filter(
                fleet_number__fleet_number=fleet_number,
            )
        )
        # 和添加PO的过程相同
        await self.handle_alter_po_shipment_post(results, shipments)
        mutable_get = request.GET.copy()
        mutable_get["warehouse"] = request.POST.get("warehouse")
        mutable_get["fleet_number"] = fleet_number
        request.GET = mutable_get
        return await self.handle_fleet_depature_get(request)

    async def handle_fleet_info_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number")
        mutable_post = request.POST.copy()
        mutable_post["name"] = request.GET.get("warehouse")
        request.POST = mutable_post
        _, context = await self.handle_fleet_warehouse_search_post(request)
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number)
        )
        context.update(
            {
                "fleet": fleet,
                "shipment": shipment,
                "carrier_options": self.carrier_options,
            }
        )
        return self.template_fleet_schedule_info, context

    async def _update_fleet_type(self, fleet:Fleet):
        shipments_list = []
        async for shipment in Shipment.objects.filter(fleet_number=fleet):
            shipments_list.append(shipment)
        
        if not shipments_list:
            context = {'error_messages':f'{fleet.fleet_number}没有绑定的预约批次，请核实！'}
            return context
        
        # 获取所有shipment_type并去重
        shipment_types = set()
        for shipment in shipments_list:
            if shipment.shipment_type:
                shipment_types.add(shipment.shipment_type)
        
        if not shipment_types:
            context = {'error_messages':f'{fleet.fleet_number}绑定的预约批次没有预约类型，请核实'}
            return context
        
        fleet_type = list(shipment_types)[0]
        fleet.fleet_type = fleet_type
        await fleet.asave()
        return None
                    
    async def handle_fleet_depature_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selected_fleet_number = request.GET.get("fleet_number")
        warehouse = request.GET.get("warehouse")
        selected_fleet = await sync_to_async(Fleet.objects.get)(
            fleet_number=selected_fleet_number
        )
        error_messages = None
        if not selected_fleet.fleet_type:
            #重新赋值类型
            ctx = await self._update_fleet_type(selected_fleet)
            if ctx and ctx.get('error_messages'):
                error_messages = ctx.get('error_messages')
            selected_fleet = await sync_to_async(Fleet.objects.get)(
                fleet_number=selected_fleet_number
            )
            
        # 因为LTL和客户自提，要求的拣货单字段不一样，所以这个是LTL和客户自提的拣货单
        arm_pickup = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number__container_number",
                "shipment_batch_number__fleet_number",
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number
            )
            .values(
                "container_number__container_number",
                "zipcode",
                "shipping_mark",
                "destination",
                "shipment_batch_number__ARM_BOL",
                "shipment_batch_number__fleet_number__carrier",
                "shipment_batch_number__fleet_number__appointment_datetime",
                "address",
                "slot",
            )
            .annotate(
                total_pcs=Sum("pcs"),
                total_pallet=Count("pallet_id", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
            )
        )
        for arm in arm_pickup:
            marks = arm["shipping_mark"]
            if marks:
                array = marks.split(",")
                if len(array) > 2:
                    parts = []
                    for i in range(0, len(array), 2):
                        part = ",".join(array[i : i + 2])
                        parts.append(part)
                    new_marks = "\n".join(parts)
                else:
                    new_marks = marks
                arm["shipping_mark"] = new_marks
            else:
                arm["shipping_mark"] = ""
        shipment = await sync_to_async(list)(
            Pallet.objects.select_related(
                "shipment_batch_number",
                "shipment_batch_number__fleet_number",
                "container_number",
                "container_number__orders__offload_id",
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__orders__offload_id__offload_at__isnull=False,
            )
            .annotate(
                str_id=Cast("id", CharField()),
            )
            .values(
                "shipment_batch_number__shipment_batch_number",
                "container_number__container_number",
                "destination",
                "shipment_batch_number__appointment_id",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__note",
                "shipment_batch_number__fleet_number__carrier",
                "shipment_batch_number__ARM_PRO",
                "is_dropped_pallet"
            )
            .annotate(
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_weight=Sum("weight_lbs", output_field=FloatField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-shipment_batch_number__shipment_appointment")
        )
        shipment_pl = await sync_to_async(list)(
            PackingList.objects.select_related(
                "shipment_batch_number",
                "shipment_batch_number__fleet_number",
                "container_number",
                "container_number__orders__offload_id",
            )
            .annotate(
                str_id=Cast("id", CharField()),
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__orders__offload_id__offload_at__isnull=True,
            )
            .values(
                "shipment_batch_number__shipment_batch_number",
                "container_number__container_number",
                "destination",
                "shipment_batch_number__appointment_id",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__note",
            )
            .annotate(
                pl_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_weight=Sum("total_weight_lbs", output_field=FloatField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_pallet=Sum("cbm", output_field=FloatField()) / 2,
            )
            .order_by("-shipment_batch_number__shipment_appointment")
        )
        for s in shipment_pl:
            if s["total_pallet"] < 1:
                s["total_pallet"] = 1
            elif s["total_pallet"] % 1 >= 0.45:
                s["total_pallet"] = int(s["total_pallet"] // 1 + 1)
            else:
                s["total_pallet"] = int(s["total_pallet"] // 1)
        shipment += shipment_pl
        packing_list = {}
        for s in shipment:
            pl = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=s[
                        "shipment_batch_number__shipment_batch_number"
                    ]
                )
            )
            packing_list[s["shipment_batch_number__shipment_batch_number"]] = pl

        pl_fleet = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number", "shipment_batch_number", "packing_list"
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number
            )
            .values(
                "container_number__container_number",
                "destination",
                "shipment_batch_number__shipment_batch_number",
                "shipment_batch_number__shipment_appointment",
                "slot",
            )
            .annotate(
                total_weight=Sum("weight_lbs"),
                total_cbm=Round(Sum("cbm"), precision=2, output_field=FloatField()),
                total_n_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-shipment_batch_number__shipment_appointment")
        )
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=selected_fleet_number)
            .order_by("-shipment_appointment")
            .values("id", "shipment_batch_number", "shipment_appointment")
        )
        if len(shipments) > 1:
            shipment_order = {}
            for i, s in enumerate(shipments, 1):
                shipment_order[s["shipment_batch_number"]] = i

            for item in pl_fleet:
                batch_num = item["shipment_batch_number__shipment_batch_number"]
                order = shipment_order.get(batch_num, 0)

                if order == 1:
                    item["load_position"] = "inside 1"
                elif order == len(shipments):
                    item["load_position"] = f"outside {len(shipments)}"
                else:
                    item["load_position"] = f"inside {order}"
        shipment_batch_numbers = []
        destinations = []
        for s in shipment:
            if (
                s.get("shipment_batch_number__shipment_batch_number")
                not in shipment_batch_numbers
            ):
                shipment_batch_numbers.append(
                    s.get("shipment_batch_number__shipment_batch_number")
                )
            destination = s.get("destination")
            lower_destination = destination.lower()
            if "Walmart-" in lower_destination:
                new_destination = destination.replace("Walmart-", "")
                destinations.append(new_destination)
            else:
                if destination not in destinations:
                    destinations.append(destination)
        mutable_post = request.POST.copy()
        mutable_post["name"] = request.GET.get("warehouse")
        request.POST = mutable_post
        _, context = await self.handle_outbound_warehouse_search_post(request)
        # 记录可能加塞的柜子，筛选条件：同一个目的地且未出库或甩板的柜子，可能没有预约批次
        criteria_plt = models.Q(
            models.Q(shipment_batch_number__fleet_number__fleet_number__isnull=True)
            | models.Q(
                shipment_batch_number__fleet_number__fleet_number__isnull=False,
                shipment_batch_number__fleet_number__departured_at__isnull=True,
            ),
            location=warehouse,
            destination__in=destinations,
            container_number__orders__offload_id__offload_at__isnull=False,
        )
        plt_unshipped = await self._get_packing_list(
            criteria_plt, selected_fleet_number
        )
        context.update(
            {
                "selected_fleet": selected_fleet,
                "shipment": shipment,
                "warehouse": warehouse,
                "shipment_batch_numbers": shipment_batch_numbers,
                "packing_list": packing_list,
                "pl_fleet": pl_fleet,
                "arm_pickup": arm_pickup,
                "plt_unshipped": plt_unshipped,
                "delivery_options": DELIVERY_METHOD_OPTIONS,
            }
        )
        if error_messages:
            context.update({'error_messages':error_messages})
        return self.template_outbound_departure, context

    async def handle_delivery_and_pod_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number", "")
        batch_number = request.GET.get("batch_number", "")
        area = request.POST.get("area")
        if area == "None" or not area:
            area = None
        criteria = models.Q(
            is_arrived=False,
            is_canceled=False,
            is_shipped=True,
            shipment_type__in=[
                "FTL",
                "LTL",
                "外配",
                "快递",
            ],  # LTL和客户自提的不需要确认送达
        ) & ~Q(status="Exception")
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        if area:
            criteria &= models.Q(origin=area)
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
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "shipments": shipments,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "shipment": json.dumps(shipment_fleet_dict),
            "warehouse_options": self.warehouse_options,
            "area": area,
        }
        return self.template_delivery_and_pod, context

    async def handle_fleet_cost_export(self, request: HttpRequest) -> HttpResponse:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Fleet Cost Template"
        headers = ["PickUp Number", "预约批次", "ISA", "费用"]
        ws.append(headers)

        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = (
            "attachment; filename=fleet_cost_template.xlsx"
        )
        wb.save(response)

        return response

    def read_csv_smart(self, file) -> pd.DataFrame:
        raw_data = file.read(10000)  # 读取前 10000 字节检测编码
        file.seek(0)

        # 检测编码
        result = chardet.detect(raw_data)
        encoding = result["encoding"]

        try:
            file.seek(0)
            df = pd.read_csv(file, encoding=encoding)
            return df
        except UnicodeDecodeError:
            encodings_to_try = ["utf-8", "gbk", "gb18030", "latin1"]
            for enc in encodings_to_try:
                try:
                    file.seek(0)  # 每次尝试前重置文件指针
                    df = pd.read_csv(file, encoding=enc)
                    return df
                except UnicodeDecodeError:
                    continue
            raise RuntimeError("无法识别文件编码，请检查文件格式！")

    async def handle_upload_fleet_cost_get(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        error_messages = []  # 错误信息
        success_count = 0
        valid_rows = []

        # 验证表单
        form = UploadFileForm(request.POST, request.FILES)
        if not form.is_valid():
            # 表单验证失败时的错误信息
            for field, errors in form.errors.items():
                for error in errors:
                    error_messages.append(f"表单错误 - {field}: {error}")
            return await self.handle_fleet_cost_record_get(request, error_messages, success_count)

        try:
            file = request.FILES["file"]
            # 读取Excel文件
            df = pd.read_excel(file)

            # 检查必要列是否存在
            if "费用" not in df.columns:
                error_messages.append(f"文件缺少必要列'费用'。找到的列: {df.columns.tolist()}")
                return await self.handle_fleet_cost_record_get(request, error_messages, success_count)

            # 处理每一行数据
            for index, row in df.iterrows():
                row_number = index + 2  # Excel行号从2开始（跳过表头）

                # 验证：费用存在 或 (PickUp Number/出库批次/预约批次 至少一个存在)
                has_cost = pd.notna(row["费用"])
                has_identifier = any([
                    pd.notna(row.get("PickUp Number")),
                    pd.notna(row.get("预约批次")),
                    pd.notna(row.get("ISA")),
                ])

                if not (has_cost or has_identifier):
                    error_messages.append(f"第{row_number}行: 缺少费用或车次识别信息")
                    continue

                # 处理ISA值
                isa_value = ""
                if pd.notna(row.get("ISA")):
                    try:
                        isa_value = str(int(float(row["ISA"]))).strip()
                    except (ValueError, TypeError) as e:
                        error_messages.append(f"第{row_number}行: ISA值 '{row['ISA']}' 转换失败 - {str(e)}")
                        continue

                # 处理PickUp Number
                pickup_number = ""
                if pd.notna(row.get("PickUp Number")):
                    pickup_number = str(row["PickUp Number"]).strip()

                # 处理预约批次
                shipment_batch_number = ""
                if pd.notna(row.get("预约批次")):
                    shipment_batch_number = str(row["预约批次"]).strip()

                # 处理费用
                fleet_cost = 0.0
                if pd.notna(row["费用"]):
                    try:
                        fleet_cost = float(row["费用"])
                    except (ValueError, TypeError) as e:
                        error_messages.append(f"第{row_number}行: 费用值 '{row['费用']}' 转换失败 - {str(e)}")
                        continue

                # 添加到有效行列表
                valid_rows.append((
                    pickup_number,
                    shipment_batch_number,
                    isa_value,
                    fleet_cost,
                    row_number
                ))

            # 处理有效数据行
            for (
                    pickup_number,
                    shipment_batch_number,
                    ISA,
                    fleet_cost,
                    row_number
            ) in valid_rows:
                try:
                    # 验证费用是否有效
                    if fleet_cost <= 0:
                        error_messages.append(f"第{row_number}行: 费用不能为负或零（当前值：{fleet_cost}）")
                        continue

                    fleet = None
                    search_criteria = ""

                    # 根据PickUp Number查找车次
                    if pickup_number:
                        fleet_query = await sync_to_async(list)(
                            Fleet.objects.filter(pickup_number=pickup_number).only(
                                "id", "fleet_number", "pickup_number", "fleet_cost"
                            )
                        )
                        if len(fleet_query) > 1:
                            error_messages.append(f"第{row_number}行: PickUp Number '{pickup_number}' 对应多个车次")
                            continue
                        if not fleet_query:
                            await sync_to_async(FleetDoseNot.objects.create)(pickup_number=pickup_number,
                                                                             user=request.user)
                            error_messages.append(
                                f"第{row_number}行: 未找到 PickUp Number '{pickup_number}' 对应的车次")
                            continue
                        fleet = fleet_query[0]
                        search_criteria = f"PickUp Number: {pickup_number}"

                    # 根据预约批次查找车次
                    elif shipment_batch_number:
                        try:
                            shipment = await sync_to_async(Shipment.objects.get)(
                                shipment_batch_number=shipment_batch_number
                            )
                            fleet = await sync_to_async(getattr)(shipment, 'fleet_number', None)
                            if not fleet:
                                await sync_to_async(FleetDoseNot.objects.create)(shipment_batch_number=shipment_batch_number,
                                                                                 user=request.user)
                                error_messages.append(
                                    f"第{row_number}行: 预约批次 '{shipment_batch_number}' 未关联车次")
                                continue
                            search_criteria = f"预约批次: {shipment_batch_number}"
                        except Shipment.DoesNotExist:
                            error_messages.append(
                                f"第{row_number}行: 未找到预约批次 '{shipment_batch_number}' 对应的记录")
                            continue

                    # 根据ISA查找车次
                    elif ISA:
                        try:
                            shipment = await sync_to_async(Shipment.objects.get)(appointment_id=ISA)
                            fleet = await sync_to_async(getattr)(shipment, 'fleet_number', None)
                            if not fleet:
                                await sync_to_async(FleetDoseNot.objects.create)(
                                    appointment_id=ISA,
                                    user=request.user)
                                error_messages.append(f"第{row_number}行: ISA '{ISA}' 未关联车次")
                                continue
                            search_criteria = f"ISA: {ISA}"
                        except Shipment.DoesNotExist:
                            error_messages.append(f"第{row_number}行: 未找到ISA '{ISA}' 对应的记录")
                            continue
                        except Shipment.MultipleObjectsReturned:
                            error_messages.append(f"第{row_number}行: ISA '{ISA}' 对应多条记录")
                            continue

                    # 没有识别信息
                    else:
                        error_messages.append(f"第{row_number}行: 缺少车次识别信息")
                        continue

                    # 更新车次费用
                    fleet.fleet_cost = fleet_cost
                    await sync_to_async(fleet.save)()

                    # 分摊车次成本
                    await self.insert_fleet_shipment_pallet_fleet_cost(
                        request, fleet.fleet_number, fleet_cost
                    )

                    # 成功计数+1
                    success_count += 1

                except Exception as e:
                    # 捕获其他未预期的异常
                    error_messages.append(f"第{row_number}行: 处理失败 - {str(e)}")
                    continue

        except Exception as e:
            # 捕获文件处理过程中的异常
            error_messages.append(f"文件处理失败: {str(e)}")

        # 返回处理结果
        return await self.handle_fleet_cost_record_get(request, error_messages, success_count)

    async def handle_download_ltl_table(self, request: HttpRequest) -> HttpResponse:
        """下载已录入页面的表格数据为Excel"""
        # 获取筛选条件
        start_time = request.POST.get("start_time")
        end_time = request.POST.get("end_time")
        area = request.POST.get("area")

        # 构建查询条件（复用原有逻辑）
        now = timezone.now()
        tz_2026_01_01 = timezone.make_aware(
            datetime(2026, 1, 1, 0, 0, 0),
            timezone.get_current_timezone()
        )

        criteria = models.Q(
            arrived_at__isnull=False,
            shipped_at__gte=tz_2026_01_01,
            shipment_schduled_at__gte=tz_2026_01_01,
            fleet_number__fleet_type__in=['LTL', '客户自提'],
            fleet_number__fleet_cost__isnull=False  # 仅已录入数据
        )

        # 时间过滤逻辑（复用原有）
        if start_time or end_time:
            start_datetime = None
            end_datetime = None

            if start_time:
                naive_start = datetime.strptime(start_time, "%Y-%m-%d")
                start_datetime = timezone.make_aware(naive_start, timezone.get_current_timezone())

            if end_time:
                naive_end = datetime.strptime(end_time, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
                end_datetime = timezone.make_aware(naive_end, timezone.get_current_timezone())

            order_filter = models.Q()
            if start_datetime:
                order_filter &= models.Q(
                    container_number__pallet__shipment_batch_number__shipped_at__gte=start_datetime
                )
            if end_datetime:
                order_filter &= models.Q(
                    container_number__pallet__shipment_batch_number__shipped_at__lte=end_datetime
                )

            container_ids = await sync_to_async(list)(
                Order.objects.filter(order_filter)
                .values_list('container_number_id', flat=True)
                .distinct()
            )

            pallet_ids = []
            if container_ids:
                pallet_ids = await sync_to_async(list)(
                    Pallet.objects.filter(container_number_id__in=container_ids)
                    .values_list('id', flat=True)
                    .distinct()
                )

            shipment_ids = []
            if pallet_ids:
                shipment_ids = await sync_to_async(list)(
                    Pallet.objects.filter(id__in=pallet_ids)
                    .values_list('shipment_batch_number', flat=True)
                    .distinct()
                )

            if start_time or end_time:
                if shipment_ids:
                    criteria &= models.Q(id__in=shipment_ids)
                else:
                    criteria &= models.Q(id__in=[])

        # 查询数据
        shipment = await sync_to_async(list)(
            Shipment.objects
            .select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )

        # 处理数据（复用原有process_shipment_data逻辑）
        processed_data = await self.process_shipment_data_for_download(shipment)

        # 构建Excel数据
        df = pd.DataFrame([
            {
                '柜号': item.container_number,
                # ✅ 【已修复】一提多卸：判断当前车次是否在分组中
                '一提多卸': '是' if hasattr(item, 'is_multi_unload') and item.is_multi_unload else '否',
                '目的仓库': item.all_locations,
                '目的地': item.destination,
                '唛头': item.shipping_mark,
                '总件数': item.shipped_pcs,
                '总卡板数': item.shipped_pallet,
                '车次费用': item.fleet_number.fleet_cost if (item.fleet_number and item.fleet_number.fleet_cost) else 0,
                '分摊价格': item.allocation_price,
                '实际出库时间': item.shipped_at.strftime("%Y-%m-%d %H:%M") if item.shipped_at else '-',
                '拆柜时间': item.offload_at.strftime("%Y-%m-%d %H:%M") if item.offload_at else '-',
                '出库批次': item.fleet_number.fleet_number if (
                        item.fleet_number and item.fleet_number.fleet_number) else '-',
                '预约批次': item.shipment_batch_number,
                'Carrier': item.carrier,
                '备注': item.note,
                '退回费用': item.fleet_number.fleet_cost_back if (
                        item.fleet_number and item.fleet_number.fleet_cost_back) else 0,
                '核实状态': '已核实' if (item.fleet_number and item.fleet_number.fleet_verify_status) else '未核实'
            }
            for item in processed_data
        ])

        # 生成Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='已录入LTL数据', index=False)

        # 构建响应
        output.seek(0)
        response = HttpResponse(
            output,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="已录入LTL数据_{now.strftime("%Y%m%d%H%M%S")}.xlsx"'
        return response

    async def process_shipment_data_for_download(self, shipment_list):
        """处理下载用的发货单数据（复用原有逻辑）"""
        processed = []
        for s in shipment_list:
            # 复用原有process_shipment_data中的逻辑
            latest_pallet = await sync_to_async(
                lambda: s.fleetshipmentpallets.order_by("-cost_input_time", "-id").first()
            )()

            @sync_to_async
            def get_related_pallets(shipment_id):
                pallets = list(Pallet.objects.filter(
                    shipment_batch_number=shipment_id
                ).select_related("container_number"))

                container_ids = [p.container_number_id for p in pallets if p.container_number_id]
                order_map = {}
                if container_ids:
                    orders = Order.objects.filter(container_number_id__in=container_ids).select_related("offload_id")
                    for order in orders:
                        order_map[order.container_number_id] = order

                group_key = lambda p: (
                    p.container_number.container_number if p.container_number else "",
                    p.shipping_mark or ""
                )

                pallet_groups = {}
                for pallet in pallets:
                    key = group_key(pallet)
                    if key not in pallet_groups:
                        pallet_groups[key] = {
                            'pallets': [],
                            'total_pallet_count': 0,
                            'total_pcs_count': 0,
                            'container_number': key[0] or "-",
                            'shipping_mark': key[1] or "-",
                            'offload_at': None,
                            'retrieval_time': None,
                            'locations': set(),
                        }

                    pallet_groups[key]['total_pallet_count'] += 1
                    pallet_groups[key]['total_pcs_count'] += pallet.pcs or 0
                    pallet_groups[key]['pallets'].append(pallet)

                    if pallet.location:
                        pallet_groups[key]['locations'].add(pallet.location)

                    if pallet.container_number_id and not pallet_groups[key]['offload_at']:
                        order = order_map.get(pallet.container_number_id)
                        if order and order.offload_id:
                            pallet_groups[key]['offload_at'] = order.offload_id.offload_at
                            pallet_groups[key]['retrieval_time'] = order.offload_id.offload_at

                for group in pallet_groups.values():
                    group['locations'] = list(group['locations']) if group['locations'] else ["-"]

                return pallet_groups

            pallet_groups = await get_related_pallets(s.id)

            for group_key, group_data in pallet_groups.items():
                shipment_copy = copy.deepcopy(s)
                shipment_copy.related_pallet = group_data['pallets']
                shipment_copy.shipped_pcs = group_data['total_pcs_count'] or 0
                shipment_copy.shipped_pallet = group_data['total_pallet_count'] or 0
                shipment_copy.container_number = group_data['container_number']
                shipment_copy.shipping_mark = group_data['shipping_mark']
                shipment_copy.offload_at = group_data['offload_at']
                shipment_copy.retrieval_time = group_data['retrieval_time']
                shipment_copy.all_locations = group_data['locations'][0]

                # 计算分摊价格（复用原有逻辑）
                fleet_code = shipment_copy.fleet_number.fleet_number if (
                        shipment_copy.fleet_number and shipment_copy.fleet_number.fleet_number
                ) else "-"

                @sync_to_async
                def get_fleet_pallet_count(fleet_code):
                    if not fleet_code:
                        return 0
                    return Pallet.objects.filter(
                        shipment_batch_number__fleet_number__fleet_number=fleet_code,
                        shipment_batch_number__fleet_number__fleet_type='LTL'
                    ).count()

                total_pallets_of_fleet = await get_fleet_pallet_count(fleet_code)
                fleet_total_cost = shipment_copy.fleet_number.fleet_cost if (
                        shipment_copy.fleet_number and shipment_copy.fleet_number.fleet_cost
                ) else 0

                if total_pallets_of_fleet > 0 and fleet_total_cost > 0:
                    allocation_price = (fleet_total_cost / total_pallets_of_fleet) * shipment_copy.shipped_pallet
                    shipment_copy.allocation_price = round(allocation_price, 2)
                else:
                    shipment_copy.allocation_price = 0

                processed.append(shipment_copy)

            if not pallet_groups:
                shipment_copy = copy.deepcopy(s)
                shipment_copy.related_pallet = []
                shipment_copy.shipped_pcs = 0
                shipment_copy.shipped_pallet = 0
                shipment_copy.container_number = "-"
                shipment_copy.shipping_mark = "-"
                shipment_copy.offload_at = None
                shipment_copy.retrieval_time = None
                shipment_copy.all_locations = ["-"]
                shipment_copy.allocation_price = 0
                processed.append(shipment_copy)

        return processed

    async def handle_fleet_cost_confirm_get(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """
        成本录入-确认成本费用
        核心逻辑：
        1. 更新Fleet的fleet_cost字段；
        2. 调用专用方法创建成本费用记录并分摊费用到FleetShipmentPallet；
        3. 兼容无托盘记录的场景，补充创建基础记录。
        """
        fleet_number = request.POST.get("fleet_number", "")
        # 修复1：增加空值/非数字校验，避免float转换报错
        fleet_cost_str = request.POST.get("fleet_cost", "")
        if not fleet_number:
            raise ValueError("车次编号不能为空")
        if not fleet_cost_str:
            raise ValueError("成本费用不能为空")
        try:
            fleet_cost = float(fleet_cost_str)
        except (ValueError, TypeError):
            raise ValueError("成本费用必须是有效数字（如 100.00）")

        # 查询车次记录
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 更新车次成本
        fleet.fleet_cost = fleet_cost
        await sync_to_async(fleet.save)()

        error_messages = []
        # 调用专用方法创建成本费用记录并分摊
        try:
            await self.insert_fleet_shipment_pallet_fleet_cost(
                request, fleet_number, fleet_cost
            )
        except RuntimeError as e:
            error_messages.append(f"成本费用分摊失败：{str(e)}")
        except ValueError as e:
            # 无托盘数据时，保留原有提示逻辑
            error_messages.append(f"{fleet_number}车次里面板子是空的")

        # 重置请求参数，返回原有逻辑
        request.POST = request.POST.copy()
        request.POST["fleet_number"] = ""
        return await self.handle_fleet_cost_record_get(request, None, 0)

    async def handle_fleet_cost_confirm_get_ltl(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """
        ltl成本录入-确认成本费用
        核心逻辑：
        1. 更新Fleet的fleet_cost字段；
        2. 调用专用方法创建成本费用记录并分摊费用到FleetShipmentPallet；
        3. 兼容无托盘记录的场景，补充创建基础记录。
        """
        fleet_number = request.POST.get("fleet_number", "")
        # 修复1：增加空值/非数字校验，避免float转换报错
        fleet_cost_str = request.POST.get("fleet_cost", "")
        if not fleet_number:
            raise ValueError("车次编号不能为空")
        if not fleet_cost_str:
            raise ValueError("成本费用不能为空")
        try:
            fleet_cost = float(fleet_cost_str)
        except (ValueError, TypeError):
            raise ValueError("成本费用必须是有效数字（如 100.00）")

        # 查询车次记录
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 更新车次成本
        fleet.fleet_cost = fleet_cost
        await sync_to_async(fleet.save)()

        error_messages = []
        # 调用专用方法创建成本费用记录并分摊
        try:
            await self.insert_fleet_shipment_pallet_fleet_cost(
                request, fleet_number, fleet_cost
            )
        except RuntimeError as e:
            error_messages.append(f"成本费用分摊失败：{str(e)}")
        except ValueError as e:
            # 无托盘数据时，保留原有提示逻辑
            error_messages.append(f"{fleet_number}车次里面板子是空的")

        # 重置请求参数，返回原有逻辑
        request.POST = request.POST.copy()
        request.POST["fleet_number"] = ""
        return await self.handle_fleet_cost_record_get_ltl(request)

    async def insert_fleet_shipment_pallet_fleet_cost(self, request, fleet_number, fleet_cost):
        """
        传入车次号-车次分摊成本费用到FleetShipmentPallet
        核心逻辑：
        1. 按PO_ID分组查询托盘数据，创建FleetShipmentPallet记录；
        2. 按总托盘数分摊本次成本费用到每条记录的expense字段。
        """
        criteria_plt = models.Q(
            shipment_batch_number__fleet_number__fleet_number=fleet_number
        )
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 查询该车次下所有FleetShipmentPallet记录，计算分摊
        fleet_shipments = await sync_to_async(list)(
            FleetShipmentPallet.objects.filter(
                fleet_number__fleet_number=fleet_number, description='成本费用'
            ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
        )

        # 该成本FleetShipmentPallet之前不存在
        if not fleet_shipments:
            # ===================== 【核心修改：只按柜号分组】 =====================
            grouped_pallets = await sync_to_async(list)(
                Pallet.objects.filter(criteria_plt)
                .values("container_number")
                .annotate(
                    actual_pallets=Count("pallet_id"),  # 该柜总板数
                    shipment_batch_number_id=models.F("shipment_batch_number"),  # 取批次
                    PO_ID=models.F("PO_ID")  # 取PO
                )
                .order_by("container_number")
            )

            if not grouped_pallets:
                raise ValueError(f"车次 {fleet_number} 无有效托盘数据，无法创建成本费用记录")

            # 批量创建标注"成本费用"的FleetShipmentPallet记录
            new_fleet_shipment_pallets = []
            now_time = timezone.now()
            for group in grouped_pallets:
                new_record = FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number_id=group['shipment_batch_number_id'],
                    PO_ID=group["PO_ID"],
                    total_pallet=group["actual_pallets"],  # 该柜总板数
                    container_number_id=group["container_number"],  # 按柜号
                    is_recorded=False,
                    cost_input_time=now_time,
                    operator=request.user,
                    description="成本费用"
                )
                new_fleet_shipment_pallets.append(new_record)

            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )

            # 重新查询
            fleet_shipments = await sync_to_async(list)(
                FleetShipmentPallet.objects.filter(
                    fleet_number__fleet_number=fleet_number, description='成本费用'
                ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
            )

            if not fleet_shipments:
                raise RuntimeError(f"车次 {fleet_number} 创建成本费用记录后，查询结果为空")

            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()
                        # 若invoicev2存在
                        if existing_invoice:
                            # 派送车次成本
                            old_cost = Decimal(str(existing_invoice.payable_delivery_cost or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            # 重新计算新成本
                            # 派送车次成本
                            existing_invoice.payable_delivery_cost = old_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_cost',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
                        # 若invoicev2不存在
                        else:
                            invoices_to_create.append(Invoicev2(
                                container_number_id=container_id,
                                created_at=current_date,
                                invoice_date=current_date,
                                payable_delivery_cost=total_expense,
                                payable_delivery_amount=total_expense,
                                payable_total_amount=total_expense
                            ))

                    if invoices_to_create:
                        await sync_to_async(Invoicev2.objects.bulk_create)(
                            invoices_to_create, batch_size=500
                        )
            except Exception as e:
                raise RuntimeError(f"成本费用记录创建/分摊失败：{str(e)}")
        else:
            # 该成本FleetShipmentPallet之前存在，需要修改车次成本， 需要把上一个该成本的分摊汇总到invoicev2的值查出来
            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                # 旧车成本
                old_fleet_shipments_total_cost = sum([fs.expense for fs in fleet_shipments if fs.expense is not None])
                old_fleet_cost_decimal = Decimal(str(old_fleet_shipments_total_cost or '0.0000'))

                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 下面是新车分摊
                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()

                        if existing_invoice:
                            # 派送车次成本
                            old_cost = Decimal(str(existing_invoice.payable_delivery_cost or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            old_fleet_cost = old_fleet_cost_decimal

                            # 重新计算新成本
                            # 派送车次成本
                            existing_invoice.payable_delivery_cost = old_cost - old_fleet_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount - old_fleet_cost + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total - old_fleet_cost + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_cost',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
            except Exception as e:
                raise RuntimeError(f"成本费用记录创建/分摊失败：{str(e)}")

    async def insert_fleet_shipment_pallet_fleet_cost_tranfer_cost(self, request, fleet_number, fleet_cost):
        """
        转仓车次成本---单独
        传入车次号-车次分摊成本费用到FleetShipmentPallet
        核心逻辑：
        1. 按PO_ID分组查询托盘数据，创建FleetShipmentPallet记录；
        2. 按总托盘数分摊本次成本费用到每条记录的expense字段。
        """
        criteria_plt = models.Q(
            transfer_batch_number__fleet_number__fleet_number=fleet_number
        )
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 查询该车次下所有FleetShipmentPallet记录，计算分摊
        fleet_shipments = await sync_to_async(list)(
            FleetShipmentPallet.objects.filter(
                fleet_number__fleet_number=fleet_number, description='成本费用'
            ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
        )

        # 该成本FleetShipmentPallet之前不存在
        if not fleet_shipments:
            # ===================== 【核心修改：只按柜号分组】 =====================
            grouped_pallets = await sync_to_async(list)(
                Pallet.objects.filter(criteria_plt)
                .values("container_number")
                .annotate(
                    actual_pallets=Count("pallet_id"),  # 该柜总板数
                    shipment_batch_number_id=models.F("shipment_batch_number"),  # 取批次
                    PO_ID=models.F("PO_ID")  # 取PO
                )
                .order_by("container_number")
            )

            if not grouped_pallets:
                raise ValueError(f"车次 {fleet_number} 无有效托盘数据，无法创建成本费用记录")

            # 批量创建标注"成本费用"的FleetShipmentPallet记录
            new_fleet_shipment_pallets = []
            now_time = timezone.now()
            for group in grouped_pallets:
                new_record = FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number_id=group['shipment_batch_number_id'],
                    PO_ID=group["PO_ID"],
                    total_pallet=group["actual_pallets"],  # 该柜总板数
                    container_number_id=group["container_number"],  # 按柜号
                    is_recorded=False,
                    cost_input_time=now_time,
                    operator=request.user,
                    description="成本费用"
                )
                new_fleet_shipment_pallets.append(new_record)

            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )

            # 重新查询
            fleet_shipments = await sync_to_async(list)(
                FleetShipmentPallet.objects.filter(
                    fleet_number__fleet_number=fleet_number, description='成本费用'
                ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
            )

            if not fleet_shipments:
                raise RuntimeError(f"车次 {fleet_number} 创建成本费用记录后，查询结果为空")

            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()
                        # 若invoicev2存在
                        if existing_invoice:
                            # 派送车次成本
                            old_cost = Decimal(str(existing_invoice.payable_delivery_cost or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            # 重新计算新成本
                            # 派送车次成本
                            existing_invoice.payable_delivery_cost = old_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_cost',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
                        # 若invoicev2不存在
                        else:
                            invoices_to_create.append(Invoicev2(
                                container_number_id=container_id,
                                created_at=current_date,
                                invoice_date=current_date,
                                payable_delivery_cost=total_expense,
                                payable_delivery_amount=total_expense,
                                payable_total_amount=total_expense
                            ))

                    if invoices_to_create:
                        await sync_to_async(Invoicev2.objects.bulk_create)(
                            invoices_to_create, batch_size=500
                        )
            except Exception as e:
                raise RuntimeError(f"成本费用记录创建/分摊失败：{str(e)}")
        else:
            # 该成本FleetShipmentPallet之前存在，需要修改车次成本， 需要把上一个该成本的分摊汇总到invoicev2的值查出来
            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                # 旧车成本
                old_fleet_shipments_total_cost = sum([fs.expense for fs in fleet_shipments if fs.expense is not None])
                old_fleet_cost_decimal = Decimal(str(old_fleet_shipments_total_cost or '0.0000'))

                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 下面是新车分摊
                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()

                        if existing_invoice:
                            # 派送车次成本
                            old_cost = Decimal(str(existing_invoice.payable_delivery_cost or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            old_fleet_cost = old_fleet_cost_decimal

                            # 重新计算新成本
                            # 派送车次成本
                            existing_invoice.payable_delivery_cost = old_cost - old_fleet_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount - old_fleet_cost + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total - old_fleet_cost + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_cost',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
            except Exception as e:
                raise RuntimeError(f"成本费用记录创建/分摊失败：{str(e)}")

    async def insert_fleet_shipment_pallet_fleet_cost_transfer(self, request, pallet_ids, fleet_number, fleet_cost):
        """
        传入车次号-车次分摊转仓费用到FleetShipmentPallet
        核心逻辑：
        1. 按PO_ID分组查询托盘数据，创建FleetShipmentPallet记录；
        2. 按总托盘数分摊本次转仓费用；
        3. 转仓费用存入独立字段 payable_delivery_transfer，不影响原有成本。
        """
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 查询该车次下所有【转仓费用】记录
        fleet_shipments = await sync_to_async(list)(
            FleetShipmentPallet.objects.filter(
                fleet_number__fleet_number=fleet_number, description='转仓费用'
            ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
        )

        if not fleet_shipments:
            grouped_pallets = await sync_to_async(list)(
                Pallet.objects.filter(id__in=pallet_ids)
                .values("PO_ID", "container_number", "shipment_batch_number")
                .annotate(actual_pallets=Count("pallet_id"))
                .order_by("shipment_batch_number", "PO_ID")
            )

            if not grouped_pallets:
                raise ValueError(f"车次 {fleet_number} 无有效托盘数据，无法创建转仓费用记录")

            new_fleet_shipment_pallets = []
            now_time = timezone.now()
            for group in grouped_pallets:
                new_record = FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number_id=group['shipment_batch_number'],
                    PO_ID=group["PO_ID"],
                    total_pallet=group["actual_pallets"],
                    container_number_id=group["container_number"],
                    is_recorded=False,
                    cost_input_time=now_time,
                    operator=request.user,
                    description="转仓费用"  # 固定
                )
                new_fleet_shipment_pallets.append(new_record)

            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )

            fleet_shipments = await sync_to_async(list)(
                FleetShipmentPallet.objects.filter(
                    fleet_number__fleet_number=fleet_number, description='转仓费用'
                ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
            )

            if not fleet_shipments:
                raise RuntimeError(f"车次 {fleet_number} 创建转仓费用记录后，查询结果为空")

            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()
                        # 若invoicev2存在
                        if existing_invoice:
                            # 转仓成本
                            old_transfer = Decimal(str(existing_invoice.payable_delivery_transfer or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            # 重新计算新成本
                            # 转仓成本
                            existing_invoice.payable_delivery_transfer = old_transfer + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_transfer',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
                        # 若invoicev2不存在
                        else:
                            invoices_to_create.append(Invoicev2(
                                container_number_id=container_id,
                                created_at=current_date,
                                invoice_date=current_date,
                                payable_delivery_transfer=total_expense,
                                payable_delivery_amount=total_expense,
                                payable_total_amount=total_expense
                            ))

                    if invoices_to_create:
                        await sync_to_async(Invoicev2.objects.bulk_create)(
                            invoices_to_create, batch_size=500
                        )
            except Exception as e:
                raise RuntimeError(f"转仓费用记录创建/分摊失败：{str(e)}")
        else:
            # 该成本FleetShipmentPallet之前存在，需要修改车次成本， 需要把上一个该成本的分摊汇总到invoicev2的值查出来
            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                # 旧车成本
                old_fleet_shipments_total_cost = sum([fs.expense for fs in fleet_shipments if fs.expense is not None])
                old_fleet_cost_decimal = Decimal(str(old_fleet_shipments_total_cost or '0.0000'))

                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊转仓费用")

                # 下面是新车分摊
                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost)) if fleet_cost not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()

                        if existing_invoice:
                            # 转仓成本
                            old_transfer = Decimal(str(existing_invoice.payable_delivery_transfer or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            old_fleet_cost = old_fleet_cost_decimal

                            # 重新计算新成本
                            # 转仓成本
                            existing_invoice.payable_delivery_transfer = old_transfer - old_fleet_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount - old_fleet_cost + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total - old_fleet_cost + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_transfer',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
            except Exception as e:
                raise RuntimeError(f"转仓费用记录创建/分摊失败：{str(e)}")

    async def insert_fleet_shipment_pallet_fleet_cost_back(self, request, fleet_number, fleet_cost_back):
        """
        传入车次号-车次分摊退回费用到FleetShipmentPallet
        核心逻辑：
        1. 按PO_ID分组查询托盘数据，创建标注"退回费用"的FleetShipmentPallet记录；
        2. 按总托盘数分摊本次退回费用到每条记录的expense字段。
        """
        criteria_plt = models.Q(
            shipment_batch_number__fleet_number__fleet_number=fleet_number
        )
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 查询该车次下所有FleetShipmentPallet记录，计算分摊
        fleet_shipments = await sync_to_async(list)(
            FleetShipmentPallet.objects.filter(
                fleet_number__fleet_number=fleet_number, description='退回费用'
            ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
        )
        if not fleet_shipments:
            # ===================== 【核心修改：只按柜号分组】 =====================
            grouped_pallets = await sync_to_async(list)(
                Pallet.objects.filter(criteria_plt)
                .values("container_number")  # 只按柜号分组
                .annotate(
                    actual_pallets=Count("pallet_id"),  # 该柜总板数
                    shipment_batch_number_id=F("shipment_batch_number"),  # 取批次
                    PO_ID=F("PO_ID")  # 取PO
                )
                .order_by("container_number")
            )

            if not grouped_pallets:
                raise ValueError(f"车次 {fleet_number} 无有效托盘数据，无法创建退回费用记录")

            # 批量创建标注"退回费用"的FleetShipmentPallet记录
            new_fleet_shipment_pallets = []
            now_time = timezone.now()
            for group in grouped_pallets:
                new_record = FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number_id=group['shipment_batch_number_id'],
                    PO_ID=group["PO_ID"],
                    total_pallet=group["actual_pallets"],  # 该柜总板数
                    container_number_id=group["container_number"],  # 按柜号
                    is_recorded=False,
                    cost_input_time=now_time,
                    operator=request.user,
                    description="退回费用"
                )
                new_fleet_shipment_pallets.append(new_record)
            # 批量新增退回费用记录
            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )
            # 关键修复：创建后重新查询并赋值fleet_shipments
            fleet_shipments = await sync_to_async(list)(
                FleetShipmentPallet.objects.filter(
                    fleet_number__fleet_number=fleet_number, description='退回费用'
                ).only("id", "PO_ID", "total_pallet", "expense", "container_number")
            )

            # 二次校验：若创建后仍为空，抛出异常
            if not fleet_shipments:
                raise RuntimeError(f"车次 {fleet_number} 创建退回费用记录后，查询结果为空")

            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊成本费用")

                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost_back)) if fleet_cost_back not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()
                        # 若invoicev2存在
                        if existing_invoice:
                            # 退回成本
                            old_refund = Decimal(str(existing_invoice.payable_delivery_refund or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            # 重新计算新成本
                            # 退回成本
                            existing_invoice.payable_delivery_refund = old_refund + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_refund',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
                        # 若invoicev2不存在
                        else:
                            invoices_to_create.append(Invoicev2(
                                container_number_id=container_id,
                                created_at=current_date,
                                invoice_date=current_date,
                                payable_delivery_refund=total_expense,
                                payable_delivery_amount=total_expense,
                                payable_total_amount=total_expense
                            ))

                    if invoices_to_create:
                        await sync_to_async(Invoicev2.objects.bulk_create)(
                            invoices_to_create, batch_size=500
                        )
            except Exception as e:
                raise RuntimeError(f"退回费用记录创建/分摊失败：{str(e)}")
        else:
            # 该成本FleetShipmentPallet之前存在，需要修改车次成本， 需要把上一个该成本的分摊汇总到invoicev2的值查出来
            try:
                # 计算总托盘数
                total_pallets_in_fleet = sum(
                    [fs.total_pallet for fs in fleet_shipments if fs.total_pallet is not None]
                )
                # 旧车成本
                old_fleet_shipments_total_cost = sum([fs.expense for fs in fleet_shipments if fs.expense is not None])
                old_fleet_cost_decimal = Decimal(str(old_fleet_shipments_total_cost or '0.0000'))

                if total_pallets_in_fleet <= 0:
                    raise ValueError(f"车次 {fleet_number} 下无有效托盘数，无法分摊退回费用")

                # 下面是新车分摊
                # 高精度 Decimal 计算
                total_pallets_decimal = Decimal(str(total_pallets_in_fleet))
                fleet_cost = Decimal(str(fleet_cost_back)) if fleet_cost_back not in (None, '') else Decimal('0.0000')

                cost_per_pallet = (fleet_cost / total_pallets_decimal).quantize(
                    Decimal("0.0000"), rounding=ROUND_HALF_UP
                )

                # 批量更新分摊
                update_records = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet is not None and shipment.total_pallet > 0:
                        shipment.expense = cost_per_pallet * Decimal(shipment.total_pallet)
                        update_records.append(shipment)

                if update_records:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        update_records, ["expense"], batch_size=500
                    )

                    # 按柜号汇总费用
                    container_totals = await sync_to_async(
                        lambda: list(FleetShipmentPallet.objects.filter(
                            id__in=[r.id for r in update_records]
                        ).values('container_number').annotate(
                            total_expense=Sum('expense')
                        ))
                    )()

                    current_date = timezone.now().date()
                    invoices_to_create = []

                    for item in container_totals:
                        container_id = item['container_number']
                        total_expense = Decimal(item.get('total_expense')) or Decimal('0.0000')

                        # 取最早的发票
                        existing_invoice = await sync_to_async(
                            Invoicev2.objects.filter(container_number_id=container_id).order_by('created_at').first
                        )()

                        if existing_invoice:
                            # 退回成本
                            old_refund = Decimal(str(existing_invoice.payable_delivery_refund or '0.0000'))
                            # 派送总成本
                            delivery_amount = Decimal(str(existing_invoice.payable_delivery_amount or '0.0000'))
                            # 应付总成本
                            total = Decimal(str(existing_invoice.payable_total_amount or '0.0000'))

                            old_fleet_cost = old_fleet_cost_decimal

                            # 重新计算新成本
                            # 转仓成本
                            existing_invoice.payable_delivery_refund = old_refund - old_fleet_cost + total_expense
                            # 派送总成本
                            existing_invoice.payable_delivery_amount = delivery_amount - old_fleet_cost + total_expense
                            # 应付总成本
                            existing_invoice.payable_total_amount = total - old_fleet_cost + total_expense
                            existing_invoice.invoice_date = current_date

                            await sync_to_async(existing_invoice.save)(
                                update_fields=['invoice_date', 'payable_delivery_refund',
                                               'payable_delivery_amount', 'payable_total_amount']
                            )
            except Exception as e:
                raise RuntimeError(f"退回费用记录创建/分摊失败：{str(e)}")


    async def handle_fleet_cost_confirm_get_back(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """
        成本录入-退回费用再次录入
        核心规则：
        1. fleet_cost：初始成本（不变）；fleet_cost_back：累计退回费用（单独累加）；
        2. 分摊基数 = fleet_cost + fleet_cost_back；
        3. 仅更新fleet_cost_back，不修改fleet_cost；
        4. 调用专用方法创建退回费用记录，并按托盘数分摊退回费用到FleetShipmentPallet。
        """
        fleet_number = request.POST.get("fleet_number", "").strip()
        fleet_cost_back_str = request.POST.get("fleet_cost_back", "").strip()

        if not fleet_number:
            raise ValueError("车次编号不能为空")
        if not fleet_cost_back_str:
            raise ValueError("退回补充费用不能为空")

        try:
            # 确保金额精度（保留2位小数）
            current_back_fee = Decimal(fleet_cost_back_str).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError):
            raise ValueError("退回补充费用必须是有效数字（如 100.00）")

        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 1. 更新Fleet的累计退回费用
        fleet.fleet_cost_back = float(current_back_fee)
        await sync_to_async(fleet.save)()

        # 2. 核心改动：调用专用方法创建退回费用记录并分摊费用
        # 传入本次新增的退回费用金额（current_back_fee）
        await self.insert_fleet_shipment_pallet_fleet_cost_back(
            request,
            fleet_number,
            current_back_fee
        )

        # 重置请求参数，返回原有逻辑
        request.POST = request.POST.copy()
        request.POST["fleet_number"] = ""
        return await self.handle_fleet_cost_record_get(
            request,
            error_messages=[]
        )

    async def handle_fleet_cost_confirm_get_back_ltl(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """
        ltl成本录入-退回费用再次录入
        核心规则：
        1. fleet_cost：初始成本（不变）；fleet_cost_back：累计退回费用（单独累加）；
        2. 分摊基数 = fleet_cost + fleet_cost_back；
        3. 仅更新fleet_cost_back，不修改fleet_cost；
        4. 调用专用方法创建退回费用记录，并按托盘数分摊退回费用到FleetShipmentPallet。
        """
        fleet_number = request.POST.get("fleet_number", "").strip()
        fleet_cost_back_str = request.POST.get("fleet_cost_back", "").strip()

        if not fleet_number:
            raise ValueError("车次编号不能为空")
        if not fleet_cost_back_str:
            raise ValueError("退回补充费用不能为空")

        try:
            # 确保金额精度（保留2位小数）
            current_back_fee = Decimal(fleet_cost_back_str).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError):
            raise ValueError("退回补充费用必须是有效数字（如 100.00）")

        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except ObjectDoesNotExist:
            raise ValueError(f"未找到车次 {fleet_number} 的记录")

        # 1. 更新Fleet的累计退回费用
        fleet.fleet_cost_back = float(current_back_fee)
        await sync_to_async(fleet.save)()

        # 2. 核心改动：调用专用方法创建退回费用记录并分摊费用
        # 传入本次新增的退回费用金额（current_back_fee）
        await self.insert_fleet_shipment_pallet_fleet_cost_back(
            request,
            fleet_number,
            current_back_fee
        )

        # 重置请求参数，返回原有逻辑
        request.POST = request.POST.copy()
        request.POST["fleet_number"] = ""
        return await self.handle_fleet_cost_record_get_ltl(
            request,
            error_messages=[]
        )

    async def handle_fleet_cost_record_get(
            self, request: HttpRequest, error_messages=None, success_count=0
    ) -> tuple[str, dict[str, Any]]:
        """ftl页面"""
        # 获取当前时间（带时区）
        now = timezone.now()
        # 最近一个月的起始时间
        default_start_time = now - timedelta(days=30)
        start_time = request.POST.get("start_time", default_start_time.strftime("%Y-%m-%d"))
        end_time = request.POST.get("end_time", now.strftime("%Y-%m-%d"))
        start_datetime = None
        end_datetime = None
        if start_time:
            naive_start = datetime.strptime(start_time, "%Y-%m-%d")
            start_datetime = timezone.make_aware(naive_start, timezone.get_current_timezone())

        if end_time:
            naive_end = datetime.strptime(end_time, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
            end_datetime = timezone.make_aware(naive_end, timezone.get_current_timezone())
        pickup_number = request.POST.get("pickup_number", "")
        fleet_number = request.POST.get("fleet_number", "")
        batch_number = request.POST.get("batch_number", "")
        area = request.POST.get("area") or None
        status = request.POST.get("status", "")
        shipment_type = None
        if status != "record":
            # 已录入
            criteria = models.Q(
                pod_uploaded_at__isnull=False,
                shipped_at__isnull=False,
                arrived_at__isnull=False,
                shipment_schduled_at__gte="2025-05-01",
                fleet_number__fleet_cost__isnull=False,
                fleet_number__fleet_type='FTL',
                shipped_at__gte=start_datetime,
                shipped_at__lte=end_datetime,

            )
            shipment_type = '已录入内容'
        else:
            # 未录入
            criteria = models.Q(
                pod_uploaded_at__isnull=False,
                shipped_at__isnull=False,
                arrived_at__isnull=False,
                shipment_schduled_at__gte="2025-05-01",
                fleet_number__fleet_cost__isnull=True,
                fleet_number__fleet_type='FTL',
                shipped_at__gte=start_datetime,
                shipped_at__lte=end_datetime,
            )
            shipment_type = '未录入内容'

        if pickup_number:
            criteria &= models.Q(fleet_number__pickup_number=pickup_number)
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        if area and area is not None and area != "None":
            criteria &= models.Q(origin=area)

        # 查询shipment基础数据
        shipment_list = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .prefetch_related(
                Prefetch(
                    "fleetshipmentpallets",
                    queryset=FleetShipmentPallet.objects.select_related("operator")
                )
            )
            .filter(criteria)
            .order_by("shipped_at")
        )

        # 新增：批量查询Pallet数据（优化性能，避免循环查询）
        # 1. 收集所有shipment的id
        shipment_ids = [s.id for s in shipment_list]
        # 2. 查询所有关联的Pallet数据并按shipment_batch_number分组
        pallet_data = await sync_to_async(list)(
            Pallet.objects.filter(
                shipment_batch_number__in=shipment_ids  # shipment_batch_number = shipment.id
            ).values("shipment_batch_number", "weight_lbs", "cbm")
        )
        # 3. 构建分组字典：key=shipment.id，value={总重, 总CBM, 总板数}
        pallet_summary = {}
        for pallet in pallet_data:
            batch_id = pallet["shipment_batch_number"]
            if batch_id not in pallet_summary:
                pallet_summary[batch_id] = {
                    "total_weight_lbs": 0.0,
                    "total_cbm": 0.0,
                    "total_pallets": 0
                }
            # 累加重量和CBM
            pallet_summary[batch_id]["total_weight_lbs"] += float(pallet["weight_lbs"] or 0)
            pallet_summary[batch_id]["total_cbm"] += float(pallet["cbm"] or 0)
            # 总板数+1（每条pallet记录对应一个板子）
            pallet_summary[batch_id]["total_pallets"] += 1

        async def process_shipment_data(shipment_items):
            processed = []
            for s in shipment_items:
                # 原有逻辑：处理fleetshipmentpallets
                latest_pallet = await sync_to_async(
                    lambda: s.fleetshipmentpallets.order_by("-cost_input_time", "-id").first()
                )()

                if latest_pallet:
                    s.pallet_cost_input_time = latest_pallet.cost_input_time
                    s.pallet_operator_name = latest_pallet.operator.username if latest_pallet.operator else None
                else:
                    s.pallet_cost_input_time = None
                    s.pallet_operator_name = None

                # 新增：绑定Pallet计算结果
                summary = pallet_summary.get(s.id, {
                    "total_weight_lbs": 0.0,
                    "total_cbm": 0.0,
                    "total_pallets": 0
                })
                s.shipped_weight = summary["total_weight_lbs"]  # 总重lbs
                s.shipped_cbm = summary["total_cbm"]  # 总CBM
                s.shipped_pallet = summary["total_pallets"]  # 总卡板数

                processed.append(s)
            return processed

        # 处理数据（包含新增的Pallet计算结果）
        shipment = await process_shipment_data(shipment_list)

        context = {
            "start_time": start_time,
            "end_time": end_time,
            "shipment_type": shipment_type,
            "pickup_number": pickup_number,
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": shipment,
            "upload_file_form": UploadFileForm(required=True),
            "warehouse_options": self.warehouse_options,
            "area": area,
            "error_messages": error_messages or [],
            "success_count": success_count,
        }
        return self.template_fleet_cost_record, context

    async def handle_fleet_cost_record_get_ltl(
            self, request: HttpRequest, error_messages=None, success_count=0
    ) -> tuple[str, dict[str, Any]]:
        # 获取当前时间（带时区）
        now = timezone.now()
        # 最近一个月的起始时间
        default_start_time = now - timedelta(days=30)
        start_time = request.POST.get("start_time", default_start_time.strftime("%Y-%m-%d"))
        end_time = request.POST.get("end_time", now.strftime("%Y-%m-%d"))

        status = request.POST.get("status", "record")

        # 时区感知的datetime对象
        tz_2026_01_01 = timezone.make_aware(
            datetime(2026, 1, 1, 0, 0, 0),
            timezone.get_current_timezone()
        )

        # 初始化查询条件
        criteria = Q(
            arrived_at__isnull=False,
            shipped_at__gte=tz_2026_01_01,
            shipment_schduled_at__gte=tz_2026_01_01,
            fleet_number__fleet_type__in=['LTL', '客户自提']
        )

        # 已录入/未录入筛选
        if status != "record":
            criteria &= Q(fleet_number__fleet_cost__isnull=False)
            shipment_type = '已录入内容'
        else:
            criteria &= Q(fleet_number__fleet_cost__isnull=True)
            shipment_type = '未录入内容'

        # 出库时间过滤逻辑（保留原有）
        if start_time or end_time:
            start_datetime = None
            end_datetime = None

            if start_time:
                naive_start = datetime.strptime(start_time, "%Y-%m-%d")
                start_datetime = timezone.make_aware(naive_start, timezone.get_current_timezone())

            if end_time:
                naive_end = datetime.strptime(end_time, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
                end_datetime = timezone.make_aware(naive_end, timezone.get_current_timezone())

            # 过滤Order
            order_filter = Q()
            if start_datetime:
                # 正确关联路径：Order→Container→Pallet→ShipmentBatchNumber→Shipment
                order_filter &= Q(
                    container_number__pallet__shipment_batch_number__shipped_at__gte=start_datetime
                )
            if end_datetime:
                order_filter &= Q(
                    container_number__pallet__shipment_batch_number__shipped_at__lte=end_datetime
                )

            # 获取Container ID
            container_ids = await sync_to_async(list)(
                Order.objects.filter(order_filter)
                .values_list('container_number_id', flat=True)
                .distinct()
            )

            # 获取Pallet ID
            pallet_ids = []
            if container_ids:
                pallet_ids = await sync_to_async(list)(
                    Pallet.objects.filter(container_number_id__in=container_ids)
                    .values_list('id', flat=True)
                    .distinct()
                )

            # 获取Shipment ID
            shipment_ids = []
            if pallet_ids:
                shipment_ids = await sync_to_async(list)(
                    Pallet.objects.filter(id__in=pallet_ids)
                    .values_list('shipment_batch_number', flat=True)
                    .distinct()
                )

            # 强制筛选
            if start_time or end_time:
                if shipment_ids:
                    criteria &= Q(id__in=shipment_ids)
                else:
                    criteria &= Q(id__in=[])

        # 核心修改：新增按核实状态排序（未核实在前）
        # 核心修改：正确实现 Supplier 为空在前，有值在后
        shipment = await sync_to_async(list)(
            Shipment.objects
            .select_related("fleet_number")
            .filter(criteria)
            .order_by(
                # 1. 未核实(FALSE=0) 在上，已核实(TRUE=1) 在下
                "fleet_number__fleet_verify_status",
                # 2. 每组内部：Supplier 为空 → 最上 | 有值 → 最下
                F("fleet_number__Supplier").asc(nulls_first=True),
            )
        )

        # ========== 新增：按车次统计总板数 ==========
        @sync_to_async
        def get_fleet_total_pallets():
            """统计每个车次的总板数 + 获取 fleetshipmentpallet 最新一条费用记录（按ID最大取最新）"""
            # 先获取所有涉及的车次号
            fleet_numbers = [s.fleet_number.fleet_number for s in shipment if s.fleet_number]
            fleet_pallet_map = {}
            fleet_expense_map = {}  # 存储每个车次【最新】的 expense

            if not fleet_numbers:
                return fleet_pallet_map, fleet_expense_map

            # 查询每个车次关联的所有shipment
            fleet_shipments = Shipment.objects.filter(
                fleet_number__fleet_number__in=fleet_numbers,
                fleet_number__fleet_type='LTL'
            ).select_related("fleet_number")

            # 统计每个车次的总板数
            for fs in fleet_shipments:
                fleet_code = fs.fleet_number.fleet_number
                # 获取该车次下所有pallet数量
                pallet_count = Pallet.objects.filter(
                    shipment_batch_number=fs.id
                ).count()
                if fleet_code not in fleet_pallet_map:
                    fleet_pallet_map[fleet_code] = 0
                fleet_pallet_map[fleet_code] += pallet_count

            # ======================
            # 核心修复：按 ID 最大取最新费用（无创建时间专用）
            # ======================
            from django.db.models import Max
            # 1. 获取每个 fleet_number_id 对应的最大 ID（最新记录）
            fleet_latest = FleetShipmentPallet.objects.filter(
                fleet_number_id__in=[fs.fleet_number_id for fs in fleet_shipments if fs.fleet_number]
            ).values('fleet_number_id').annotate(
                latest_id=Max('id')  # 用 id 最大 = 最新
            )

            # 2. 取出最新记录的 expense
            for item in fleet_latest:
                fleet_id = item['fleet_number_id']
                latest_rec = FleetShipmentPallet.objects.filter(
                    fleet_number_id=fleet_id,
                    id=item['latest_id']
                ).first()

                if latest_rec:
                    # ✅ 按【车次CODE】存储，才能和前面匹配成功
                    fleet_code = next(
                        (fs.fleet_number.fleet_number for fs in fleet_shipments if fs.fleet_number_id == fleet_id),
                        None)
                    if fleet_code:
                        fleet_expense_map[fleet_code] = latest_rec.expense or 0

            return fleet_pallet_map, fleet_expense_map

        # 获取车次总板数字典
        fleet_total_pallets, fleet_expense_map = await get_fleet_total_pallets()

        # ========== 修复：一提多卸分组和总成本计算（核心修改） ==========
        @sync_to_async
        def get_multi_unload_groups():
            """
            【已修复】
            一提多卸分组规则：
            Pallet 表中 ltl_correlation_id 相同 → 视为同一组
            """
            # 步骤1：获取所有有 ltl_correlation_id 的 Pallet（不为空、不为空字符串）
            pallets = Pallet.objects.filter(
                ltl_correlation_id__isnull=False
            ).exclude(ltl_correlation_id="")

            # 步骤2：按 ltl_correlation_id 分组（相同值 = 同一提多卸组）
            group_map = {}  # ltl_correlation_id → 组信息
            for pallet in pallets:
                group_id = pallet.ltl_correlation_id
                if not group_id:
                    continue

                # 获取该托盘对应的 ShipmentBatchNumber
                shipment_id = pallet.shipment_batch_number_id
                if not shipment_id:
                    continue

                # 获取该 shipment 对应的 车次
                shipment_obj = Shipment.objects.filter(id=shipment_id).first()
                if not shipment_obj or not shipment_obj.fleet_number:
                    continue

                fleet_number = shipment_obj.fleet_number.fleet_number
                fleet_cost = shipment_obj.fleet_number.fleet_cost or 0

                # 初始化分组
                if group_id not in group_map:
                    group_map[group_id] = {
                        "total_cost": 0,
                        "fleet_numbers": []
                    }

                # 累加成本 & 加入车次
                group = group_map[group_id]
                if fleet_number not in group["fleet_numbers"]:
                    group["fleet_numbers"].append(fleet_number)
                    group["total_cost"] += fleet_cost

            # 步骤3：构建前端需要的映射
            fleet_to_group_map = {}
            multi_unload_map = {}

            for group_id, group_info in group_map.items():
                fleet_numbers = group_info["fleet_numbers"]
                total_cost = group_info["total_cost"]

                # 每个车次都指向同组所有车次
                for fn in fleet_numbers:
                    multi_unload_map[fn] = fleet_numbers
                    fleet_to_group_map[fn] = {
                        "group_id": group_id,
                        "total_cost": total_cost,
                        "member_ids": []  # 不需要ID，保持兼容即可
                    }

            return {
                "group_map": group_map,
                "fleet_map": fleet_to_group_map,
                "multi_unload_map": multi_unload_map
            }

        # 调用函数并获取正确格式的 multi_unload_map
        multi_unload_data = await get_multi_unload_groups()
        # 替换原有赋值（关键！）
        multi_unload_map = multi_unload_data['multi_unload_map']  # 前端用的车次→同组列表
        fleet_to_group_map = multi_unload_data['fleet_map']  # 原有车次→分组信息

        # ========== 移除重复的 multi_unload_data 赋值 ==========

        # 处理数据：核心修改 - 修复一提多卸信息挂载
        async def process_shipment_data(shipment_list):
            processed = []
            for s in shipment_list:
                # 核心修改1：查询当前shipment关联的所有pallet（修复字典取值问题）
                @sync_to_async
                def get_related_pallets(shipment_id):
                    """查询关联的pallet数据（包含location），并按柜号+唛头分组"""
                    # 方案1：不使用.values()，直接获取模型实例（推荐，避免字典取值错误）
                    pallets = list(Pallet.objects.filter(
                        shipment_batch_number=shipment_id
                    ).select_related("container_number"))  # 移除.values()，返回模型实例

                    # 提取Container ID用于查Order
                    # 修复：模型实例用属性访问 .container_number_id
                    container_ids = [p.container_number_id for p in pallets if p.container_number_id]
                    order_map = {}
                    if container_ids:
                        orders = Order.objects.filter(
                            container_number_id__in=container_ids
                        ).select_related("offload_id")
                        for order in orders:
                            order_map[order.container_number_id] = order

                    # 按 柜号 + 唛头 分组
                    # 修复：模型实例用属性访问
                    group_key = lambda p: (
                        p.container_number.container_number if p.container_number else "",
                        p.shipping_mark or ""
                    )

                    # 初始化分组字典
                    pallet_groups = {}

                    for pallet in pallets:
                        key = group_key(pallet)
                        if key not in pallet_groups:
                            pallet_groups[key] = {
                                'pallets': [],
                                'total_pallet_count': 0,
                                'total_pcs_count': 0,
                                'container_number': key[0] or "-",
                                'shipping_mark': key[1] or "-",
                                'offload_at': None,
                                'retrieval_time': None,
                                'locations': set(),
                            }

                        # 累加数量（修复：模型实例用属性访问）
                        pallet_groups[key]['total_pallet_count'] += 1
                        pallet_groups[key]['total_pcs_count'] += pallet.pcs or 0
                        pallet_groups[key]['pallets'].append(pallet)

                        # 新增：收集location（去重，修复：模型实例用属性访问）
                        if pallet.location:
                            pallet_groups[key]['locations'].add(pallet.location)

                        # 补充提柜时间（取第一个有效时间，修复：模型实例用属性访问）
                        if pallet.container_number_id and not pallet_groups[key]['offload_at']:
                            order = order_map.get(pallet.container_number_id)
                            if order and order.offload_id:
                                pallet_groups[key]['offload_at'] = order.offload_id.offload_at
                                pallet_groups[key]['retrieval_time'] = order.offload_id.offload_at

                    # 将locations集合转为列表，方便前端展示
                    for group in pallet_groups.values():
                        group['locations'] = list(group['locations']) if group['locations'] else ["-"]

                    return pallet_groups

                # 获取按柜号+唛头分组的pallet数据（包含location）
                pallet_groups = await get_related_pallets(s.id)

                # 核心修改2：每个分组生成独立的发货单条目（包含location信息）
                for group_key, group_data in pallet_groups.items():
                    # 复制原shipment对象（避免引用同一对象）
                    shipment_copy = copy.deepcopy(s)

                    # 挂载分组相关数据
                    shipment_copy.related_pallet = group_data['pallets']
                    shipment_copy.shipped_pcs = group_data['total_pcs_count'] or 0
                    shipment_copy.shipped_pallet = group_data['total_pallet_count'] or 0
                    shipment_copy.container_number = group_data['container_number']
                    shipment_copy.shipping_mark = group_data['shipping_mark']
                    shipment_copy.offload_at = group_data['offload_at']
                    shipment_copy.retrieval_time = group_data['retrieval_time']
                    shipment_copy.all_locations = group_data['locations'][0]  # 该分组所有location（去重）

                    # 挂载车次号
                    shipment_copy.fleet_number_code = shipment_copy.fleet_number.fleet_number if (
                            shipment_copy.fleet_number and shipment_copy.fleet_number.fleet_number) else "-"

                    # ========== 修复：分摊价格逻辑（KEY FIX） ==========
                    # 1. 获取当前车次总板数
                    fleet_code = shipment_copy.fleet_number_code
                    total_pallets_of_fleet = fleet_total_pallets.get(fleet_code, 0)

                    # 2. 当前柜子板数
                    current_pallet_count = shipment_copy.shipped_pallet

                    # 3. ✅ 修复：优先取最新费用表，没有再取fleet_cost
                    fleet_total_cost = fleet_expense_map.get(
                        shipment_copy.fleet_number_id,  # 按 fleet ID 取最新费用
                        shipment_copy.fleet_number.fleet_cost or 0
                    )

                    # 4. ✅ 修复：只要有成本就计算，不再判断 fleet_total_cost>0
                    if total_pallets_of_fleet > 0 and current_pallet_count > 0:
                        allocation_price = (fleet_total_cost / total_pallets_of_fleet) * current_pallet_count
                        shipment_copy.allocation_price = round(allocation_price, 2)
                    else:
                        # 兜底显示真实成本，不直接归 0
                        shipment_copy.allocation_price = round(fleet_total_cost, 2) if fleet_total_cost else 0

                    # ========== ✅ 最终修复：按 Pallet.ltl_correlation_id 判断一提多卸 ==========
                    is_multi_unload = False
                    multi_unload_total_cost = 0
                    multi_unload_group_id = ""
                    multi_unload_member_ids = []

                    # 判断当前车次是否在一提多卸分组中
                    fleet_num = shipment_copy.fleet_number_code
                    if fleet_num in fleet_to_group_map:
                        is_multi_unload = True
                        group_info = fleet_to_group_map[fleet_num]
                        multi_unload_total_cost = group_info['total_cost']
                        multi_unload_group_id = group_info['group_id']
                        multi_unload_member_ids = group_info['member_ids']

                    # 挂载到对象
                    shipment_copy.is_multi_unload = is_multi_unload
                    shipment_copy.multi_unload_total_cost = multi_unload_total_cost
                    shipment_copy.multi_unload_group_id = multi_unload_group_id
                    shipment_copy.multi_unload_member_ids = multi_unload_member_ids

                    processed.append(shipment_copy)

                # 兼容没有pallet的情况
                if not pallet_groups:
                    # 创建空条目
                    shipment_copy = copy.deepcopy(s)
                    shipment_copy.related_pallet = []
                    shipment_copy.shipped_pcs = 0
                    shipment_copy.shipped_pallet = 0
                    shipment_copy.container_number = "-"
                    shipment_copy.shipping_mark = "-"
                    shipment_copy.offload_at = None
                    shipment_copy.retrieval_time = None
                    shipment_copy.all_locations = ["-"]
                    # 成本录入信息
                    shipment_copy.fleet_number_code = shipment_copy.fleet_number.fleet_number if (
                            shipment_copy.fleet_number and shipment_copy.fleet_number.fleet_number) else "-"
                    # ========== 新增：空条目分摊价格默认0 ==========
                    shipment_copy.allocation_price = 0

                    # ========== 修复：空条目一提多卸信息 ==========
                    is_multi_unload = False
                    fleet_num = shipment_copy.fleet_number_code
                    if fleet_num in fleet_to_group_map:
                        is_multi_unload = True

                    shipment_copy.is_multi_unload = is_multi_unload
                    shipment_copy.multi_unload_total_cost = 0
                    shipment_copy.multi_unload_group_id = ""
                    shipment_copy.multi_unload_member_ids = []

                    processed.append(shipment_copy)

            return processed

        # 处理数据
        shipment = await process_shipment_data(shipment)

        # 构建上下文
        context = {
            "shipment_type": shipment_type,
            "start_time": start_time,
            "end_time": end_time,
            "fleet": shipment,
            "upload_file_form": UploadFileForm(required=True),
            "warehouse_options": self.warehouse_options,
            "error_messages": error_messages or [],
            "success_count": success_count,
            # 新增：传递一提多卸分组数据到前端
            "multi_unload_map": multi_unload_map,
            "fleet_to_group_map": fleet_to_group_map,  # 新增：车次号→分组映射
        }
        return self.template_fleet_cost_record_ltl, context

    async def handle_bind_multi_unload(self, request: HttpRequest):
        """绑定一提多卸（批量更新 Pallet.ltl_correlation_id 字段）"""
        logger = logging.getLogger(__name__)
        try:
            # 获取选中的车次号
            fleet_numbers = request.POST.get('selected_fleet_numbers', '').split(',')
            fleet_numbers = [f.strip() for f in fleet_numbers if f.strip()]

            if not fleet_numbers or len(fleet_numbers) < 2:
                error_messages = ['请至少选择2个车次进行绑定']
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

            # 生成随机唯一标识
            correlation_id = str(uuid.uuid4())[:8]

            # 批量更新 Pallet 的 ltl_correlation_id
            @sync_to_async
            def update_pallet_correlation():
                with transaction.atomic():
                    # 1. 找到这些车次对应的 Shipment
                    shipment_ids = list(Shipment.objects.filter(
                        fleet_number__fleet_number__in=fleet_numbers
                    ).values_list('id', flat=True))

                    if not shipment_ids:
                        return 0

                    # 2. 更新这些 Shipment 下所有 Pallet 的 ltl_correlation_id
                    updated_count = Pallet.objects.filter(
                        shipment_batch_number__in=shipment_ids
                    ).update(ltl_correlation_id=correlation_id)

                    return updated_count

            updated_count = await update_pallet_correlation()

            error_messages = []
            if updated_count == 0:
                error_messages.append('绑定失败：未找到任何托盘数据')
            else:
                error_messages.append(f'绑定成功：共更新 {updated_count} 个托盘数据')

            return await self.handle_fleet_cost_record_get_ltl(
                request, error_messages, updated_count
            )
        except Exception as e:
            logger.error('绑定一提多卸失败', exc_info=True)
            error_messages = [f'绑定失败：{str(e)[:200]}']
            return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

    async def handle_unbind_multi_unload(self, request: HttpRequest):
        """解绑一提多卸（清空 Pallet.ltl_correlation_id）"""
        logger = logging.getLogger(__name__)
        try:
            # 获取选中的车次号
            fleet_numbers = request.POST.get('selected_fleet_numbers', '').split(',')
            fleet_numbers = [f.strip() for f in fleet_numbers if f.strip()]

            if not fleet_numbers:
                error_messages = ['请选择需要解绑的车次']
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

            # 批量清空 Pallet 的 ltl_correlation_id
            @sync_to_async
            def clear_pallet_correlation():
                with transaction.atomic():
                    # 1. 找到这些车次对应的 Shipment
                    shipment_ids = list(Shipment.objects.filter(
                        fleet_number__fleet_number__in=fleet_numbers
                    ).values_list('id', flat=True))

                    if not shipment_ids:
                        return 0

                    # 2. 清空这些 Shipment 下所有 Pallet 的 ltl_correlation_id
                    updated_count = Pallet.objects.filter(
                        shipment_batch_number__in=shipment_ids
                    ).update(ltl_correlation_id=None)

                    return updated_count

            updated_count = await clear_pallet_correlation()

            error_messages = []
            if updated_count == 0:
                error_messages.append('解绑失败：未找到任何托盘数据')
            else:
                error_messages.append(f'解绑成功：共清空 {updated_count} 个托盘数据')

            return await self.handle_fleet_cost_record_get_ltl(
                request, error_messages, updated_count
            )
        except Exception as e:
            logger.error('解绑一提多卸失败', exc_info=True)
            error_messages = [f'解绑失败：{str(e)[:200]}']
            return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)


    async def handle_batch_confirm_ltl_price(self, request):
        """批量确认未录入价格的核心逻辑（接收每行的价格）"""
        error_messages = []
        success_count = 0

        try:
            # 1. 获取并解析批量价格数据（修复：添加JSON解析）
            batch_prices_str = request.POST.get("batch_prices_data", "[]")
            try:
                # 解析JSON字符串为列表
                batch_prices = json.loads(batch_prices_str)
                # 验证是否为列表
                if not isinstance(batch_prices, list):
                    error_messages.append("批量价格数据格式错误：不是列表类型")
                    batch_prices = []
            except json.JSONDecodeError as e:
                error_messages.append(f"批量价格数据解析失败：{str(e)}")
                batch_prices = []

            # 2. 遍历每条数据，更新价格
            for idx, item in enumerate(batch_prices):
                try:
                    # 更严格的字段验证
                    fleet_number = item.get("fleet_number")
                    price = item.get("fleet_cost")

                    if not fleet_number or fleet_number.strip() == '':
                        error_messages.append(f"第{idx + 1}条数据：车次号为空")
                        continue


                    # 转换价格为浮点数
                    try:
                        price_float = float(price)
                    except (ValueError, TypeError):
                        error_messages.append(f"第{idx + 1}条数据（车次：{fleet_number}）：价格格式错误「{price}」")
                        continue

                    if price_float < 0:
                        error_messages.append(f"第{idx + 1}条数据（车次：{fleet_number}）：价格不能为负数「{price_float}」")
                        continue

                    # 3. 更新数据库记录
                    try:
                        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
                        fleet.fleet_cost = price_float
                        await sync_to_async(fleet.save)()
                        success_count += 1
                    except Fleet.DoesNotExist:
                        error_messages.append(f"第{idx + 1}条数据：车次号「{fleet_number}」不存在")
                    except Exception as e:
                        error_messages.append(f"第{idx + 1}条数据（车次：{fleet_number}）：处理失败「{str(e)}」")

                except Exception as e:
                    error_messages.append(f"处理第{idx + 1}条数据时发生未知错误：{str(e)}")

        except Exception as e:
            error_messages.append(f"批量处理主逻辑异常：{str(e)}")

        # 4. 返回结果页面
        return await self.handle_fleet_cost_record_get_ltl(
            request, error_messages=error_messages, success_count=success_count
        )

    async def handle_batch_confirm_ltl_note(self, request):
        """批量确认备注"""
        error_messages = []
        success_count = 0

        try:
            # 1. 获取并解析批量价格数据（修复：添加JSON解析）
            batch_notes_str = request.POST.get("batch_notes_data", "[]")
            try:
                # 解析JSON字符串为列表
                batch_notes = json.loads(batch_notes_str)
                # 验证是否为列表
                if not isinstance(batch_notes, list):
                    error_messages.append("批量价格数据格式错误：不是列表类型")
                    batch_notes = []
            except json.JSONDecodeError as e:
                error_messages.append(f"批量价格数据解析失败：{str(e)}")
                batch_notes = []

            # 2. 遍历每条数据，更新价格
            for idx, item in enumerate(batch_notes):
                try:
                    # 更严格的字段验证
                    fleet_number = item.get("fleet_number")
                    note = item.get("note")

                    if not fleet_number or fleet_number.strip() == '':
                        error_messages.append(f"第{idx + 1}条数据：车次号为空")
                        continue

                    # 3. 更新数据库记录
                    try:
                        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
                        shipment = await sync_to_async(Shipment.objects.get)(fleet_number=fleet.id)
                        shipment.note = note
                        await sync_to_async(shipment.save)()
                        success_count += 1
                    except Fleet.DoesNotExist:
                        error_messages.append(f"第{idx + 1}条数据：车次号「{fleet_number}」不存在")
                    except Exception as e:
                        error_messages.append(f"第{idx + 1}条数据（车次：{fleet_number}）：处理失败「{str(e)}」")

                except Exception as e:
                    error_messages.append(f"处理第{idx + 1}条数据时发生未知错误：{str(e)}")

        except Exception as e:
            error_messages.append(f"批量处理主逻辑异常：{str(e)}")

        # 4. 返回结果页面
        return await self.handle_fleet_cost_record_get_ltl(
            request, error_messages=error_messages, success_count=success_count
        )

    async def handle_batch_confirm_ltl_supplier(self, request):
        """批量确认供应商"""
        error_messages = []
        success_count = 0

        try:
            # 1. 获取并解析批量价格数据（修复：添加JSON解析）
            batch_notes_str = request.POST.get("batch_suppliers_data", "[]")
            try:
                # 解析JSON字符串为列表
                batch_notes = json.loads(batch_notes_str)
                # 验证是否为列表
                if not isinstance(batch_notes, list):
                    error_messages.append("批量价格数据格式错误：不是列表类型")
                    batch_notes = []
            except json.JSONDecodeError as e:
                error_messages.append(f"批量价格数据解析失败：{str(e)}")
                batch_notes = []

            # 2. 遍历每条数据，更新价格
            for idx, item in enumerate(batch_notes):
                try:
                    # 更严格的字段验证
                    fleet_number = item.get("fleet_number")
                    supplier = item.get("supplier")

                    if not fleet_number or fleet_number.strip() == '':
                        error_messages.append(f"第{idx + 1}条数据：车次号为空")
                        continue

                    # 3. 更新数据库记录
                    try:
                        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
                        fleet.Supplier = supplier
                        await sync_to_async(fleet.save)()
                        success_count += 1
                    except Fleet.DoesNotExist:
                        error_messages.append(f"第{idx + 1}条数据：车次号「{fleet_number}」不存在")
                    except Exception as e:
                        error_messages.append(f"第{idx + 1}条数据（车次：{fleet_number}）：处理失败「{str(e)}」")

                except Exception as e:
                    error_messages.append(f"处理第{idx + 1}条数据时发生未知错误：{str(e)}")

        except Exception as e:
            error_messages.append(f"批量处理主逻辑异常：{str(e)}")

        # 4. 返回结果页面
        return await self.handle_fleet_cost_record_get_ltl(
            request, error_messages=error_messages, success_count=success_count
        )



    async def handle_update_fleet_supplier(self, request):
        """确认供应商"""
        fleet_id = request.POST.get("fleet_number")
        supplier = request.POST.get("supplier", "").strip()
        fleet = await sync_to_async(Fleet.objects.get)(id=fleet_id)
        fleet.Supplier = supplier
        await sync_to_async(fleet.save)()

        success_count = 1
        return await self.handle_fleet_cost_record_get_ltl(
            request, error_messages=[], success_count=success_count
        )

    async def handle_batch_allocate_ltl_cost(self, request: HttpRequest):
        """
        LTL批量分摊成本核心逻辑（修复浮点数精度问题）：
        1. 总成本按选中记录的总板数分摊到每个车次（每车次成本 = 总成本/总板数 × 该车次板数）
        2. 更新Fleet表的fleet_cost字段为分摊后的成本
        3. 调用已有insert_fleet_shipment_pallet_fleet_cost方法，完成车次成本的二次分摊
        """
        logger = logging.getLogger(__name__)
        error_messages = []
        success_count = 0

        try:
            # 1. 获取前端参数
            total_batch_cost = request.POST.get('total_batch_cost')
            selected_fleet_numbers = request.POST.get('selected_fleet_numbers', '').split(',')
            area = request.POST.get('area', '')

            # 2. 基础参数校验（改用Decimal处理金额）
            if not total_batch_cost:
                error_messages.append('请输入总成本总值')
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

            try:
                # 关键：改用Decimal存储金额，避免浮点数误差
                total_batch_cost = Decimal(total_batch_cost).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
                if total_batch_cost <= 0:
                    error_messages.append('总成本必须大于0')
                    return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)
            except (ValueError, Decimal.InvalidOperation):
                error_messages.append('总成本格式错误，必须为数字')
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

            # 过滤空值
            selected_fleet_numbers = [fleet_num.strip() for fleet_num in selected_fleet_numbers if fleet_num.strip()]
            if not selected_fleet_numbers:
                error_messages.append('请至少选择一条记录进行分摊')
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

            # 3. 批量查询选中记录的板数和车次信息
            @sync_to_async
            def get_selected_fleet_data(fleet_numbers):
                fleet_data = []
                # Step1: 先查询符合条件的Shipment
                shipments = Shipment.objects.filter(
                    fleet_number__fleet_number__in=fleet_numbers
                ).values_list('id', 'fleet_number__fleet_number')
                shipment_fleet_map = {shipment_id: fleet_num for shipment_id, fleet_num in shipments}

                if not shipment_fleet_map:
                    return fleet_data

                # Step2: 查询这些Shipment关联的所有Pallet
                pallets = Pallet.objects.filter(
                    shipment_batch_number__in=shipment_fleet_map.keys()
                ).select_related('container_number')

                # Step3: 按车次号分组，计算每个车次的总板数
                fleet_group = {}
                for pallet in pallets:
                    fleet_num = shipment_fleet_map.get(pallet.shipment_batch_number_id)
                    if not fleet_num:
                        continue

                    container_num = pallet.container_number.container_number if pallet.container_number else '未知柜号'

                    if fleet_num not in fleet_group:
                        fleet_group[fleet_num] = {
                            'fleet_number': fleet_num,
                            'total_pallet': 0,
                            'container_numbers': set()
                        }

                    fleet_group[fleet_num]['total_pallet'] += 1
                    fleet_group[fleet_num]['container_numbers'].add(container_num)

                # 转换为列表
                for fleet_num, data in fleet_group.items():
                    fleet_data.append({
                        'fleet_number': fleet_num,
                        'total_pallet': data['total_pallet'],
                        'container_numbers': ', '.join(data['container_numbers'])
                    })
                return fleet_data

            fleet_data = await get_selected_fleet_data(selected_fleet_numbers)
            if not fleet_data:
                error_messages.append('未找到选中的车次数据')
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

            # 4. 计算所有选中车次的总板数
            total_pallets = sum([item['total_pallet'] for item in fleet_data])
            if total_pallets <= 0:
                error_messages.append('选中记录的总卡板数为0，无法分摊成本')
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

            # ========== 核心修复1：改用Decimal计算，避免浮点数误差 ==========
            # 5. 计算每板基础成本（不提前四舍五入，保留高精度）
            cost_per_pallet = total_batch_cost / Decimal(total_pallets)

            # 6. 批量处理：更新Fleet表 + 调用insert方法
            async def batch_call_insert_method(fleet_data, cost_per_pallet, total_batch_cost, request):
                processed_count = 0
                errors = []

                @sync_to_async
                def update_fleet_cost_sync(fleet_data, cost_per_pallet, total_batch_cost):
                    tasks = []
                    with transaction.atomic():
                        # 第一步：计算所有车次的分摊成本（先不四舍五入）
                        fleet_cost_list = []
                        total_allocated = Decimal('0.00')
                        for item in fleet_data:
                            fleet_num = item['fleet_number']
                            fleet_pallet_count = item['total_pallet']
                            container_numbers = item['container_numbers']

                            if fleet_pallet_count <= 0:
                                errors.append(f'车次{fleet_num}（柜号：{container_numbers}）板数为0，跳过处理')
                                continue

                            # 计算该车次分摊成本（高精度，不四舍五入）
                            fleet_cost = cost_per_pallet * Decimal(fleet_pallet_count)
                            fleet_cost_list.append({
                                'fleet_number': fleet_num,
                                'fleet_cost': fleet_cost,
                                'container_numbers': container_numbers,
                                'pallet_count': fleet_pallet_count
                            })
                            total_allocated += fleet_cost

                        # ========== 核心修复2：最后一个车次兜底凑整，保证总成本一致 ==========
                        # 计算误差（总分摊成本 vs 原始总成本）
                        cost_diff = total_batch_cost - total_allocated.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
                        if fleet_cost_list and cost_diff != 0:
                            # 把误差加到最后一个车次上（也可以平均分配，根据业务选择）
                            last_fleet = fleet_cost_list[-1]
                            last_fleet['fleet_cost'] += cost_diff
                            logger.info(f'分摊误差{cost_diff}元，已加到最后一个车次{last_fleet["fleet_number"]}')

                        # 第二步：更新Fleet表（四舍五入到2位小数）
                        for item in fleet_cost_list:
                            fleet_num = item['fleet_number']
                            fleet_cost = item['fleet_cost'].quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
                            container_numbers = item['container_numbers']

                            try:
                                fleet_obj = Fleet.objects.get(fleet_number=fleet_num)
                                fleet_obj.fleet_cost = float(fleet_cost)  # 转float存入数据库（若字段是FloatField）
                                # 若fleet_cost是DecimalField，直接存：fleet_obj.fleet_cost = fleet_cost
                                fleet_obj.save(update_fields=['fleet_cost'])

                                logger.info(f'车次{fleet_num}的fleet_cost字段已更新为：{fleet_cost}元')

                                # 收集insert方法的任务参数
                                tasks.append({
                                    'fleet_number': fleet_num,
                                    'fleet_total_cost': float(fleet_cost),
                                    'container_numbers': container_numbers,
                                    'pallet_count': item['pallet_count']
                                })
                            except ObjectDoesNotExist:
                                errors.append(f'车次{fleet_num}（柜号：{container_numbers}）不存在，无法更新fleet_cost')
                            except Exception as e:
                                errors.append(f'车次{fleet_num}（柜号：{container_numbers}）更新失败：{str(e)[:50]}')

                    return tasks

                # 执行同步更新
                tasks = await update_fleet_cost_sync(fleet_data, cost_per_pallet, total_batch_cost)

                # 调用insert方法
                for task in tasks:
                    try:
                        await self.insert_fleet_shipment_pallet_fleet_cost(
                            request=request,
                            fleet_number=task['fleet_number'],
                            fleet_cost=task['fleet_total_cost']
                        )
                        processed_count += 1
                        logger.info(f'车次{task["fleet_number"]}二次分摊完成')
                    except Exception as e:
                        errors.append(f'车次{task["fleet_number"]}二次分摊失败：{str(e)[:50]}')

                return processed_count, errors

            # 执行批量处理（传入total_batch_cost用于凑整）
            success_count, process_errors = await batch_call_insert_method(
                fleet_data, cost_per_pallet, total_batch_cost, request
            )
            error_messages.extend(process_errors)

            # 日志记录
            logger.info(
                f'LTL批量分摊完成：总成本{total_batch_cost}元，总板数{total_pallets}，'
                f'成功处理{success_count}个车次，失败{len(process_errors)}个'
            )

        except Exception as e:
            logger.error('LTL批量分摊整体异常', exc_info=True)
            error_messages.append(f'批量分摊失败：{str(e)[:200]}')

        # 返回结果页面
        return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

    async def handle_pod_upload_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.GET.get("fleet_number", "")
        batch_number = request.GET.get("batch_number", "")

        area = request.POST.get("area") or None
        arrived_at = request.POST.get("arrived_at")

        criteria = models.Q(
            models.Q(models.Q(pod_link__isnull=True) | models.Q(pod_link="")),
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_schduled_at__gte="2024-12-01",
        )
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        if area and area is not None and area != "None":
            criteria &= models.Q(origin=area)
        if (
            arrived_at
            and arrived_at is not None
            and arrived_at != ""
            and arrived_at != "None"
        ):
            arrived_at = datetime.strptime(arrived_at, "%Y-%m-%d")
            criteria &= models.Q(
                arrived_at__year=arrived_at.year,
                arrived_at__month=arrived_at.month,
                arrived_at__day=arrived_at.day,
            )
            arrived_at = arrived_at.strftime("%Y-%m-%d")
        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        context = {
            "fleet_number": fleet_number,
            "batch_number": batch_number,
            "fleet": shipment,
            "upload_file_form": UploadFileForm(required=True),
            "warehouse_options": self.warehouse_options,
            "area": area,
            "arrived_at": arrived_at,
        }
        return self.template_pod_upload, context

    async def handle_download_ltl_template(self, request: HttpRequest):
        """ltl下载模板"""
        # 创建LTL模板，表头为：柜号、目的地、唛头、成本
        df = pd.DataFrame(columns=['柜号', '目的地', '唛头', '成本', '备注'])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='LTL成本模板', index=False)
        # 重置指针
        output.seek(0)
        response = HttpResponse(output,
                                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="LTL成本录入模板.xlsx"'
        return response


    async def handle_upload_ltl_cost(self, request: HttpRequest):
        """LTL批量上传成本Excel（修复：支持同柜号不同唛头独立上传 + 仅fleet_verify_status=False时覆盖成本）"""
        logger = logging.getLogger(__name__)
        # 1. 校验文件是否存在
        if not request.FILES.get('ltl_file'):
            error_messages = ['未上传文件']
            success_count = 0
            return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

        file = request.FILES['ltl_file']

        try:
            # 2. 读取Excel文件（处理常见格式问题）
            df = pd.read_excel(file, dtype={'柜号': str, '成本': str})  # 先以字符串读取，避免自动转换
            df = df.fillna('')  # 空值替换为空字符串

            # 3. 校验表头
            required_columns = ['柜号', '目的地', '唛头', '成本', '备注']
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                error_messages = [
                    f'上传文件表头错误，缺少字段：{", ".join(missing_cols)}（必须包含：柜号、目的地、唛头、成本、备注）'
                ]
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

            success_count = 0
            error_messages = []
            # ========== 修复1：去重逻辑改为「柜号+唛头」联合去重 ==========
            processed_container_mark = set()  # 记录已处理的（柜号+唛头）组合

            # 4. 批量提取有效数据（先过滤无效行）
            valid_rows = []
            for idx, row in df.iterrows():
                row_num = idx + 2  # Excel行号（从2开始）
                container_number = str(row['柜号']).strip()
                fleet_cost_str = str(row['成本']).strip()
                destination = str(row['目的地']).strip()
                shipping_mark = str(row['唛头']).strip()
                note = str(row['备注']).strip()

                # 4.1 校验必填字段
                if not container_number:
                    error_messages.append(f'第{row_num}行：柜号不能为空')
                    continue

                if not fleet_cost_str:
                    error_messages.append(f'第{row_num}行：成本不能为空')
                    continue

                if not shipping_mark:
                    error_messages.append(f'第{row_num}行：唛头不能为空')
                    continue

                # 4.2 校验成本格式（数字/非负数）
                try:
                    fleet_cost = float(fleet_cost_str)
                    if fleet_cost < 0:
                        error_messages.append(f'第{row_num}行：成本不能为负数（当前值：{fleet_cost_str}）')
                        continue
                except ValueError:
                    error_messages.append(f'第{row_num}行：成本格式错误，必须为数字（当前值：{fleet_cost_str}）')
                    continue

                # ========== 修复1核心：按「柜号+唛头」去重 ==========
                container_mark_key = f"{container_number}_{shipping_mark}"
                if container_mark_key in processed_container_mark:
                    logger.warning(f'第{row_num}行：柜号{container_number} + 唛头{shipping_mark} 重复，已跳过')
                    continue
                processed_container_mark.add(container_mark_key)

                valid_rows.append({
                    'row_num': row_num,
                    'container_number': container_number,
                    'fleet_cost': fleet_cost,
                    'destination': destination,
                    'shipping_mark': shipping_mark,
                    'note': note
                })

            if not valid_rows:
                error_messages = error_messages or ['无有效数据可处理']
                return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

            # 5. 批量查询数据库（减少数据库交互）
            # 提取所有柜号和唛头
            container_numbers = [row['container_number'] for row in valid_rows]
            shipping_marks = [row['shipping_mark'] for row in valid_rows]

            # ========== 修复2：重构映射逻辑，按「柜号+唛头」获取Fleet ID ==========
            @sync_to_async
            def get_fleet_id_mapping(container_nums, shipping_marks):
                """批量获取（柜号+唛头）对应的Fleet ID + Shipment原有备注"""
                fleet_mapping = {}  # key: 柜号+唛头, value: (fleet_id, shipment_note)
                # 关联查询Pallet表 + Shipment表，获取（柜号+唛头）对应的Fleet ID和原有备注
                queryset = Pallet.objects.select_related(
                    'container_number',
                    'shipment_batch_number',
                    'shipment'  # 关联Shipment表
                ).filter(
                    container_number__container_number__in=container_nums,
                    shipping_mark__in=shipping_marks
                ).values(
                    'container_number__container_number',
                    'shipping_mark',
                    'shipment_batch_number__fleet_number',  # Fleet表的ID
                    'shipment_batch_number__note'  # Shipment表的原有备注
                )

                for item in queryset:
                    container_num = item['container_number__container_number']
                    shipping_mark = item['shipping_mark']
                    fleet_id = item['shipment_batch_number__fleet_number']
                    shipment_note = item['shipment_batch_number__note'] or ''  # 原有备注（空值转空字符串）

                    # 按「柜号+唛头」作为key存储，确保不同唛头有独立映射
                    key = f"{container_num}_{shipping_mark}"
                    # 仅当Fleet ID存在时才记录，且保留第一个匹配项
                    if fleet_id and key not in fleet_mapping:
                        fleet_mapping[key] = (fleet_id, shipment_note)  # 新增：返回fleet_id + 原有备注
                return fleet_mapping

            # 获取（柜号+唛头）→Fleet ID的映射
            fleet_id_mapping = await get_fleet_id_mapping(container_numbers, shipping_marks)

            # 批量查询所有需要的Fleet数据（新增：包含fleet_verify_status字段）
            @sync_to_async
            def get_fleet_info_by_ids(fleet_ids):
                """批量查询Fleet表，获取ID→(车次号, 验证状态)的映射"""
                fleet_info = {}
                if not fleet_ids:
                    return fleet_info

                # 核心修复：兼容元组/整数两种类型，提取车次ID并转整数
                fleet_ids_int = []
                for item in fleet_ids:
                    try:
                        # 情况1：item是元组 → 取第一个元素
                        if isinstance(item, (tuple, list)):
                            fid = int(item[0]) if item and item[0] else None
                        # 情况2：item是整数/字符串 → 直接转整数
                        else:
                            fid = int(item) if item else None

                        if fid is not None:
                            fleet_ids_int.append(fid)
                    except (ValueError, TypeError):
                        logger.warning(f"无效的车次ID：{item}，已跳过")
                        continue

                # 去重（避免重复查询数据库）
                fleet_ids_int = list(set(fleet_ids_int))

                if not fleet_ids_int:
                    return fleet_info

                # 批量查询Fleet表
                queryset = Fleet.objects.filter(id__in=fleet_ids_int).values('id', 'fleet_number',
                                                                             'fleet_verify_status')
                for item in queryset:
                    fleet_info[item['id']] = {
                        'fleet_number': item['fleet_number'],
                        'fleet_verify_status': item['fleet_verify_status']
                    }
                return fleet_info

            # 提取所有Fleet ID并批量查询车次号+验证状态
            fleet_ids = [v[0] for v in fleet_id_mapping.values()]
            fleet_info = await get_fleet_info_by_ids(fleet_ids) if fleet_ids else {}

            # ========== 核心：定义更新Fleet表fleet_cost字段的函数 ==========
            @sync_to_async
            def update_fleet_cost_field(fleet_id, cost):
                """更新Fleet表的fleet_cost字段（仅当fleet_verify_status=False时更新，其他状态直接跳过）"""
                try:
                    fleet_obj = Fleet.objects.filter(id=fleet_id).first()
                    if not fleet_obj:
                        # Fleet ID不存在，返回提示（便于日志排查）
                        return False, f'Fleet ID {fleet_id} 不存在'

                    # 核心逻辑：仅当验证状态为False时更新成本，其他状态直接跳过（返回成功+提示）
                    if fleet_obj.fleet_verify_status is False:
                        fleet_obj.fleet_cost = cost
                        fleet_obj.save(update_fields=['fleet_cost'])  # 只更新指定字段，提升性能
                        return True, ''
                    else:
                        # 验证状态非False，跳过更新，返回成功+跳过提示（不影响整体流程）
                        return True, f'Fleet ID {fleet_id} 验证状态为{str(fleet_obj.fleet_verify_status)}，跳过成本更新'
                except Exception as e:
                    # 仅捕获异常时返回失败
                    return False, f'更新Fleet表失败：{str(e)[:50]}'

            # ========== 新增：更新Shipment表备注（拼接逻辑） ==========
            @sync_to_async
            def update_shipment_note(container_num, shipping_mark, new_note):
                """更新Shipment表备注：新备注拼接到原有备注后"""
                try:
                    # 找到对应Pallet关联的Shipment记录
                    pallet = Pallet.objects.filter(
                        container_number__container_number=container_num,
                        shipping_mark=shipping_mark
                    ).select_related('shipment_batch_number').first()

                    if not pallet or not pallet.shipment_batch_number:
                        return False, f'柜号{container_num}+唛头{shipping_mark} 未找到关联的Shipment记录'

                    shipment = pallet.shipment_batch_number
                    original_note = shipment.note or ''
                    # 拼接备注：原有备注 + 分号分隔 + 新备注（仅当新备注非空时拼接）
                    if new_note.strip():
                        if original_note:
                            shipment.note = f"{original_note}; {new_note.strip()}"
                        else:
                            shipment.note = new_note.strip()
                        shipment.save(update_fields=['note'])
                    return True, ''
                except Exception as e:
                    return False, f'更新Shipment备注失败：{str(e)[:50]}'

            # 6. 批量处理数据（事务控制 + 异步兼容）
            async def batch_update_fleet_cost(rows, fleet_id_map, fleet_info_map):
                """批量录入成本（异步+事务兼容版）"""
                nonlocal success_count

                # 同步事务内处理数据校验和基础逻辑
                @sync_to_async
                def process_in_transaction():
                    nonlocal success_count
                    process_errors = []
                    valid_fleet_data = []  # 存储校验通过的车次号+成本数据

                    with transaction.atomic():  # 事务：要么全成功，要么全失败
                        for row in rows:
                            container_num = row['container_number']
                            shipping_mark = row['shipping_mark']
                            fleet_cost = row['fleet_cost']
                            row_num = row['row_num']
                            upload_note = row['note'].strip()  # 上传的备注

                            # ========== 修复3：按「柜号+唛头」查找Fleet ID + 原有备注 ==========
                            key = f"{container_num}_{shipping_mark}"
                            # 检查（柜号+唛头）是否关联了Fleet ID
                            if key not in fleet_id_map:
                                process_errors.append(
                                    f'第{row_num}行：柜号{container_num}+唛头{shipping_mark} 不存在/未关联车次'
                                )
                                continue

                            fleet_id, original_shipment_note = fleet_id_map[key]  # 解构：Fleet ID + 原有备注
                            # 检查Fleet ID是否能查到车次信息
                            if fleet_id not in fleet_info_map:
                                process_errors.append(
                                    f'第{row_num}行：柜号{container_num}+唛头{shipping_mark} 关联的车次ID({fleet_id})不存在'
                                )
                                continue

                            fleet_detail = fleet_info_map[fleet_id]
                            fleet_number = fleet_detail['fleet_number']
                            fleet_verify_status = fleet_detail['fleet_verify_status']

                            if not fleet_number:
                                process_errors.append(
                                    f'第{row_num}行：柜号{container_num}+唛头{shipping_mark} 关联的车次ID({fleet_id})无车次号'
                                )
                                continue

                            # ========== 新增前置校验：验证状态是否为False ==========
                            if fleet_verify_status is not False:
                                process_errors.append(
                                    f'第{row_num}行：柜号{container_num}+唛头{shipping_mark} 关联的车次{fleet_number}（ID:{fleet_id}）验证状态为{str(fleet_verify_status)}，不允许覆盖成本'
                                )
                                continue

                            # 校验通过，记录待录入的数据（新增：传递原有备注和上传备注）
                            valid_fleet_data.append({
                                'row_num': row_num,
                                'container_num': container_num,
                                'shipping_mark': shipping_mark,
                                'fleet_id': fleet_id,
                                'fleet_number': fleet_number,
                                'fleet_cost': fleet_cost,
                                'original_shipment_note': original_shipment_note,  # 原有备注
                                'upload_note': upload_note  # 上传的备注
                            })
                            success_count += 1  # 累加成功计数

                    return process_errors, valid_fleet_data

                # 执行同步事务处理
                process_errors, valid_fleet_data = await process_in_transaction()
                error_messages.extend(process_errors)

                # 异步调用成本录入方法（处理每个校验通过的数据）
                for data in valid_fleet_data:
                    try:
                        # 步骤1：调用原有成本录入方法（建议也改为按柜号+唛头录入）
                        await self.insert_fleet_shipment_pallet_fleet_cost(
                            request, data['fleet_number'], data['fleet_cost']
                        )

                        # 步骤2：更新Fleet表的fleet_cost字段（已包含验证状态校验）
                        update_success, update_msg = await update_fleet_cost_field(
                            data['fleet_id'], data['fleet_cost']
                        )

                        if not update_success:
                            # 仅当更新失败（如Fleet ID不存在/异常）时才抛异常
                            raise Exception(update_msg)
                        elif update_msg:
                            # 跳过更新的提示，记录日志但不影响成功计数
                            logger.info(f'第{data["row_num"]}行：{update_msg}')
                            error_messages.append(f'第{data["row_num"]}行：{update_msg}')

                        # ========== 新增步骤3：更新Shipment表备注（拼接逻辑） ==========
                        if data['upload_note']:  # 仅当上传备注非空时才更新
                            note_update_success, note_update_msg = await update_shipment_note(
                                data['container_num'],
                                data['shipping_mark'],
                                data['upload_note']
                            )
                            if not note_update_success:
                                logger.warning(f'第{data["row_num"]}行：{note_update_msg}')
                                error_messages.append(f'第{data["row_num"]}行：{note_update_msg}')
                            else:
                                logger.info(
                                    f'第{data["row_num"]}行：柜号{data["container_num"]}+唛头{data["shipping_mark"]} 备注更新完成，原有备注：{data["original_shipment_note"][:50]}，新增备注：{data["upload_note"][:50]}'
                                )

                        logger.info(
                            f'第{data["row_num"]}行：柜号{data["container_num"]}+唛头{data["shipping_mark"]}（车次{data["fleet_number"]}）成本处理完成，金额：{data["fleet_cost"]}'
                        )
                    except Exception as e:
                        logger.error(
                            f'第{data["row_num"]}行：柜号{data["container_num"]}+唛头{data["shipping_mark"]}（车次{data["fleet_number"]}）录入异常',
                            exc_info=True
                        )
                        error_messages.append(
                            f'第{data["row_num"]}行：柜号{data["container_num"]}+唛头{data["shipping_mark"]} 处理失败：{str(e)[:100]}')
                        success_count -= 1  # 仅异常时才扣减成功数

            # 执行批量录入
            await batch_update_fleet_cost(valid_rows, fleet_id_mapping, fleet_info)

            # 7. 返回结果
            return await self.handle_fleet_cost_record_get_ltl(request, error_messages, success_count)

        except Exception as e:
            # 捕获全局异常
            logger.error('LTL批量上传成本整体异常', exc_info=True)
            error_messages = [f'文件处理失败：{str(e)[:200]}']
            return await self.handle_fleet_cost_record_get_ltl(request, error_messages, 0)

    async def handle_download_recorded_fleet_cost(self, request: HttpRequest):
        """FTL已录入派送成本下载（完全对齐前端展示逻辑）"""
        # 1. 获取筛选条件（与前端查询条件完全一致）
        pickup_number = request.POST.get("pickup_number", "")
        fleet_number = request.POST.get("fleet_number", "")
        batch_number = request.POST.get("batch_number", "")
        area = request.POST.get("area") or None

        # 2. 构建已录入数据的查询条件（复用前端已录入筛选逻辑）
        criteria = Q(
            pod_uploaded_at__isnull=False,
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_schduled_at__gte="2025-05-01",
            fleet_number__fleet_cost__isnull=False,  # 已录入费用
        )

        # 追加筛选条件
        if pickup_number:
            criteria &= Q(fleet_number__pickup_number=pickup_number)  # 前端是精确匹配，保持一致
        if fleet_number:
            criteria &= Q(fleet_number__fleet_number=fleet_number)  # 前端是精确匹配
        if batch_number:
            criteria &= Q(shipment_batch_number=batch_number)  # 前端是精确匹配
        if area and area is not None and area != "None":
            criteria &= Q(origin=area)

        # 3. 查询Shipment基础数据（复用前端的关联查询和排序）
        shipment_list = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .prefetch_related(
                Prefetch(
                    "fleetshipmentpallets",
                    queryset=FleetShipmentPallet.objects.select_related("operator")
                )
            )
            .filter(criteria)
            .order_by("shipped_at")
        )

        # 4. 批量计算Pallet汇总数据（完全复用前端的性能优化逻辑）
        # 4.1 收集所有shipment的id
        shipment_ids = [s.id for s in shipment_list]
        # 4.2 查询所有关联的Pallet数据并按shipment_batch_number分组
        pallet_data = await sync_to_async(list)(
            Pallet.objects.filter(
                shipment_batch_number__in=shipment_ids  # shipment_batch_number = shipment.id
            ).values("shipment_batch_number", "weight_lbs", "cbm")
        )
        # 4.3 构建分组字典：key=shipment.id，value={总重, 总CBM, 总板数}
        pallet_summary = {}
        for pallet in pallet_data:
            batch_id = pallet["shipment_batch_number"]
            if batch_id not in pallet_summary:
                pallet_summary[batch_id] = {
                    "total_weight_lbs": 0.0,
                    "total_cbm": 0.0,
                    "total_pallets": 0
                }
            # 累加重量和CBM（兼容空值）
            pallet_summary[batch_id]["total_weight_lbs"] += float(pallet["weight_lbs"] or 0)
            pallet_summary[batch_id]["total_cbm"] += float(pallet["cbm"] or 0)
            # 总板数+1（每条pallet记录对应一个板子）
            pallet_summary[batch_id]["total_pallets"] += 1

        # 5. 处理Shipment数据（补充成本录入信息和Pallet汇总数据）
        @sync_to_async
        def process_shipment_for_csv(shipment_list, pallet_summary):
            processed_data = []
            for s in shipment_list:
                # 5.1 获取成本录入信息（复用前端逻辑）
                latest_pallet = s.fleetshipmentpallets.order_by("-cost_input_time", "-id").first()

                # 5.2 组装单条数据（与前端表格字段一一对应）
                data = {
                    # 基础字段
                    "pickup_number": s.fleet_number.pickup_number if s.fleet_number else "",
                    "fleet_cost": s.fleet_number.fleet_cost if (s.fleet_number and s.fleet_number.fleet_cost) else "",
                    "fleet_number": s.fleet_number.fleet_number if (
                                s.fleet_number and s.fleet_number.fleet_number) else "",
                    "shipment_batch_number": s.shipment_batch_number or "",
                    "appointment_id": s.appointment_id or "",
                    "carrier": s.carrier or "",
                    # 日期字段（兼容空值）
                    "appointment_datetime": s.fleet_number.appointment_datetime.strftime("%Y-%m-%d")
                    if (s.fleet_number and s.fleet_number.appointment_datetime) else "",
                    "departured_at": s.fleet_number.departured_at.strftime("%Y-%m-%d")
                    if (s.fleet_number and s.fleet_number.departured_at) else "",
                    # Pallet汇总字段
                    "shipped_weight": round(pallet_summary.get(s.id, {}).get("total_weight_lbs", 0.0), 2),
                    "shipped_cbm": round(pallet_summary.get(s.id, {}).get("total_cbm", 0.0), 2),
                    "shipped_pallet": pallet_summary.get(s.id, {}).get("total_pallets", 0),
                    # 其他字段
                    "note": s.note or "",
                    "fleet_cost_back": s.fleet_number.fleet_cost_back if (
                                s.fleet_number and s.fleet_number.fleet_cost_back) else "",
                    "pallet_cost_input_time": latest_pallet.cost_input_time.strftime("%Y-%m-%d")
                    if (latest_pallet and latest_pallet.cost_input_time) else "",
                    "pallet_operator_name": latest_pallet.operator.username
                    if (latest_pallet and latest_pallet.operator) else ""
                }
                processed_data.append(data)
            return processed_data

        # 执行数据处理
        processed_data = await process_shipment_for_csv(shipment_list, pallet_summary)

        # 6. 生成CSV响应
        response = HttpResponse(content_type='text/csv')
        filename = f"FTL已录入派送成本_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        # 解决中文文件名乱码问题
        response['Content-Disposition'] = f'attachment; filename="{filename}"'.encode('utf-8')

        # 7. 写入CSV表头和内容
        writer = csv.writer(response)
        # 表头（与前端表格列完全一致）
        writer.writerow([
            'PickUp Number', '费用', '出库批次', '预约批次', '预约号',
            'Carrier', '预约发车日期', '实际发车日期', '总重lbs', '总CBM',
            '总卡板数', '备注', '退回费用', '录入时间', '操作人'
        ])

        # 写入数据行
        for item in processed_data:
            writer.writerow([
                item["pickup_number"],
                item["fleet_cost"],
                item["fleet_number"],
                item["shipment_batch_number"],
                item["appointment_id"],
                item["carrier"],
                item["appointment_datetime"],
                item["departured_at"],
                item["shipped_weight"],
                item["shipped_cbm"],
                item["shipped_pallet"],
                item["note"],
                item["fleet_cost_back"],
                item["pallet_cost_input_time"],
                item["pallet_operator_name"]
            ])

        return response

    async def handle_update_fleet_verify_status(self, request: HttpRequest):
        """
        统一处理单条/批量更新核实状态
        - 修复核心：用sync_to_async包装所有数据库操作，兼容异步上下文
        """
        logger = logging.getLogger(__name__)
        logger.info(f"【更新核实状态】接收到的POST参数：{dict(request.POST)}")

        # 1. 解析参数（兼容单条/批量）
        single_fleet_number = request.POST.get('fleet_number')
        single_is_verified = request.POST.get('is_verified', 'false') == 'true'

        batch_fleet_numbers = request.POST.get('selected_fleet_numbers', '')
        batch_action = request.POST.get('verify_action', 'verify')
        batch_is_verified = batch_action == 'verify'

        # 2. 确定要更新的车次号列表和核实状态
        fleet_number_list = []
        is_verified = False

        # 优先处理批量操作
        if batch_fleet_numbers:
            # 严格清洗：去重、去空、去空格
            fleet_number_list = list(set([num.strip() for num in batch_fleet_numbers.split(',') if num.strip()]))
            is_verified = batch_is_verified
            logger.info(f"【批量操作】清洗后待更新车次号列表：{fleet_number_list}，核实状态：{is_verified}")
        # 处理单条操作
        elif single_fleet_number:
            fleet_number_list = [single_fleet_number.strip()]  # 单条也清洗
            is_verified = single_is_verified
            logger.info(f"【单条操作】清洗后待更新车次号：{fleet_number_list}，核实状态：{is_verified}")
        # 无有效参数
        else:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].append("未获取到有效的车次号")
            error_message = "【更新失败】未获取到有效的车次号"
            return await self.handle_fleet_cost_record_get_ltl(request, error_message, 0)

        # 3. 定义同步的数据库操作函数（核心修复）
        def update_fleet_verify_status_sync(fleet_numbers, verify_status):
            """同步更新核实状态（包装成异步可调用）"""
            try:
                # 同步查询匹配数量
                match_count = Fleet.objects.filter(fleet_number__in=fleet_numbers).count()
                logger.info(f"【查询匹配数据】车次号列表{fleet_numbers}匹配到{match_count}条记录")

                if match_count == 0:
                    return 0, [f"未找到匹配的车次号：{fleet_numbers}"]

                # 同步批量更新
                success_count = Fleet.objects.filter(
                    fleet_number__in=fleet_numbers
                ).update(fleet_verify_status=verify_status)

                logger.info(f"【更新成功】{'核实' if verify_status else '取消核实'} {success_count} 条记录")
                return success_count, [f"成功{'核实' if verify_status else '取消核实'} {success_count} 条记录"]

            except Exception as e:
                logger.error(f"【更新失败】操作异常：{str(e)}", exc_info=True)
                return 0, [f"{'核实' if verify_status else '取消核实'}操作失败：{str(e)}"]

        # 4. 执行更新操作（同步函数转异步）
        success_count, error_messages = await sync_to_async(update_fleet_verify_status_sync)(
            fleet_number_list, is_verified
        )

        # 5. 保存错误信息到session
        if error_messages:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].extend(error_messages)
            request.session.modified = True  # 异步下必须标记session修改

        # 6. 返回原页面
        return await self.handle_fleet_cost_record_get_ltl(request, None, success_count)

    async def handle_rollback_fleet_status(self, request: HttpRequest):
        """
        统一处理单条/批量退回未录入状态
        - 支持单个退回：从fleet_number获取车次号
        - 支持批量退回：从selected_fleet_numbers获取多个车次号
        - 核心逻辑：不是删除数据，而是重置核实状态/成本字段为未录入状态
        """
        logger = logging.getLogger(__name__)
        logger.info(f"【退回未录入】接收到的POST参数：{dict(request.POST)}")

        # 1. 解析参数（兼容单条/批量）
        single_fleet_number = request.POST.get('fleet_number')
        batch_fleet_numbers = request.POST.get('selected_fleet_numbers', '')

        # 2. 确定要更新的车次号列表
        fleet_number_list = []

        # 优先处理批量操作
        if batch_fleet_numbers:
            # 严格清洗：去重、去空、去空格
            fleet_number_list = list(set([num.strip() for num in batch_fleet_numbers.split(',') if num.strip()]))
            logger.info(f"【批量退回】清洗后车次号列表：{fleet_number_list}")
        # 处理单条操作
        elif single_fleet_number:
            fleet_number_list = [single_fleet_number.strip()]  # 单条也清洗
            logger.info(f"【单条退回】车次号：{fleet_number_list}")
        # 无有效参数
        else:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].append("未获取到有效的车次号")
            logger.warning("【退回失败】未获取到有效的车次号")
            return await self.handle_fleet_cost_record_get_ltl(request, None, 0)

        # 3. 定义同步的数据库操作函数（核心：重置为未录入状态，而非删除）
        def rollback_fleet_status_sync(fleet_numbers):
            """同步退回未录入状态"""
            try:
                # 同步查询匹配数量
                match_count = Fleet.objects.filter(fleet_number__in=fleet_numbers).count()
                logger.info(f"【查询匹配数据】车次号列表{fleet_numbers}匹配到{match_count}条记录")

                if match_count == 0:
                    return 0, [f"未找到匹配的车次号：{fleet_numbers}"]

                # 同步批量更新（重置为未录入状态）
                # 根据业务需求调整字段：比如重置核实状态、清空成本、退回状态等
                success_count = Fleet.objects.filter(
                    fleet_number__in=fleet_numbers
                ).update(
                    fleet_verify_status=False,  # 重置核实状态为未核实
                    fleet_cost=None,  # 清空已录入的成本
                    fleet_cost_back=None,  # 清空退回费用（如有）
                    # 可添加其他需要重置的字段
                )

                logger.info(f"【退回成功】成功退回 {success_count} 条记录到未录入状态")
                return success_count, [f"成功退回 {success_count} 条记录到未录入状态"]

            except Exception as e:
                logger.error(f"【退回失败】操作异常：{str(e)}", exc_info=True)
                return 0, [f"退回操作失败：{str(e)}"]

        # 4. 执行更新操作（同步函数转异步）
        success_count, error_messages = await sync_to_async(rollback_fleet_status_sync)(fleet_number_list)

        # 5. 保存错误信息到session
        if error_messages:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].extend(error_messages)
            request.session.modified = True  # 异步下必须标记session修改

        # 6. 返回原页面
        return await self.handle_fleet_cost_record_get_ltl(request, None, success_count)

    async def handle_batch_confirm_verify(self, request: HttpRequest):
        """
        ftl统一处理单条/批量更新核实状态
        - 核心优化：改用ID作为唯一标识，避免车次号重复问题
        - 修复核心：用sync_to_async包装所有数据库操作，兼容异步上下文
        """
        logger = logging.getLogger(__name__)

        # 1. 解析参数（兼容单条/批量，改用ID）
        single_fleet_id = request.POST.get('fleet_id')
        single_is_verified = request.POST.get('is_verified', 'false') == 'true'

        # 批量ID：接收数组形式的ID列表
        batch_fleet_ids = request.POST.getlist('selected_fleet_ids')  # 关键：用getlist接收多个ID
        batch_action = request.POST.get('verify_action', 'verify')
        batch_is_verified = batch_action == 'verify'

        # 调试：打印解析后的参数
        logger.info(f"【解析后】批量ID列表：{batch_fleet_ids}，操作类型：{batch_action}，核实状态：{batch_is_verified}")

        # 2. 确定要更新的ID列表和核实状态
        fleet_id_list = []
        is_verified = False

        # 优先处理批量操作
        if batch_fleet_ids:
            # 严格清洗：去重、去空、确保是数字
            fleet_id_list = list(set([id.strip() for id in batch_fleet_ids if id.strip().isdigit()]))
            is_verified = batch_is_verified
        # 处理单条操作
        elif single_fleet_id and single_fleet_id.strip().isdigit():
            fleet_id_list = [single_fleet_id.strip()]
            is_verified = single_is_verified
        # 无有效参数
        else:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].append("未获取到有效的记录ID")
            error_message = "【更新失败】未获取到有效的记录ID"
            logger.error(error_message)
            return await self.handle_fleet_cost_record_get(request, error_message, 0)

        # 3. 定义同步的数据库操作函数（核心：按ID查询）
        def update_fleet_verify_status_sync(fleet_ids, verify_status):
            """同步更新核实状态（包装成异步可调用）"""
            try:
                # 同步查询匹配数量（按ID查询，更精准）
                match_count = Fleet.objects.filter(id__in=fleet_ids).count()
                if match_count == 0:
                    return 0, [f"未找到匹配的记录ID：{fleet_ids}"]

                # 同步批量更新（按ID更新）
                success_count = Fleet.objects.filter(
                    id__in=fleet_ids
                ).update(fleet_verify_status=verify_status)
                return success_count, None

            except Exception as e:
                logger.error(f"【更新失败】操作异常：{str(e)}", exc_info=True)
                return 0, [f"{'核实' if verify_status else '取消核实'}操作失败：{str(e)}"]

        # 4. 执行更新操作（同步函数转异步）
        success_count, error_messages = await sync_to_async(update_fleet_verify_status_sync)(
            fleet_id_list, is_verified
        )

        # 5. 保存错误信息到session
        if error_messages:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].extend(error_messages)
            request.session.modified = True  # 异步下必须标记session修改

        # 6. 返回原页面
        return await self.handle_fleet_cost_record_get(request, None, success_count)

    async def handle_batch_cancel_verify(self, request: HttpRequest):
        """
        ftl统一处理单条/批量退回未录入状态
        - 核心优化：改用ID作为唯一标识
        - 核心逻辑：不是删除数据，而是重置核实状态/成本字段为未录入状态
        """
        logger = logging.getLogger(__name__)

        # 1. 解析参数（兼容单条/批量，改用ID）
        single_fleet_id = request.POST.get('fleet_id')
        batch_fleet_ids = request.POST.getlist('selected_fleet_ids')  # 关键：用getlist接收多个ID

        # 2. 确定要更新的ID列表
        fleet_id_list = []

        # 优先处理批量操作
        if batch_fleet_ids:
            # 严格清洗：去重、去空、确保是数字
            fleet_id_list = list(set([id.strip() for id in batch_fleet_ids if id.strip().isdigit()]))
        # 处理单条操作
        elif single_fleet_id and single_fleet_id.strip().isdigit():
            fleet_id_list = [single_fleet_id.strip()]
        # 无有效参数
        else:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].append("未获取到有效的记录ID")
            return await self.handle_fleet_cost_record_get(request, None, 0)

        # 3. 定义同步的数据库操作函数（核心：按ID重置状态）
        def rollback_fleet_status_sync(fleet_ids):
            """重置为未核实"""
            try:
                # 同步查询匹配数量（按ID查询）
                match_count = Fleet.objects.filter(id__in=fleet_ids).count()

                if match_count == 0:
                    return 0, [f"未找到匹配的记录ID：{fleet_ids}"]

                # 同步批量更新（重置为未录入状态）
                success_count = Fleet.objects.filter(
                    id__in=fleet_ids  # 按ID更新，精准操作
                ).update(
                    fleet_verify_status=False  # 重置核实状态为未核实
                )

                return success_count, []

            except Exception as e:
                logger.error(f"【退回失败】操作异常：{str(e)}", exc_info=True)
                return 0, [f"退回操作失败：{str(e)}"]

        # 4. 执行更新操作（同步函数转异步）
        success_count, error_messages = await sync_to_async(rollback_fleet_status_sync)(fleet_id_list)

        # 5. 保存错误信息到session
        if error_messages:
            if 'error_messages' not in request.session:
                request.session['error_messages'] = []
            request.session['error_messages'].extend(error_messages)
            request.session.modified = True  # 异步下必须标记session修改

        # 6. 返回原页面
        return await self.handle_fleet_cost_record_get(request, None, success_count)

    async def handle_fleet_warehouse_search_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = (
            request.POST.get("name")
            if request.POST.get("name")
            else request.POST.get("warehouse")
        )
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                origin=warehouse,
                fleet_number__isnull=True,
                in_use=True,
                is_canceled=False,
                appointment_id__isnull=False,
                shipment_appointment__gte=timezone.datetime(2025, 10, 1),
                #shipment_type="FTL",   非FTL的，都会自动排车，所以这个条件可以暂时隐藏
            ).exclude(
                appointment_id__icontains='None'
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
            "warehouse_form": warehouse_form,
            "warehouse": warehouse,
            "shipment_ids": [],
        }
        return self.template_fleet, context

    async def handle_add_appointment_to_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selections = request.POST.getlist("is_appointment_added")
        ids = request.POST.getlist("shipment_ids")
        selected_ids = [int(id) for s, id in zip(selections, ids) if s == "on"]
        if selected_ids:
            current_time = datetime.now()
            # 加载出库批次管理原状态信息
            _, context = await self.handle_fleet_warehouse_search_post(request)
            fleet_number = (
                "F"
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            shipment_selected = await sync_to_async(list)(
                Shipment.objects.filter(id__in=selected_ids)
            )
            total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
            for s in shipment_selected:
                total_weight += s.total_weight
                total_cbm += s.total_cbm
                total_pcs += s.total_pcs
                total_pallet += s.total_pallet
            fleet_data = {
                "fleet_number": fleet_number,
                "fleet_type": shipment_selected[0].shipment_type,
                "origin": request.POST.get("warehouse"),
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": total_pallet,
                "total_pcs": total_pcs,
            }
            context.update(
                {
                    "shipment_ids": selected_ids,
                    "fleet_number": fleet_number,
                    "shipment_selected": shipment_selected,
                    "fleet_data": fleet_data,
                    "carrier_options": self.carrier_options,
                }
            )
            return self.template_fleet_schedule, context
        else:
            return await self.handle_fleet_warehouse_search_post(request)

    async def handle_fleet_confirmation_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        '''确认排车操作'''
        current_time = datetime.now()
        try:
            fleet_data = ast.literal_eval(request.POST.get("fleet_data"))
        except Exception as e:
            raise ValueError(f"fleet_data 解析失败: {e}")
        
        if name:
            shipment_ids = request.POST.get("selected_ids")
        else:
            shipment_ids = request.POST.get("selected_ids").strip("][").split(", ")
            shipment_ids = [int(i) for i in shipment_ids]

        if len(shipment_ids) == 0: raise ValueError("传过来的预约批次是空的！")

        shipment = await sync_to_async(list)(
                Shipment.objects.filter(id__in=shipment_ids)
            )
        shipment_type = shipment[0].shipment_type if shipment else None
        fleet_type = request.POST.get("fleet_type", "") or shipment_type

        try:
            await sync_to_async(Fleet.objects.get)(
                fleet_number=fleet_data["fleet_number"]
            )
            return await self.handle_fleet_warehouse_search_post(request)
        except:
            fleet_data.update(
                {
                    "carrier": request.POST.get("carrier", ""),
                    "license_plate": request.POST.get("license_plate", ""),
                    "motor_carrier_number": request.POST.get(
                        "motor_carrier_number", ""
                    ),
                    "dot_number": request.POST.get("dot_number", ""),
                    "third_party_address": request.POST.get("third_party_address", ""),
                    "pickup_number": request.POST.get("pickup_number", ""),
                    "appointment_datetime": request.POST.get("appointment_datetime"),
                    "scheduled_at": current_time,
                    "note": request.POST.get("note", ""),
                    "multipule_destination": True if len(shipment_ids) > 1 else False,
                    "fleet_type": fleet_type,
                    "fleet_cost": request.POST.get("fleet_cost")
                }
            )

            if not fleet_data.get("fleet_number"):
                # 如果字典里没有，尝试从 request.POST 直接读取
                req_fleet_no = request.POST.get("fleet_number")
                if req_fleet_no:
                    fleet_data["fleet_number"] = req_fleet_no
                else:
                    # 如果连 request 里都没有，这里必须报错或抛出异常，否则数据库会报错
                    raise ValueError("无法获取车次号(fleet_number)，请检查参数传递")
            fleet = Fleet(**fleet_data)
            await sync_to_async(fleet.save)()
            
            for s in shipment:
                s.fleet_number = fleet
            await sync_to_async(bulk_update_with_history)(
                shipment,
                Shipment,
                fields=["fleet_number"],
            )
            # await sync_to_async(Shipment.objects.bulk_update)(
            #     shipment, ["fleet_number"]
            # )
            if name == "post_nsop":
                return True
            return await self.handle_fleet_warehouse_search_post(request)

    async def handle_update_fleet_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        fleet.carrier = request.POST.get("carrier", "")
        fleet.license_plate = request.POST.get("license_plate", "")
        fleet.motor_carrier_number = request.POST.get("motor_carrier_number", "")
        fleet.dot_number = request.POST.get("dot_number", "")
        fleet.third_party_address = request.POST.get("third_party_address", "")
        fleet.pickup_number = request.POST.get("pickup_number", "")
        fleet.appointment_datetime = request.POST.get("appointment_datetime")
        fleet.note = request.POST.get("note", "")
        fleet_cost_str = request.POST.get("fleet_cost", "").strip()
        if fleet_cost_str:
            fleet.fleet_cost = float(fleet_cost_str)
        await sync_to_async(fleet.save)()
        
        # 分摊车次成本
        if fleet_cost_str:
            await self.insert_fleet_shipment_pallet_fleet_cost(request, fleet.fleet_number, fleet_cost_str)
        mutable_get = request.GET.copy()
        mutable_get["warehouse"] = request.POST.get("warehouse")
        mutable_get["fleet_number"] = fleet_number
        request.GET = mutable_get
        if name == "post_nsop":
            return True
        return await self.handle_fleet_info_get(request)

    async def handle_cancel_fleet_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        if fleet.departured_at is not None:
            raise RuntimeError(
                f"Shipment with batch number {fleet_number} has been shipped!"
            )
        await sync_to_async(fleet.delete)()
        
        warehouse = request.POST.get("warehouse")
        mutable_post = request.POST.copy()
        mutable_post["name"] = warehouse
        request.POST = mutable_post
        if name == "post_nsop":
            return True
        return await self.handle_fleet_warehouse_search_post(request)

    async def handle_outbound_warehouse_search_post(
        self, request: HttpRequest, error_messages: None = None
    ) -> tuple[str, dict[str, Any]]:
        warehouse = (
            request.POST.get("name")
            if request.POST.get("name")
            else request.POST.get("warehouse")
        )
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
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
            "warehouse": warehouse,
            "warehouse_form": warehouse_form,
            "fleet": fleet,
            "error_messages": error_messages,
        }
        return self.template_outbound, context

    async def handle_export_packing_list_post(
            self, request: HttpRequest
    ) -> HttpResponse:
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("customerInfo")
        packing_list = []

        if customerInfo:
            customer_info = json.loads(customerInfo)
            for row in customer_info:
                packing_list.append(
                    {
                        "container_number__container_number": row[0].strip(),
                        "shipment_batch_number__shipment_batch_number": row[1].strip(),
                        "destination": row[2].strip(),
                        "total_cbm": row[3].strip(),
                        "total_n_pallet": row[4].strip(),
                        "slot": row[6].strip(),
                        "Appointment": row[7].strip(),
                        "shipment_batch_number__fleet_number__pickup_number": row[8].strip(),
                    }
                )
        else:
            packing_list_db1 = await sync_to_async(list)(
                PackingList.objects.select_related(
                    "container_number", "shipment_batch_number",
                    "pallet", "shipment_batch_number__fleet_number"
                )
                .filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__orders__offload_id__offload_at__isnull=True,
                )
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment",
                    "shipment_batch_number__fleet_number__pickup_number"
                )
                .annotate(
                    total_weight=Round(
                        Sum("total_weight_lbs", output_field=FloatField()), 2
                    ),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 2),
                    total_n_pallet=Sum("cbm", output_field=FloatField()) / 2,
                )
                .order_by("-shipment_batch_number__shipment_appointment")
            )

            for item in packing_list_db1:
                if item["total_n_pallet"] < 1:
                    item["total_n_pallet"] = 1
                elif item["total_n_pallet"] % 1 >= 0.45:
                    item["total_n_pallet"] = int(item["total_n_pallet"] // 1 + 1)
                else:
                    item["total_n_pallet"] = int(item["total_n_pallet"] // 1)
                item["total_n_pallet"] = f"预 {item['total_n_pallet']}"

            packing_list_db2 = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number", "shipment_batch_number",
                    "shipment_batch_number__fleet_number"
                )
                .filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__orders__offload_id__offload_at__isnull=False,
                )
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment",
                    "slot",
                    "shipment_batch_number__fleet_number__pickup_number"
                )
                .annotate(
                    total_weight=Round(Sum("weight_lbs", output_field=FloatField()), 2),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 2),
                    total_n_pallet=Count("pallet_id", distinct=True),
                )
                .order_by("-shipment_batch_number__shipment_appointment")
            )

            packing_list = packing_list_db1 + packing_list_db2

        shipment = await sync_to_async(list)(
            Shipment.objects.filter(fleet_number__fleet_number=fleet_number).order_by(
                "-shipment_appointment"
            )
        )

        df = pd.DataFrame(packing_list)

        if len(shipment) > 1:
            total_shipments = len(shipment)
            for i, s in enumerate(shipment, 1):
                if i == 1:
                    position = "inside 1"
                elif i == total_shipments:
                    position = f"outside {total_shipments}"
                else:
                    position = f"inside {i}"

                df.loc[
                    df["shipment_batch_number__shipment_batch_number"] == s.shipment_batch_number,
                    "一提两卸"
                ] = position

        df = df.rename(
            columns={
                "container_number__container_number": "柜号",
                "destination": "仓点",
                "shipment_batch_number__shipment_batch_number": "预约批次",
                "total_cbm": "CBM",
                "total_n_pallet": "板数",
                "shipment_batch_number__shipment_appointment": "Appointment",
                "slot": "库位",
            }
        )

        pickup_number = ""
        original_pickup_col = "shipment_batch_number__fleet_number__pickup_number"
        if not df.empty and original_pickup_col in df.columns:
            non_empty_pickups = df[original_pickup_col].dropna()
            if not non_empty_pickups.empty:
                pickup_number = non_empty_pickups.iloc[0]
            df = df.drop(columns=[original_pickup_col])

        first_row_data = [f"Pickup Number: {pickup_number}"] + [""] * (len(df.columns) - 1)
        first_row = pd.DataFrame([first_row_data], columns=[f"col_{i}" for i in range(len(df.columns))])
        header_row = pd.DataFrame([df.columns.tolist()], columns=first_row.columns)
        df.columns = first_row.columns
        df = pd.concat([first_row, header_row, df], ignore_index=True)

        if len(shipment) > 1:
            column_order = ["柜号", "预约批次", "仓点", "CBM", "板数", "一提两卸", "Appointment", "库位"]
        else:
            column_order = ["柜号", "预约批次", "仓点", "CBM", "板数", "库位"]

        original_cols = df.iloc[1].tolist()
        col_mapping = {original: new for original, new in zip(original_cols, df.columns)}
        keep_cols = [col_mapping[col] for col in column_order if col in col_mapping]
        df = df[keep_cols]

        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = (
            f"attachment; filename=packing_list_{fleet_number}.csv"
        )
        df.to_csv(path_or_buf=response, index=False, header=False)
        return response

    async def handle_export_bol_post(self, request: HttpRequest) -> HttpResponse:
        '''导出BOL文件的基础函数'''
        batch_number = request.POST.get("shipment_batch_number")
        warehouse = request.POST.get("warehouse")
        customerInfo = request.POST.get("customerInfo")
        pickupList = request.POST.get("pickupList")
        fleet_number = request.POST.get("fleet_number")

        # 进行判断，如果在前端进行了表的修改，就用修改后的表，如果没有修改，就用packing_list直接查询的
        if customerInfo:
            customer_info = json.loads(customerInfo)
            packing_list = []
            for row in customer_info:
                packing_list.append(
                    {
                        "container_number": row[0].strip(),
                        "shipping_mark": row[1].strip(),
                        "fba_id": row[2].strip(),
                        "ref_id": row[3].strip(),
                        "pcs": row[4].strip(),
                    }
                )
        else:
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=batch_number,
                )
            )
            for pl in packing_list:
                if pl.shipping_mark:
                    pl.shipping_mark = pl.shipping_mark.replace("/", "\n")

                if pl.fba_id:
                    pl.fba_id = pl.fba_id.replace("/", "\n")

                if pl.ref_id:
                    pl.ref_id = pl.ref_id.replace("/", "\n")
        warehouse_obj = (
            await sync_to_async(ZemWarehouse.objects.get)(name=warehouse)
            if warehouse
            else None
        )
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        try:
            address_chinese_char = False if shipment.address.isascii() else True           
        except:
            address_chinese_char = True
        destination_chinese_char = False if shipment.destination.isascii() else True
        try:
            note_chinese_char = False if shipment.note.isascii() else True
        except:
            note_chinese_char = False
        is_private_warehouse = (
            True
            if re.search(r"([A-Z]{2})[-,\s]?(\d{5})", shipment.destination.upper())
            else False
        )
        # 最后一页加上拣货单:
        pallet = await self.pickupList_get(pickupList, fleet_number, warehouse)
        if not shipment.fleet_number:
            raise ValueError("该约未排车")
        # 判断一下是不是NJ私仓的，因为NJ私仓的要多加一列板数
        is_NJ_private = False
        pallet_count = 0

        if "NJ" in shipment.origin and shipment.shipment_type == "LTL":
            is_NJ_private = True
            # 查找板数，因为私仓都是一票一个约，所以就查这个约里有几个板子，就是板数
            pallet_count = await sync_to_async(
                Pallet.objects.filter(
                    shipment_batch_number__shipment_batch_number=batch_number
                ).count
            )()
        pickup_time = shipment.pickup_time
        # 如果目的地没有沃尔玛，预约账户是沃尔玛的，导出地址加上沃尔玛前缀
        if "walmart" in shipment.shipment_account.lower():
            destination = shipment.destination
            if destination and "walmart" not in destination.lower():
                shipment.destination = f"walmart-{destination}"
            # 如果 destination 为空，也加上 walmart 前缀
            elif not destination:
                shipment.destination = "walmart"
        context = {
            "warehouse_obj": warehouse_obj.address,
            "warehouse": warehouse,
            "batch_number": batch_number,
            "pickup_number": (
                shipment.fleet_number.pickup_number
                if shipment.fleet_number and shipment.fleet_number.pickup_number
                else None
            ),
            "pickup_time": shipment.pickup_time,
            "fleet_number": shipment.fleet_number.fleet_number,
            "shipment": shipment,
            "packing_list": packing_list,
            "is_NJ_private": is_NJ_private,
            "pallet_count": pallet_count,
            "pallet": pallet,
            "address_chinese_char": address_chinese_char,
            "destination_chinese_char": destination_chinese_char,
            "note_chinese_char": note_chinese_char,
            "is_private_warehouse": is_private_warehouse,
        }
        if warehouse == "LA-91761":
            template = get_template(self.template_la_bol_pickup)
        else:  # 因为目前没有库位信息，所以BOL先不加这个信息
            template = get_template(self.template_bol_pickup)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="BOL_{batch_number}.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response

    async def handle_update_fleet_note(self, request: HttpRequest) -> HttpResponse:
        """ltl保存备注 """
        shipment_id = request.POST.get("shipment_id")
        note = request.POST.get("note")
        await sync_to_async(lambda: Shipment.objects.filter(id=shipment_id).update(note=note))()
        return await self.handle_fleet_cost_record_get_ltl(request, None, 1)

    async def handle_export_bol_packing_list_post(self, request: HttpRequest) -> HttpResponse:
        batch_number = request.POST.get("shipment_batch_number")
        warehouse = request.POST.get("warehouse")
        customerInfo = request.POST.get("customerInfo")
        pickupList = request.POST.get("pickupList")
        fleet_number = request.POST.get("fleet_number")

        # 进行判断，如果在前端进行了表的修改，就用修改后的表，如果没有修改，就用packing_list直接查询的
        if customerInfo:
            customer_info = json.loads(customerInfo)
            packing_list = []
            for row in customer_info:
                packing_list.append(
                    {
                        "container_number": row[0].strip(),
                        "shipping_mark": row[1].strip(),
                        "fba_id": row[2].strip(),
                        "ref_id": row[3].strip(),
                        "pcs": row[4].strip(),
                    }
                )
        else:
            packing_list = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number__shipment_batch_number=batch_number,
                )
            )
            for pl in packing_list:
                if pl.shipping_mark:
                    pl.shipping_mark = pl.shipping_mark.replace("/", "\n")

                if pl.fba_id:
                    pl.fba_id = pl.fba_id.replace("/", "\n")

                if pl.ref_id:
                    pl.ref_id = pl.ref_id.replace("/", "\n")
        warehouse_obj = (
            await sync_to_async(ZemWarehouse.objects.get)(name=warehouse)
            if warehouse
            else None
        )
        shipment = await sync_to_async(
            Shipment.objects.select_related("fleet_number").get
        )(shipment_batch_number=batch_number)
        try:
            address_chinese_char = False if shipment.address.isascii() else True
        except:
            address_chinese_char = True
        destination_chinese_char = False if shipment.destination.isascii() else True
        try:
            note_chinese_char = False if shipment.note.isascii() else True
        except:
            note_chinese_char = False
        is_private_warehouse = (
            True
            if re.search(r"([A-Z]{2})[-,\s]?(\d{5})", shipment.destination.upper())
            else False
        )
        # 最后一页加上拣货单:
        pallet = await self.pickupList_get(pickupList, fleet_number, warehouse)
        if not shipment.fleet_number:
            raise ValueError("该约未排车")
        # 判断一下是不是NJ私仓的，因为NJ私仓的要多加一列板数
        is_NJ_private = False
        pallet_count = 0

        if "NJ" in shipment.origin and shipment.shipment_type == "LTL":
            is_NJ_private = True
            # 查找板数，因为私仓都是一票一个约，所以就查这个约里有几个板子，就是板数
            pallet_count = await sync_to_async(
                Pallet.objects.filter(
                    shipment_batch_number__shipment_batch_number=batch_number
                ).count
            )()
        pickup_time = shipment.pickup_time
        # 如果目的地没有沃尔玛，预约账户是沃尔玛的，导出地址加上沃尔玛前缀
        if "walmart" in shipment.shipment_account.lower():
            destination = shipment.destination
            if destination and "walmart" not in destination.lower():
                shipment.destination = f"walmart-{destination}"
            # 如果 destination 为空，也加上 walmart 前缀
            elif not destination:
                shipment.destination = "walmart"
        context = {
            "warehouse_obj": warehouse_obj.address,
            "warehouse": warehouse,
            "batch_number": batch_number,
            "pickup_number": (
                shipment.fleet_number.pickup_number
                if shipment.fleet_number and shipment.fleet_number.pickup_number
                else None
            ),
            "pickup_time": shipment.pickup_time,
            "fleet_number": shipment.fleet_number.fleet_number,
            "shipment": shipment,
            "packing_list": packing_list,
            "is_NJ_private": is_NJ_private,
            "pallet_count": pallet_count,
            "pallet": pallet,
            "address_chinese_char": address_chinese_char,
            "destination_chinese_char": destination_chinese_char,
            "note_chinese_char": note_chinese_char,
            "is_private_warehouse": is_private_warehouse,
        }
        if warehouse == "LA-91761":
            template = get_template(self.template_la_bol_pickup_alone)
        else:  # 因为目前没有库位信息，所以BOL先不加这个信息
            template = get_template(self.template_bol_pickup_alone)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'inline; filename="拣货单_{batch_number}.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response


    async def pickupList_get(
        self, pickupList: Any, fleet_number: str, warehouse: str
    ) -> tuple[Any]:
        pallet: list[Pallet] | None = None
        if pickupList:  # 有值就转成列表
            pickupList = json.loads(pickupList)
        if pickupList:  # 判断是不是空列表
            pallet = []
            for row in pickupList:
                if not any(
                    str(item).strip() for item in row
                ):  # 检查列表所有元素是否为空
                    continue
                pallet_data = {
                    "container_number__container_number": row[0].strip(),
                    "shipment_batch_number__shipment_batch_number": row[1].strip(),
                    "destination": row[2].strip(),
                    "total_weight": row[3].strip(),
                    "total_cbm": row[4].strip(),
                    "total_n_pallet": row[5].strip(),
                }
                if len(row) >= 6 and row[5]:
                    pallet_data["一提两卸"] = row[5].strip()
                pallet.append(pallet_data)
        else:
            pallet = await sync_to_async(list)(
                PackingList.objects.select_related(
                    "container_number", "shipment_batch_number", "pallet"
                )
                .filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__orders__offload_id__offload_at__isnull=True,
                )
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment",
                )
                .annotate(
                    total_weight=Round(
                        Sum("total_weight_lbs", output_field=FloatField()), 2
                    ),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 2),
                    total_n_pallet=Sum("cbm", output_field=FloatField()) / 2,
                )
                .order_by("-shipment_batch_number__shipment_appointment")
            )
            for s in pallet:
                n = round(s['total_cbm'] / 2) if round(s['total_cbm'] / 2) else 1
                s["total_n_pallet"] = f"预 {n}"
                s["slot"] = ""
            plt = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number", "shipment_batch_number"
                )
                .filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__orders__offload_id__offload_at__isnull=False,
                )
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__shipment_appointment",
                    "slot",
                )
                .annotate(
                    total_weight=Round(Sum("weight_lbs", output_field=FloatField()), 2),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 2),
                    total_n_pallet=Cast(
                        Count("pallet_id", distinct=True), output_field=CharField()
                    ),
                )
                .order_by("-shipment_batch_number__shipment_appointment")
            )
            pallet += plt
            pallet.sort(
                key=lambda x: (
                    x.get("shipment_batch_number__shipment_appointment", ""),
                    x.get("destination", ""),
                ),
                reverse=True,
            )
            shipments = await sync_to_async(list)(
                Shipment.objects.filter(
                    fleet_number__fleet_number=fleet_number
                ).order_by("-shipment_appointment")
            )
            df = pd.DataFrame(pallet)
            if len(shipments) > 1:
                total = len(shipments)  # 获取总数量
                for i, s in enumerate(shipments, 1):  # 从1开始计数
                    if i == 1:  # 第一个
                        position = "inside 1"
                    elif i == total:  # 最后一个
                        position = f"outside {total}"
                    else:  # 中间的
                        position = f"inside {i}"

                    df.loc[
                        df["shipment_batch_number__shipment_batch_number"]
                        == s.shipment_batch_number,
                        "一提两卸",
                    ] = position

                pallet = df.to_dict("records")
        for plt in pallet:
            order = await sync_to_async(Order.objects.get)(
                container_number__container_number=plt[
                    "container_number__container_number"
                ]
            )
            warehouse_plt = await sync_to_async(getattr)(order, "warehouse")
            warehouse_plt = str(warehouse_plt)
            if warehouse_plt and warehouse_plt != warehouse:
                warehouse_prefix = warehouse_plt.split("-")[0]
                plt["destination"] = f"{plt['destination']} ({warehouse_prefix})"
        processed_pallet = []
        prev_destination = None
        for item in pallet:
            current_destination = item.get("destination")
            if prev_destination is not None and current_destination != prev_destination:
                # 插入空行（创建一个包含所有必要字段的空字典）
                empty_row = {
                    "container_number__container_number": "  ",
                    "destination": "  ",
                    "total_cbm": "       ——",
                    "total_weight_lbs": "  ",
                    "total_n_pallet": "  ",
                    "shipment_batch_number__shipment_batch_number": "  ",
                    "shipment_batch_number__shipment_appointment": "  ",
                    "slot": "  ",
                    "一提两卸": "  ",
                    "is_spacer": True,  # 表示是否是空行
                    "force_text": True,
                }
                processed_pallet.append(empty_row)
            processed_pallet.append(item)
            prev_destination = current_destination
        return processed_pallet

    async def handle_fleet_departure_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        
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

        error_messages = []
        empty_indices = [i for i, ids in enumerate(plt_ids) if ids == ['']]
        if empty_indices:
            # 将索引转换为用户友好的位置（从1开始计数）
            positions = [f"第{i+1}组" for i in empty_indices]
            error_message = f"有未打板的货物，请核实！行数：{', '.join(positions)}"
            
            if name == "post_nsop":
                return {'error_messages': error_message}
            error_messages.append(error_message)
            return await self.handle_outbound_warehouse_search_post(request,error_messages)
        
        #判断是否有未解扣的板子，有的话，就直接报错
        all_flat_ids = [pid for group in plt_ids for pid in group]

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(shipment_batch_number__in=batch_number).order_by(
                "-shipment_appointment"
            )
        )
        #甩板的板子
        unshipped_pallet_ids = []
        shipped_pallet_ids = []
        for plt_id, p_schedule, p_shipped in zip(
            plt_ids, scheduled_pallet, actual_shipped_pallet
        ):
            if p_schedule > p_shipped:
                unshipped_count = p_schedule - p_shipped
                unshipped_pallet_ids += plt_id[: p_schedule - p_shipped]
                shipped_pallet_ids += plt_id[unshipped_count:p_schedule]
            elif p_schedule == p_shipped:
                shipped_pallet_ids += plt_id[:p_schedule]
            else:
                if name == "post_nsop":
                    return {'error_messages':"出库板数大于实际库存，请核实！"}
                error_messages.append(f"出库板数大于实际库存，请核实！")
                return await self.handle_outbound_warehouse_search_post(request,error_messages)

        #未出的就是甩板，记录甩板状态
        await sync_to_async(
            lambda: Pallet.objects.filter(id__in=unshipped_pallet_ids).update(is_dropped_pallet=True)
        )()
        #要出库的查看下是否有未解扣的
        shipped_pallets = await sync_to_async(
            lambda: list(Pallet.objects.filter(id__in=shipped_pallet_ids).select_related('container_number'))
        )()
        # 找出包含"暂扣留仓"的板子
        hold_pallets = await sync_to_async(
            lambda: [pallet for pallet in shipped_pallets if "暂扣留仓" in getattr(pallet, 'delivery_method', '')]
        )()

        # 如果有暂扣留仓的板子，记录错误信息       
        if hold_pallets:
            if name == "post_nsop":
                return {'error_messages':"存在未解扣的板子，不能确认出库！"}
            error_messages.append(f"存在板子未解扣，不能确认出库")
            return await self.handle_outbound_warehouse_search_post(request,error_messages)
        
        unshipped_pallet = await sync_to_async(list)(
            Pallet.objects.select_related("shipment_batch_number").filter(
                id__in=unshipped_pallet_ids
            )
        )
        # 把出库的板子的slot改为空    
        await sync_to_async(
            lambda: Pallet.objects.filter(id__in=shipped_pallet_ids)
            .update(slot=None)
        )()
        unshipped_pallet_ids = [int(pid) for pid in unshipped_pallet_ids]

        await sync_to_async(Pallet.objects.filter(
            id__in=unshipped_pallet_ids
        ).update)(shipment_batch_number=None)
     
        shipment_pallet = {}
        for p in unshipped_pallet:
            if p.shipment_batch_number.shipment_batch_number not in shipment_pallet:
                shipment_pallet[p.shipment_batch_number.shipment_batch_number] = [p]
            else:
                shipment_pallet[p.shipment_batch_number.shipment_batch_number].append(p)
        updated_shipment = []
        updated_pallet = []
        (
            fleet_shipped_weight,
            fleet_shipped_cbm,
            fleet_shipped_pallet,
            fleet_shipped_pcs,
        ) = (0, 0, 0, 0)
        for s in shipment:
            if shipment_pallet.get(s.shipment_batch_number):
                dumped_pallets = len(
                    set(
                        [
                            p.pallet_id
                            for p in shipment_pallet.get(s.shipment_batch_number)
                        ]
                    )
                )
            else:
                dumped_pallets = 0
            tzinfo = self._parse_tzinfo(s.origin)
            s.is_shipped = True
            s.shipped_at = departured_at
            s.shipped_at_utc = self._parse_ts(departured_at, tzinfo)
            s.shipped_pallet = s.total_pallet - dumped_pallets
            if dumped_pallets > 0:
                dumped_weight = sum(
                    [p.weight_lbs for p in shipment_pallet.get(s.shipment_batch_number)]
                )
                dumped_cbm = sum(
                    [p.cbm for p in shipment_pallet.get(s.shipment_batch_number)]
                )
                dumped_pcs = sum(
                    [p.pcs for p in shipment_pallet.get(s.shipment_batch_number)]
                )
                s.pallet_dumpped = dumped_pallets
                s.is_full_out = False
                s.shipped_weight = s.total_weight - dumped_weight
                s.shipped_cbm = s.total_weight - dumped_cbm
                s.shipped_pcs = s.total_weight - dumped_pcs
                for p in shipment_pallet.get(s.shipment_batch_number):
                    p.shipment_batch_number = None
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
            if s.shipment_type == "客户自提":
                s.arrived_at = departured_at
                s.arrived_at_utc = self._parse_ts(departured_at, tzinfo)
                s.is_arrived = True
        if fleet.fleet_type == "客户自提":
            fleet.arrived_at = departured_at
        fleet.departured_at = departured_at
        fleet.shipped_weight = fleet_shipped_weight
        fleet.shipped_cbm = fleet_shipped_cbm
        fleet.shipped_pallet = fleet_shipped_pallet
        fleet.shipped_pcs = fleet_shipped_pcs
        await sync_to_async(bulk_update_with_history)(
            updated_shipment,
            Shipment,
            fields=[
                "shipped_pallet",
                "shipped_weight",
                "shipped_cbm",
                "shipped_pcs",
                "is_shipped",
                "shipped_at",
                "shipped_at_utc",
                "pallet_dumpped",
                "is_full_out",
                "arrived_at",
                "is_arrived",
            ],
        )
        await sync_to_async(bulk_update_with_history)(
            updated_pallet,
            Pallet,
            fields=["shipment_batch_number"],
        )
        await sync_to_async(fleet.save)()
        # await sync_to_async(lambda: None)()
        # 构建fleet_shipment_pallet表
        # 获取每行记录的第一个板子的id，为了读PO_ID
        sample_pallet_ids = [
            group.split(",")[-1] for group in request.POST.getlist("plt_ids")
        ]
        
        pallets = await sync_to_async(list)(
            Pallet.objects.filter(id__in=sample_pallet_ids)
            .select_related("shipment_batch_number", "container_number")
            .only("id","pallet_id", "PO_ID", "shipment_batch_number", "container_number")
        )
        error = None
        if not pallets:
            error = "查不到有效板子！"
        pallet_mapping = {
            p.pallet_id: (p.PO_ID, p.shipment_batch_number, p.container_number, p.id)
            for p in pallets
            if p.PO_ID
        }
        new_fleet_shipment_pallets = []
        for first_pallet_id, actual_pallets in zip(
            sample_pallet_ids, actual_shipped_pallet
        ):
            target_id = int(first_pallet_id)
            matched_key = None
            for key, value in pallet_mapping.items():
                if value[3] == target_id:
                    matched_key = key
                    break
            if matched_key is None:
                continue

            po_id, shipment, container_number, _ = pallet_mapping[matched_key]
            new_record = FleetShipmentPallet(
                fleet_number=fleet,
                pickup_number=fleet.pickup_number,
                shipment_batch_number=shipment,
                PO_ID=po_id,
                total_pallet=actual_pallets,
                container_number=container_number,
            )
            new_fleet_shipment_pallets.append(new_record)

        if new_fleet_shipment_pallets:
            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )
        else:
            if name == "post_nsop":
                return {'error_messages':f"成本记录没有生成成功！{error}"}
            error_messages.append(f"成本记录没有生成成功！{error}")
        if name == "post_nsop":
            return {'success_messages':f"{fleet_number}车出库成功"}
        return await self.handle_outbound_warehouse_search_post(request)

    async def handle_confirm_delivery_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        arrived_ats = request.POST.getlist("arrived_at")  # 使用 getlist 获取数组
        # fleet_numbers = request.POST.getlist("fleet_number")  # 使用 getlist 获取数组
        shipments = request.POST.getlist("shipment_batch_number")
        if not isinstance(arrived_ats, list):
            arrived_ats = [arrived_ats]
        if not isinstance(shipments, list):
            shipments = [shipments]
        if len(arrived_ats) != len(shipments):
            raise ValueError(f"length is not valid!")
        for arrived_at, ship in zip(arrived_ats, shipments):
            shipment = await sync_to_async(
                lambda: Shipment.objects.select_related("fleet_number").get(
                    shipment_batch_number=ship
                )
            )()
            fleet = shipment.fleet_number

            tzinfo = self._parse_tzinfo(shipment.origin)
            shipment.arrived_at = arrived_at
            shipment.arrived_at_utc = self._parse_ts(arrived_at, tzinfo)
            shipment.is_arrived = True
            await sync_to_async(shipment.save)()
            if fleet:
                fleet.arrived_at = arrived_at
                await sync_to_async(fleet.save)()
        if name == "post_nsop":
            return True
        return await self.handle_delivery_and_pod_get(request)

    async def handle_pod_upload_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        conn = await self._get_sharepoint_auth()
        if "file" in request.FILES:
            files = request.FILES.getlist("file")
            shipment_batch_numbers = request.POST.getlist("shipment_batch_number")
            if isinstance(shipment_batch_numbers, str):
                shipment_batch_numbers = [shipment_batch_numbers]
            if len(files) != len(shipment_batch_numbers):
                raise ValueError("文件数量和 shipment_batch_number 数量不匹配")
            for file, shipment_batch_number in zip(files, shipment_batch_numbers):
                await self._upload_file_to_sharepoint(conn, shipment_batch_number, file)
        else:
            raise ValueError("未找到上传的文件")
        if name == "post_nsop":
            return True
        return await self.handle_pod_upload_get(request)

    async def _upload_file_to_sharepoint(
        self, conn, shipment_batch_number: str, file
    ) -> None:
        shipment = await sync_to_async(Shipment.objects.get)(
            shipment_batch_number=shipment_batch_number
        )
        file_extension = os.path.splitext(file.name)[1]  # 提取扩展名
        file_path = os.path.join(
            SP_DOC_LIB, f"{SYSTEM_FOLDER}/pod/{APP_ENV}"
        )  # 文档库名称，系统文件夹名称，当前环境
        # 上传到SharePoint
        try:
            sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
            resp = sp_folder.upload_file(
                f"{shipment_batch_number}{file_extension}", file
            ).execute_query()
        except:
            conn = await self._get_sharepoint_auth()
            sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
            resp = sp_folder.upload_file(
                f"{shipment_batch_number}{file_extension}", file
            ).execute_query()
        # 生成并获取链接
        link = (
            resp.share_link(SharingLinkKind.AnonymousView)
            .execute_query()
            .value.to_json()["sharingLinkInfo"]["Url"]
        )
        shipment.pod_link = link
        shipment.pod_uploaded_at = timezone.now()
        await sync_to_async(shipment.save)()

    async def _export_ltl_label(self, request: HttpRequest) -> HttpResponse:
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("customerInfo")
        contact_flag = False  # 表示地址栏空出来，客服手动P上去
        contact = ""
        if customerInfo and customerInfo != "[]":
            customerInfo = json.loads(customerInfo)
            for row in customerInfo:
                if row[8] != "":
                    contact_flag = True
                    contact = row[8]
                    contact = re.sub("[\u4e00-\u9fff]", " ", contact)
                    contact = re.sub(r"\uFF0C", ",", contact)
                    new_contact = contact.split(";")
                    contact = {
                        "company": new_contact[0].strip(),
                        "Road": new_contact[1].strip(),
                        "city": new_contact[2].strip(),
                        "name": new_contact[3],
                        "phone": new_contact[4],
                    }
        arm_pickup = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number__container_number",
                "shipment_batch_number__fleet_number",
            )
            .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
            .values(
                "container_number__container_number",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__ARM_PRO",
                "shipment_batch_number__fleet_number__carrier",
                "shipment_batch_number__fleet_number__appointment_datetime",
                "destination",
                "shipping_mark",
            )
            .annotate(
                total_pcs=Sum("pcs"),
                total_pallet=Count("pallet_id", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
            )
        )
        pallets = 0
        for arm in arm_pickup:
            arm_pro = arm["shipment_batch_number__ARM_PRO"]
            carrier = arm["shipment_batch_number__fleet_number__carrier"]
            pickup_time = arm["shipment_batch_number__shipment_appointment"]
            container_number = arm["container_number__container_number"]
            destination = arm["destination"]
            shipping_mark = arm["shipping_mark"]
            pallets += arm["total_pallet"]
        pickup_time_str = str(pickup_time)
        date_str = datetime.strptime(pickup_time_str[:19], "%Y-%m-%d %H:%M:%S")
        pickup_time = date_str.strftime("%Y-%m-%d")

        # 生成条形码
        barcode_type = "code128"
        barcode_class = barcode.get_barcode_class(barcode_type)
        if arm_pro == "" or arm_pro == "None" or arm_pro == None:
            barcode_content = f"{container_number}|{shipping_mark}"
        else:
            barcode_content = f"{arm_pro}"
        my_barcode = barcode_class(
            barcode_content, writer=ImageWriter()
        )  # 将条形码转换为图像形式
        buffer = io.BytesIO()  # 创建缓冲区
        my_barcode.write(buffer, options={"dpi": 600})  # 缓冲区存储图像
        buffer.seek(0)
        image = Image.open(buffer)
        width, height = image.size
        new_height = int(height * 0.7)
        cropped_image = image.crop((0, 0, width, new_height))
        new_buffer = io.BytesIO()
        cropped_image.save(new_buffer, format="PNG")

        barcode_base64 = base64.b64encode(new_buffer.getvalue()).decode("utf-8")
        data = [
            {
                "warehouse": request.POST.get("warehouse"),
                "arm_pro": arm_pro,
                "barcode": barcode_base64,
                "carrier": carrier,
                "contact": contact,
                "contact_flag": contact_flag,
                "fraction": f"{i + 1}/{pallets}",
            }
            for i in range(pallets)
        ]
        context = {"data": data}
        template = get_template(self.template_ltl_label)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{pickup_time}+{container_number}+{destination}+{shipping_mark}+LABEL.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response

    async def _export_ltl_bol(self, request: HttpRequest) -> HttpResponse:
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("customerInfo")
        warehouse = request.POST.get("warehouse")
        contact_flag = False  # 表示地址栏空出来，客服手动P上去
        contact = {}
        if customerInfo and customerInfo != "[]":
            customerInfo = json.loads(customerInfo)
            arm_pickup = [
                [
                    "container_number__container_number",
                    "destination",
                    "shipping_mark",
                    "shipment_batch_number__ARM_PRO",
                    "total_pallet",
                    "total_pcs",
                    "shipment_batch_number__fleet_number__carrier",
                    "shipment_batch_number__note",
                ]
            ]
            has_slot_column = any(len(row) > 10 for row in customerInfo)
            if has_slot_column:
                arm_pickup[0].append("slot")
            for row in customerInfo:
                if row[8] != "" and "None" not in row[7]:
                    contact_flag = True
                    contact = row[8]
                    contact = re.sub("[\u4e00-\u9fff]", " ", contact)
                    contact = re.sub(r"\uFF0C", ",", contact)
                    new_contact = contact.split(";")
                    contact = {
                        "company": new_contact[0].strip(),
                        "Road": new_contact[1].strip(),
                        "city": new_contact[2].strip(),
                        "name": new_contact[3],
                        "phone": new_contact[4],
                    }
                arm_pickup.append(
                    [
                        row[0].strip(),
                        row[1].strip(),
                        row[2].strip(),
                        row[3].strip(),
                        int(row[4].strip()),
                        int(row[5].strip()),
                        row[6].strip(),
                        row[9].strip(),
                        (
                            row[10].strip()
                            if len(row) > 10 and row[10] is not None
                            else ""
                        ),
                    ]
                )
            keys = arm_pickup[0]
            arm_pickup_dict_list = []
            for row in arm_pickup[1:]:
                row_dict = dict(zip(keys, row))
                arm_pickup_dict_list.append(row_dict)
            arm_pickup = arm_pickup_dict_list
        else:  # 没有就从数据库查
            arm_pickup = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number__container_number",
                    "shipment_batch_number__fleet_number",
                )
                .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
                .values(
                    "container_number__container_number",
                    "shipment_batch_number__shipment_appointment",
                    "shipment_batch_number__ARM_PRO",
                    "shipment_batch_number__fleet_number__carrier",
                    "shipment_batch_number__fleet_number__appointment_datetime",
                    "shipment_batch_number__fleet_number__fleet_type",
                    "destination",
                    "shipping_mark",
                    "shipment_batch_number__note",
                    "slot",
                )
                .annotate(
                    total_pcs=Sum("pcs"),
                    total_pallet=Count("pallet_id", distinct=True),
                    total_weight=Sum("weight_lbs"),
                    total_cbm=Sum("cbm"),
                )
            )

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        pickup_time_str = fleet.appointment_datetime
        pickup_time = pickup_time_str.strftime("%Y-%m-%d")
        pallet = 0
        pcs = 0
        shipping_mark = ""
        notes = set()
        for arm in arm_pickup:
            arm_pro = arm["shipment_batch_number__ARM_PRO"]
            carrier = arm["shipment_batch_number__fleet_number__carrier"]
            pallet += arm["total_pallet"]
            pcs += int(arm["total_pcs"])
            container_number = arm["container_number__container_number"]
            destination = arm["destination"]
            shipping_mark += arm["shipping_mark"]
            notes.add(arm["shipment_batch_number__note"])
            marks = arm["shipping_mark"]
            if marks:
                array = marks.split(",")
                if len(array) > 1:
                    parts = []
                    for i in range(0, len(array)):
                        part = ",".join(array[i : i + 1])
                        parts.append(part)
                    new_marks = "\n".join(parts)
                else:
                    new_marks = marks
            arm["shipping_mark"] = new_marks
        notes_str = "<br>".join(filter(None, notes))
        # 生成条形码

        barcode_type = "code128"
        barcode_class = barcode.get_barcode_class(barcode_type)
        if arm_pro == "" or arm_pro == "None" or arm_pro == None:
            barcode_content = f"{container_number}|{destination}"
        else:
            barcode_content = f"{arm_pro}"
        barcode_content = barcode_content.replace('\xa0', ' ')
        my_barcode = barcode_class(
            barcode_content, writer=ImageWriter()
        )  # 将条形码转换为图像形式
        buffer = io.BytesIO()  # 创建缓冲区
        my_barcode.write(buffer, options={"dpi": 600})  # 缓冲区存储图像
        buffer.seek(0)
        image = Image.open(buffer)
        width, height = image.size
        new_height = int(height * 0.7)
        cropped_image = image.crop((0, 0, width, new_height))
        new_buffer = io.BytesIO()
        cropped_image.save(new_buffer, format="PNG")

        barcode_base64 = base64.b64encode(new_buffer.getvalue()).decode("utf-8")
        # 增加一个拣货单的表格
        context = {
            "warehouse": warehouse,
            "arm_pro": arm_pro,
            "carrier": carrier,
            "pallet": pallet,
            "pcs": pcs,
            "barcode": barcode_base64,
            "arm_pickup": arm_pickup,
            "contact": contact,
            "contact_flag": contact_flag,
            "pickup_time": pickup_time,
            "notes": notes_str,
        }
        template = get_template(self.template_ltl_bol)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{container_number}+{destination}+{shipping_mark}+BOL.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response

    def safe_value(value, default=""):
        return value.strip() if value is not None else default

    # 上传客户自提的BOL文件
    async def handle_bol_upload_post(self, request: HttpRequest) -> HttpResponse:
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("customerInfo")
        notes = ""
        pickup_number = ""

        # 如果在界面输入了，就用界面添加后的值
        if customerInfo and customerInfo != "[]":
            customer_info = json.loads(customerInfo)
            arm_pickup = [
                [
                    "container",
                    "destination",
                    "mark",
                    "pallet",
                    "pcs",
                    "carrier",
                    "pickup",
                ]
            ]
            for row in customer_info:
                # 把提货时间修改格式
                pickup_time = row[6].strip()
                s_time = pickup_time.split(" ")[0]
                dt = datetime.strptime(s_time, "%Y-%m-%d")
                new_string = dt.strftime("%m-%d")
                destination = re.sub(r"[\u4e00-\u9fff]", " ", row[1])
                arm_pickup.append(
                    [
                        row[0].strip(),
                        destination,
                        row[2].strip(),
                        row[3].strip(),
                        row[4].strip(),
                        row[5].strip(),
                        s_time,
                    ]
                )

        else:  # 没有就从数据库查
            arm_pickup = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number__container_number",
                    "shipment_batch_number__fleet_number",
                )
                .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipping_mark",
                    "shipment_batch_number__fleet_number__fleet_type",
                    "shipment_batch_number__fleet_number__carrier",
                    "shipment_batch_number__fleet_number__appointment_datetime",
                    "shipment_batch_number__fleet_number__pickup_number",  # 提取pickup_number
                    "shipment_batch_number__note",
                )
                .annotate(
                    total_pcs=Count("pcs", distinct=True),
                    total_pallet=Count("pallet_id", distinct=True),
                )
            )
            if arm_pickup:
                new_list = []
                for p in arm_pickup:
                    # 保存pickup_number（从数据库提取）
                    pickup_number = p["shipment_batch_number__fleet_number__pickup_number"] or ""
                    p_time = p["shipment_batch_number__fleet_number__appointment_datetime"]

                    # 提取年、月、日
                    year = p_time.year
                    month = p_time.month
                    day = p_time.day
                    p_time = f"{year}-{month}-{day}"
                    destination = re.sub(r"[\u4e00-\u9fff]", " ", p["destination"])
                    new_list.append(
                        [
                            p["container_number__container_number"],
                            destination,
                            p["shipping_mark"],
                            p["total_pallet"],
                            p["total_pcs"],
                            p["shipment_batch_number__fleet_number__carrier"],
                            p_time,
                        ]
                    )
                    notes += p["shipment_batch_number__note"] or ""  # 拼接备注
                arm_pickup = [
                                 [
                                     "container",
                                     "destination",
                                     "mark",
                                     "pallet",
                                     "pcs",
                                     "carrier",
                                     "pickup",
                                 ]
                             ] + new_list
            else:
                raise ValueError("柜子未拆柜，请核实")
            s_time = arm_pickup[1][-1]
            dt = datetime.strptime(s_time, "%Y-%m-%d")
            new_string = dt.strftime("%m-%d")

        # BOL需要在后面加一个拣货单
        df = pd.DataFrame(arm_pickup[1:], columns=arm_pickup[0])

        # 添加换行函数
        def wrap_text(text, max_length=11):
            """将文本按最大长度换行"""
            if not isinstance(text, str):
                text = str(text)

            if len(text) <= max_length:
                return text

            # 按最大长度分割文本
            wrapped_lines = []
            for i in range(0, len(text), max_length):
                wrapped_lines.append(text[i:i + max_length])
            return '\n'.join(wrapped_lines)

        # 对DataFrame应用换行处理
        df_wrapped = df.applymap(wrap_text)

        files = request.FILES.getlist("files")
        if files:
            system_name = platform.system()
            zh_font_path = None

            # ✅ 按系统类型设置默认路径
            if system_name == "Windows":
                zh_font_path = "C:/Windows/Fonts/msyh.ttc"  # 微软雅黑
            else:  # Linux
                # Linux 通常用 Noto 或思源黑体字体
                possible_fonts = [
                    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                    "/usr/share/fonts/truetype/arphic/uming.ttc",  # 备用
                    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",  # 文泉驿微米黑
                ]
                for path in possible_fonts:
                    if os.path.exists(path):
                        zh_font_path = path
                        break

            # ✅ 检查字体文件是否存在，否则退回默认英文字体
            if zh_font_path and os.path.exists(zh_font_path):
                zh_font = fm.FontProperties(fname=zh_font_path)
                plt.rcParams["font.family"] = zh_font.get_name()
            else:
                plt.rcParams["font.family"] = "DejaVu Sans"

            plt.rcParams["axes.unicode_minus"] = False  # 防止负号乱码

            for file in files:
                # 设置通用字体避免警告
                # plt.rcParams['font.family'] = ['sans-serif']
                # plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']

                # 保持原来的A4尺寸
                fig, ax = plt.subplots(figsize=(10.4, 8.5))
                ax.axis("tight")
                ax.axis("off")
                # 稍微减小顶部边距，为标题留出一点空间
                fig.subplots_adjust(top=1.45)  # 从1.5微调到1.45

                # 在表格上方添加标题
                ax.text(
                    0.5,  # 水平居中
                    0.9,  # 非常靠近顶部，在表格上方
                    "Pickup List",
                    fontdict={"size": 12, "weight": "bold"},
                    va="top",
                    ha="center",
                    transform=ax.transAxes,
                )

                # 在标题下方添加Pickup Number
                ax.text(
                    0.5,  # 水平居中
                    0.85,  # 紧挨着标题
                    f"Pickup Number: {pickup_number}",
                    fontdict={"size": 10},
                    va="top",
                    ha="center",
                    transform=ax.transAxes,
                )

                # 创建表格 - 保持原来的位置和设置
                the_table = ax.table(
                    cellText=df_wrapped.values,
                    colLabels=df_wrapped.columns,
                    loc="upper center",
                    cellLoc="center",
                    bbox=[0.12, 0.7, 0.8, 0.12]  # [x0, y0, width, height]
                )

                # 设置表格样式 - 保持原来的设置，只增加行高
                for pos, cell in the_table.get_celld().items():
                    cell.set_fontsize(10)  # 保持原来的字体大小

                    # 启用文本换行功能
                    cell.set_text_props(wrap=True)

                    # 增加行高以容纳换行文本
                    if pos[0] != 1:  # 数据行
                        cell.set_height(0.04)  # 从0.03增加到0.04
                    else:  # 表头行
                        cell.set_height(0.025)  # 从0.02增加到0.025

                    # 列宽设置保持不变
                    if pos[1] == 0 or pos[1] == 1 or pos[1] == 2:
                        cell.set_width(0.15)
                    elif pos[1] == 3 or pos[1] == 4:
                        cell.set_width(0.06)
                    else:
                        cell.set_width(0.12)

                table_bbox = the_table.get_window_extent(
                    renderer=ax.figure.canvas.get_renderer()
                )
                table_bbox = table_bbox.transformed(
                    ax.transAxes.inverted()
                )
                table_bottom = table_bbox.y0

                # 1. 绘制Notes
                notes_y = table_bottom - 0.04  # 稍微增加间距
                ax.text(
                    0.05,
                    notes_y,
                    f"Notes: {notes}",
                    fontdict={"size": 10},
                    va="top",
                    ha="left",
                    transform=ax.transAxes,
                )

                # 2. 绘制pickup_number
                pickup_y = notes_y - 0.03
                ax.text(
                    0.05,
                    pickup_y,
                    f"pickup_number: {pickup_number}",
                    fontdict={"size": 10},
                    va="top",
                    ha="left",
                    transform=ax.transAxes,
                )

                # 保存表格和文本到buffer
                buf_table = io.BytesIO()
                fig.savefig(buf_table, format="pdf", bbox_inches="tight")
                buf_table.seek(0)

                # 合并PDF
                merger = PdfMerger()
                temp_pdf_io = io.BytesIO(file.read())
                merger.append(PdfReader(temp_pdf_io))
                merger.append(PdfReader(buf_table))

                # 写入输出文件
                output_buf = io.BytesIO()
                merger.write(output_buf)
                output_buf.seek(0)

                file_name = file.name

        response = HttpResponse(output_buf.getvalue(), content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{new_string}-{file_name}.pdf"'
        )
        return response

    async def handle_abnormal_fleet_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        status = request.POST.get("abnormal_status", "").strip()
        description = request.POST.get("abnormal_description", "").strip()
        # fleet_number = request.POST.get("fleet_number")
        shipment_batch_number = request.POST.get("shipment_batch_number")
        fleet_cost = request.POST.get("fleet_cost")
        abnormal_cost = request.POST.get("abnormal_cost")

        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related("fleet_number").get(
                shipment_batch_number=shipment_batch_number
            )
        )()
        fleet = shipment.fleet_number
        if fleet:
            fleet.is_canceled = True
            fleet.status = "Exception"
            fleet.status_description = f"{status}-{description}"
            if fleet_cost is not None:
                fleet.fleet_cost = float(fleet_cost)
                # 分摊成本
                await self.insert_fleet_shipment_pallet_fleet_cost(request, fleet.fleet_number, fleet_cost)
            else:
                raise ValueError("车次成本不能为空！")
            if abnormal_cost is not None:
                fleet.fleet_cost_back = float(abnormal_cost)
                # 分摊退回费用金额
                await self.insert_fleet_shipment_pallet_fleet_cost_back(
                    request,
                    fleet.fleet_number,
                    abnormal_cost
                )

        if not shipment.previous_fleets:
            shipment.previous_fleets = fleet.fleet_number
        else:
            shipment.previous_fleets += f",{fleet.fleet_number}"
        shipment.status = "Exception"
        shipment.status_description = f"{status}-{description}"
        shipment.fleet_number = None
        shipment.is_shipped = False
        shipment.shipped_at = None
        shipment.shipped_at_utc = None
        
        await sync_to_async(shipment.save)()
        if fleet:
            await sync_to_async(fleet.save)()  

        
        if name == "post_nsop":
            return True
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

    async def _get_packing_list(
        self,
        plt_criteria: models.Q | None = None,
        selected_fleet_number: str | None = None,
    ) -> list[Any]:
        data = []
        if plt_criteria:
            pal_list = await sync_to_async(list)(
                Pallet.objects.prefetch_related(
                    "container_number",
                    "container_number__orders",
                    "container_number__orders__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__orders__offload_id",
                    "container_number__orders__customer_name",
                    "container_number__orders__retrieval_id",
                )
                .filter(
                    plt_criteria,
                )
                .annotate(
                    schedule_status=Case(
                        When(
                            Q(
                                container_number__orders__offload_id__offload_at__lte=datetime.now().date()
                                + timedelta(days=-7)
                            ),
                            then=Value("past_due"),
                        ),
                        default=Value("on_time"),
                        output_field=CharField(),
                    ),
                    str_id=Cast("id", CharField()),
                )
                .values(
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_number",
                    "shipment_batch_number__appointment_id",
                    "shipment_batch_number__shipment_appointment",
                    "container_number__container_number",
                    "container_number__orders__customer_name__zem_name",
                    "destination",
                    "PO_ID",
                    "address",
                    "delivery_method",
                    "container_number__orders__offload_id__offload_at",
                    "schedule_status",
                    "abnormal_palletization",
                    "po_expired",
                    target_retrieval_timestamp=F(
                        "container_number__orders__retrieval_id__target_retrieval_timestamp"
                    ),
                    target_retrieval_timestamp_lower=F(
                        "container_number__orders__retrieval_id__target_retrieval_timestamp_lower"
                    ),
                    temp_t49_pickup=F(
                        "container_number__orders__retrieval_id__temp_t49_available_for_pickup"
                    ),
                    warehouse=F(
                        "container_number__orders__retrieval_id__retrieval_destination_precise"
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
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                )
                .order_by("container_number__orders__offload_id__offload_at")
            )
            for pal in pal_list:
                if (
                    pal["shipment_batch_number__fleet_number__fleet_number"]
                    != selected_fleet_number
                ):
                    data.append(pal)
        return data

    async def handle_alter_po_shipment_post(
        self, result: list[dict[str, Any]], shipment: list[Shipment]
    ) -> None:
        # result是加塞的表格，plt_id,目的地，原板数，出库板数，柜号
        for p in result:
            for s in shipment:
                if p["destination"] == s.destination:
                    # 将该条记录加到约里
                    Utilized_pallets = await sync_to_async(list)(
                        Pallet.objects.select_related("container_number").filter(
                            id__in=p["ids"]
                        )
                    )

                    # 板子绑定要加塞的约
                    master_shipment_mapping = {}  # 改主约的PO_ID和shipment组对
                    plt_shipment_po_ids = set()  # 需要改实际约的
                    master_shipment = None
                    packing_lists_to_update = []
                    pallet_lists_to_update = []
                    for plt in Utilized_pallets:
                        if not p["has_master_shipment"]:
                            # 没有主约时
                            # 这是没有主约又被完全加塞的情况，找到第一次被加塞的约为主约
                            earliest_shipment = await sync_to_async(
                                Shipment.objects.filter(
                                    id__in=Pallet.objects.filter(PO_ID=plt.PO_ID)
                                    .exclude(shipment_batch_number__isnull=True)
                                    .values_list("shipment_batch_number", flat=True)
                                )
                                .order_by("shipment_appointment")
                                .first
                            )()
                            master_shipment = (
                                earliest_shipment if earliest_shipment else s
                            )
                            master_shipment_mapping[plt.PO_ID] = master_shipment
                            plt.master_shipment_batch_number = master_shipment

                        plt.shipment_batch_number = s
                        plt_shipment_po_ids.add(plt.PO_ID)
                        s.total_weight += plt.weight_lbs
                        s.total_pcs += plt.pcs
                        s.total_cbm += plt.cbm

                    if master_shipment_mapping:
                        master_pls = await sync_to_async(list)(
                            PackingList.objects.filter(
                                PO_ID__in=list(master_shipment_mapping.keys())
                            )
                        )
                        for pl in master_pls:
                            pl.master_shipment_batch_number = master_shipment_mapping[
                                pl.PO_ID
                            ]
                            packing_lists_to_update.append(pl)

                        master_plts = await sync_to_async(list)(
                            Pallet.objects.filter(
                                PO_ID__in=list(master_shipment_mapping.keys())
                            )
                        )
                        for plt in master_plts:
                            plt.master_shipment_batch_number = master_shipment_mapping[
                                plt.PO_ID
                            ]
                            pallet_lists_to_update.append(plt)

                    await sync_to_async(bulk_update_with_history)(
                        packing_lists_to_update,
                        PackingList,
                        fields=[
                            "shipment_batch_number",
                            "master_shipment_batch_number",
                        ],
                    )
                    await sync_to_async(bulk_update_with_history)(
                        pallet_lists_to_update,
                        Pallet,
                        fields=[
                            "shipment_batch_number",
                            "master_shipment_batch_number",
                        ],
                    )
                    # pl绑定实际约
                    if plt_shipment_po_ids:
                        packing_lists = await sync_to_async(list)(
                            PackingList.objects.filter(
                                PO_ID__in=list(plt_shipment_po_ids)
                            )
                        )
                        await sync_to_async(bulk_update_with_history)(
                            packing_lists,
                            PackingList,
                            fields=[
                                "shipment_batch_number",
                                "master_shipment_batch_number",
                            ],
                        )

                    s.total_pallet += p["pallets"]
                    await sync_to_async(bulk_update_with_history)(
                        Utilized_pallets,
                        Pallet,
                        fields=[
                            "shipment_batch_number",
                            "master_shipment_batch_number",
                        ],
                    )

                    order = await sync_to_async(list)(
                        Order.objects.select_related(
                            "retrieval_id", "warehouse", "container_number"
                        ).filter(
                            container_number__container_number__in=p["container_number"]
                        )
                    )
                    assigned_warehouse = s.origin
                    warehouse = await sync_to_async(ZemWarehouse.objects.get)(
                        name=assigned_warehouse
                    )
                    updated_order, updated_retrieval = [], []
                    for o in order:
                        if (
                            not o.warehouse
                            or not o.retrieval_id.retrieval_destination_precise
                        ):
                            o.warehouse = warehouse
                            o.retrieval_id.retrieval_destination_precise = (
                                assigned_warehouse
                            )
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

    async def _get_sharepoint_auth(self) -> ClientContext:
        ctx = ClientContext(SP_URL).with_client_certificate(
            SP_TENANT,
            SP_CLIENT_ID,
            SP_THUMBPRINT,
            private_key=SP_PRIVATE_KEY,
            scopes=[SP_SCOPE],
        )
        return ctx

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    def _parse_tzinfo(self, s: str) -> str:
        if not isinstance(s, str):
            return "America/New_York"
        if "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: Any, tzinfo: str) -> str:
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
