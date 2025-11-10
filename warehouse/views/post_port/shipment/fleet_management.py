import ast
import base64
import io
import json
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Any
import os
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
    When,
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
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
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
    template_bol = "export_file/bol_base_template.html"
    template_bol_pickup = "export_file/bol_template.html"
    template_la_bol_pickup = "export_file/LA_bol_template.html"
    template_ltl_label = "export_file/ltl_label.html"
    template_ltl_bol = "export_file/ltl_bol.html"
    template_abnormal_fleet_warehouse_search = (
        "post_port/shipment/abnormal/01_fleet_management_main.html"
    )
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX", "CA": "CA"}
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
        "CA-91789": "CA-91789",
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
            template, context = await self.handle_outbound_warehouse_search_post(
                request
            )
            return render(request, template, context)
        elif step == "export_packing_list":
            return await self.handle_export_packing_list_post(request)
        elif step == "export_bol":
            return await self.handle_export_bol_post(request)
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
        elif step == "fleet_cost_confirm":
            template, context = await self.handle_fleet_cost_confirm_get(request)
            return render(request, template, context)
        elif step == "upload_fleet_cost":
            template, context = await self.handle_upload_fleet_cost_get(request)
            return render(request, template, context)
        elif step == "download_fleet_cost_template":
            return await self.handle_fleet_cost_export(request)
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

    async def handle_fleet_depature_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selected_fleet_number = request.GET.get("fleet_number")
        warehouse = request.GET.get("warehouse")
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
                total_pcs=Sum("pcs", distinct=True),
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
                "container_number__order__offload_id",
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__order__offload_id__offload_at__isnull=False,
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
                "container_number__order__offload_id",
            )
            .annotate(
                str_id=Cast("id", CharField()),
            )
            .filter(
                shipment_batch_number__fleet_number__fleet_number=selected_fleet_number,
                container_number__order__offload_id__offload_at__isnull=True,
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
            container_number__order__offload_id__offload_at__isnull=False,
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
        headers = ["PickUp Number", "出库批次", "预约批次", "ISA", "费用"]
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
        form = UploadFileForm(request.POST, request.FILES)
        error_messages = [] #错误信息
        success_count = 0
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            if "费用" in df.columns:
                valid_rows = []
                for index, row in df.iterrows():
                    #满足：费用存在 或 (PickUp Number/出库批次/预约批次 至少一个存在)
                    if pd.notna(row["费用"]) or any([
                        pd.notna(row.get("PickUp Number")),
                        pd.notna(row.get("出库批次")),
                        pd.notna(row.get("预约批次")),
                        pd.notna(row.get("ISA")),
                    ]):
                        isa_value = ""
                        if pd.notna(row["ISA"]):
                            try:
                                isa_value = str(int(float(row["ISA"]))).strip()
                            except (ValueError, TypeError) as e:
                                error_messages.append(f"第{index+2}行: ISA值 '{isa_value}' 转换失败 - {str(e)}")
                                continue 
                        row_data = (
                            str(row["PickUp Number"]).strip() if pd.notna(row["PickUp Number"]) else "",
                            str(row["出库批次"]).strip() if pd.notna(row["出库批次"]) else "",
                            str(row["预约批次"]).strip() if pd.notna(row["预约批次"]) else "",
                            isa_value,
                            float(row["费用"]) if pd.notna(row["费用"]) else 0.0,
                            index + 2
                        )
                        valid_rows.append(row_data)
            else:
                error_messages.append(f"文件缺少必要列。找到的列: {df.columns.tolist()}")
                return await self.handle_fleet_cost_record_get(request, error_messages, 0)

            for (
                pickup_number,
                fleet_number,
                shipment_batch_number,
                ISA,
                fleet_cost,
                row_number
            ) in valid_rows:
                #try:
                if fleet_cost <= 0:
                    error_messages.append(f"第{row_number}行: 费用不能为负或零")
                    continue
                fleet = None
                search_criteria = ""
                # 更新车次表的价格
                if pickup_number:
                    fleet_query = await sync_to_async(list)(
                        Fleet.objects.filter(pickup_number=pickup_number).only(
                            "id", "fleet_number", "pickup_number"
                        )
                    )
                    if len(fleet_query) > 1:
                        error_messages.append(f"第{row_number}行: PickUp Number '{pickup_number}' 对应多个车次")
                        continue
                    if not fleet_query:
                        error_messages.append(f"第{row_number}行: 未找到 PickUp Number '{pickup_number}' 对应的车次")
                        continue
                    fleet = fleet_query[0]
                    search_criteria = f"PickUp Number: {pickup_number}"
                elif shipment_batch_number:
                    try:
                        shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
                        fleet = await sync_to_async(getattr)(shipment, 'fleet_number')
                        if not fleet:
                            error_messages.append(f"第{row_number}行: 未找到ISA '{ISA}' 对应的车次")
                            continue
                        search_criteria = f"预约批次: {shipment_batch_number}"
                    except Shipment.DoesNotExist:
                        error_messages.append(f"第{row_number}行: 未找到预约批次 '{shipment_batch_number}' 对应的车次")
                        continue
                elif fleet_number:
                    try:
                        fleet = await sync_to_async(Fleet.objects.get)(
                            fleet_number=fleet_number
                        )
                        search_criteria = f"出库批次: {fleet_number}"
                    except Fleet.DoesNotExist:
                        error_messages.append(f"第{row_number}行: 未找出库批次 '{fleet_number}' 对应的车次")
                        continue
                elif ISA:                     
                    try:
                        shipment = await sync_to_async(Shipment.objects.get)(appointment_id=ISA)
                        fleet = await sync_to_async(getattr)(shipment, 'fleet_number')
                        if not fleet:
                            error_messages.append(f"第{row_number}行: 未找到ISA '{ISA}' 对应的车次")
                            continue
                        search_criteria = f"ISA: {ISA}"
                    except Shipment.DoesNotExist:
                        error_messages.append(f"第{row_number}行: 未找到ISA '{ISA}' 对应的车次")
                        continue
                    except Shipment.MultipleObjectsReturned:
                        error_messages.append(f"第{row_number}行: ISA '{ISA}' 对应多条记录")
                        continue
                else:
                    error_messages.append(f"第{row_number}行: 缺少车次识别信息")
                    continue
                
                if hasattr(fleet, 'fleet_cost') and fleet.fleet_cost is not None:
                    # 检查是否有相关的FleetShipmentPallet记录且已记录
                    existing_records = await sync_to_async(list)(
                        FleetShipmentPallet.objects.filter(
                            models.Q(fleet_number=fleet) | 
                            models.Q(pickup_number=fleet.pickup_number)
                        )
                    )
                    if existing_records and any(record.is_recorded for record in existing_records):
                        error_messages.append(f"第{row_number}行 ({search_criteria}): 费用已经登记过，不能修改")
                        continue

                fleet.fleet_cost = fleet_cost
                await sync_to_async(fleet.save)()

                # 更新fleetshipmentpallet表
                if pickup_number:
                    criteria = models.Q(pickup_number=pickup_number)
                elif shipment_batch_number:
                    criteria = models.Q(
                        shipment_batch_number__shipment_batch_number=shipment_batch_number
                    )
                elif fleet_number:
                    criteria = models.Q(fleet_number__fleet_number=fleet_number)
                elif ISA:
                    criteria = models.Q(
                        shipment_batch_number__appointment_id=ISA
                    )
                else:
                    criteria = models.Q(fleet_number=fleet)
                fleet_shipments = await sync_to_async(list)(
                    FleetShipmentPallet.objects.filter(criteria).only(
                        "id", "total_pallet", "expense", "is_recorded"
                    )
                )

                if not fleet_shipments:
                    # 如果找不到，说明这个车次，在系统上没有经过确认出库那一步，这里再补上
                    if shipment_batch_number:
                        criteria_plt = models.Q(
                            shipment_batch_number__shipment_batch_number=shipment_batch_number
                        )
                    elif ISA:
                        criteria_plt = models.Q(
                            shipment_batch_number__appointment_id=ISA
                        )
                    elif fleet_number:
                        criteria_plt = models.Q(
                            shipment_batch_number__fleet_number=fleet_number
                        )
                    elif pickup_number:
                        criteria_plt = models.Q(
                            shipment_batch_number__fleet_number__pickup_number=pickup_number
                        )
                    else:
                        criteria_plt = models.Q(shipment_batch_number__fleet_number=fleet)
                    #先找到这个车/约里面的板子，按PO_ID分组，因为一组PO_ID存成一条记录
                    grouped_pallets = await sync_to_async(list)(
                        Pallet.objects.filter(criteria_plt)
                        .values("shipment_batch_number", "PO_ID", "container_number")
                        .annotate(
                            actual_pallets=Count("pallet_id")
                        )  # 计算每组的板子数量
                        .order_by("shipment_batch_number", "PO_ID")
                    )
                    new_fleet_shipment_pallets = []
                    if not grouped_pallets:
                        error_messages.append(f"第{row_number}行 ({search_criteria}): 这个批次里面板数是空的")
                        continue
                    for group in grouped_pallets:
                        new_record = FleetShipmentPallet(
                            fleet_number=fleet,
                            pickup_number=fleet.pickup_number,
                            shipment_batch_number_id=group["shipment_batch_number"],
                            PO_ID=group["PO_ID"],
                            total_pallet=group["actual_pallets"],
                            container_number_id=group["container_number"],
                            is_recorded=False, #这里只是登记，没有记录到总费用，所以默认是False
                        )
                        new_fleet_shipment_pallets.append(new_record)

                    await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                        new_fleet_shipment_pallets, batch_size=500
                    )
                    fleet_shipments = await sync_to_async(list)(
                        FleetShipmentPallet.objects.filter(criteria).only(
                            "id", "total_pallet", "expense", "is_recorded"
                        )
                    )
                if any(getattr(fs, 'is_recorded', False) for fs in fleet_shipments):
                    error_messages.append(f"第{row_number}行 ({search_criteria}): 费用已经登记过，不能修改")
                    #这个费用已经被记录到总成本里面了，就不能修改
                    continue
                #计算下这条记录涉及的总板数，如果这条记录是一个约的，就是这个约多少板子，如果这条记录是一个车的，就是这个车有多少板子
                total_pallets = sum(
                    fs.total_pallet for fs in fleet_shipments if fs.total_pallet
                )
                if total_pallets <= 0:
                    error_messages.append(f"第{row_number}行 ({search_criteria}): 这个批次里面板数是空的")
                    continue
                #这条记录的总费用/总板数=每个板子的单价
                cost_per_pallet = fleet_cost / total_pallets

                updates = []
                for shipment in fleet_shipments:
                    if shipment.total_pallet:
                        shipment.expense = cost_per_pallet * shipment.total_pallet
                        updates.append(shipment)
                #前面是建记录，这里是计算这条记录的expense，因为一个车有多条fleetshipmentpallet，要根据板数和板子单价计算这套记录的expense
                if updates:
                    await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
                        updates, ["expense"]
                    )
                success_count += 1
                # except Exception as e:
                #     error_messages.append(f"第{row_number}行: 处理错误 - {str(e)}")
                #     continue
        return await self.handle_fleet_cost_record_get(request,error_messages, success_count)

    async def handle_fleet_cost_confirm_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        fleet_number = request.POST.get("fleet_number", "")
        fleet_cost = float(request.POST.get("fleet_cost", ""))
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        fleet.fleet_cost = fleet_cost
        await sync_to_async(fleet.save)()
        error_messages = []
        fleet_shipments = await sync_to_async(list)(
            FleetShipmentPallet.objects.filter(
                fleet_number__fleet_number=fleet_number
            ).only("PO_ID", "total_pallet")
        )
        if not fleet_shipments:
            # 如果找不到，说明这个车次，在系统上没有经过确认出库那一步，这里再补上
            criteria_plt = models.Q(
                shipment_batch_number__fleet_number__fleet_number=fleet_number
            )
            #先找到这个车/约里面的板子，按PO_ID分组，因为一组PO_ID存成一条记录
            grouped_pallets = await sync_to_async(list)(
                Pallet.objects.filter(criteria_plt)
                .values("shipment_batch_number", "PO_ID", "container_number")
                .annotate(
                    actual_pallets=Count("pallet_id")
                )  # 计算每组的板子数量
                .order_by("shipment_batch_number", "PO_ID")
            )
            new_fleet_shipment_pallets = []
            if not grouped_pallets:
                error_messages.append(f"{fleet_number}车次里面板子是空的")
            for group in grouped_pallets:
                new_record = FleetShipmentPallet(
                    fleet_number=fleet,
                    pickup_number=fleet.pickup_number,
                    shipment_batch_number_id=group["shipment_batch_number"],
                    PO_ID=group["PO_ID"],
                    total_pallet=group["actual_pallets"],
                    container_number_id=group["container_number"],
                    is_recorded=False, #这里只是登记，没有记录到总费用，所以默认是False
                )
                new_fleet_shipment_pallets.append(new_record)

            await sync_to_async(FleetShipmentPallet.objects.bulk_create)(
                new_fleet_shipment_pallets, batch_size=500
            )
            fleet_shipments = await sync_to_async(list)(
                FleetShipmentPallet.objects.filter(
                    fleet_number__fleet_number=fleet_number
                ).only("PO_ID", "total_pallet")
            )
        total_pallets_in_fleet = sum(
            [fs.total_pallet for fs in fleet_shipments if fs.total_pallet]
        )
        if total_pallets_in_fleet == 0:
            raise ValueError("未查找该车次下的相关板子记录")
        cost_per_pallet = fleet_cost / total_pallets_in_fleet

        for shipment in fleet_shipments:
            if shipment.total_pallet:
                shipment.expense = cost_per_pallet * shipment.total_pallet

        await sync_to_async(FleetShipmentPallet.objects.bulk_update)(
            fleet_shipments, ["expense"]
        )

        request.POST = request.POST.copy()
        request.POST["fleet_number"] = ""
        return await self.handle_fleet_cost_record_get(request)

    async def handle_fleet_cost_record_get(
        self, request: HttpRequest, error_messages=None, success_count=0
    ) -> tuple[str, dict[str, Any]]:
        pickup_number = request.POST.get("pickup_number", "")
        fleet_number = request.POST.get("fleet_number", "")
        batch_number = request.POST.get("batch_number", "")
        area = request.POST.get("area") or None
        status = request.POST.get("status", "")
        if status != "record":
            criteria = models.Q(
                pod_uploaded_at__isnull=False,
                shipped_at__isnull=False,
                arrived_at__isnull=False,
                shipment_schduled_at__gte="2025-05-01",
                fleet_number__fleet_cost__isnull=False,
            )
            
        else:
            criteria = models.Q(
                pod_uploaded_at__isnull=False,
                shipped_at__isnull=False,
                arrived_at__isnull=False,
                shipment_schduled_at__gte="2025-05-01",
                fleet_number__fleet_cost__isnull=True,
            )
        if pickup_number:
            criteria &= models.Q(fleet_number__pickup_number=pickup_number)
        if fleet_number:
            criteria &= models.Q(fleet_number__fleet_number=fleet_number)
        if batch_number:
            criteria &= models.Q(shipment_batch_number=batch_number)
        if area and area is not None and area != "None":
            criteria &= models.Q(origin=area)

        shipment = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        context = {
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
        current_time = datetime.now()
        fleet_data = ast.literal_eval(request.POST.get("fleet_data"))
        if name:
            shipment_ids = request.POST.get("selected_ids")
        else:
            shipment_ids = request.POST.get("selected_ids").strip("][").split(", ")
            shipment_ids = [int(i) for i in shipment_ids]
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
                }
            )
            fleet = Fleet(**fleet_data)
            await sync_to_async(fleet.save)()
            shipment = await sync_to_async(list)(
                Shipment.objects.filter(id__in=shipment_ids)
            )
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
        await sync_to_async(fleet.save)()
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
                    container_number__order__offload_id__offload_at__isnull=True,
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
                    container_number__order__offload_id__offload_at__isnull=False,
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
        context = {
            "warehouse_obj": warehouse_obj.address,
            "warehouse": warehouse,
            "batch_number": batch_number,
            "pickup_number": (
                shipment.fleet_number.pickup_number
                if shipment.fleet_number and shipment.fleet_number.pickup_number
                else None
            ),
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
                    container_number__order__offload_id__offload_at__isnull=True,
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
                s["total_n_pallet"] = f"预 {round(s['total_cbm'] / 2)}"
                s["slot"] = ""
            plt = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number", "shipment_batch_number"
                )
                .filter(
                    shipment_batch_number__fleet_number__fleet_number=fleet_number,
                    container_number__order__offload_id__offload_at__isnull=False,
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
                key=lambda x: x.get("shipment_batch_number__shipment_appointment", ""),
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
        else:
            contact = ""
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
                if row[8] != "" and "None" not in row[8]:
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
            pcs += arm["total_pcs"]
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
                wrapped_lines.append(text[i:i+max_length])
            return '\n'.join(wrapped_lines)

        # 对DataFrame应用换行处理
        df_wrapped = df.applymap(wrap_text)

        files = request.FILES.getlist("files")
        if files:
             # ✅ 注册中文字体
            try:
                # Windows 通常有微软雅黑
                zh_font_path = "C:/Windows/Fonts/msyh.ttc"
                zh_font = fm.FontProperties(fname=zh_font_path)
            except Exception:
                # Linux / Mac 可改为 Noto 或思源黑体
                zh_font_path = "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"
                zh_font = fm.FontProperties(fname=zh_font_path)

            # 设置全局字体（这一行非常关键）
            plt.rcParams['font.family'] = zh_font.get_name()
            plt.rcParams['axes.unicode_minus'] = False  # 防止负号乱码

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

        if not shipment.previous_fleets:
            shipment.previous_fleets = fleet.fleet_number
        else:
            shipment.previous_fleets += f",{fleet.fleet_number}"
        shipment.status = "Exception"
        shipment.status_description = f"{status}-{description}"
        shipment.fleet_number = None
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
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "container_number__order__retrieval_id",
                )
                .filter(
                    plt_criteria,
                )
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
                )
                .values(
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_number",
                    "shipment_batch_number__appointment_id",
                    "shipment_batch_number__shipment_appointment",
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "destination",
                    "PO_ID",
                    "address",
                    "delivery_method",
                    "container_number__order__offload_id__offload_at",
                    "schedule_status",
                    "abnormal_palletization",
                    "po_expired",
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
                .order_by("container_number__order__offload_id__offload_at")
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
