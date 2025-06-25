import json
import os
import random
import re
import string
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import chardet
import numpy as np
import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.retrieval import Retrieval
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    ADDITIONAL_CONTAINER,
    CONTAINER_PICKUP_CARRIER,
    DELIVERY_METHOD_CODE,
    DELIVERY_METHOD_OPTIONS,
    PACKING_LIST_TEMP_COL_MAPPING,
    SHIPPING_LINE_OPTIONS,
    WAREHOUSE_OPTIONS,
)
from warehouse.views.export_file import export_do


class OrderCreation(View):
    # template_main = 'pre_port/create_order/01_order_creation_and_management.html'
    template_order_create_base = (
        "pre_port/create_order/02_base_order_creation_status.html"
    )
    template_order_create_supplement = "pre_port/create_order/03_order_creation.html"
    template_order_create_supplement_pl_tab = (
        "pre_port/create_order/03_order_creation_packing_list_tab.html"
    )
    template_order_list = "order_management/order_list.html"
    template_order_details = "order_management/order_details.html"
    template_order_details_pl = "order_management/order_details_pl_tab.html"
    order_type = {"": "", "转运": "转运", "直送": "直送", "转运组合": "转运组合"}
    area = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    container_type = {
        "45HQ/GP": "45HQ/GP",
        "40HQ/GP": "40HQ/GP",
        "20GP": "20GP",
        "53HQ": "53HQ",
    }

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_order_basic_info_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "container_info_supplement":
            template, context = await self.handle_order_supplemental_info_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "order_management_list":
            template, context = await self.handle_order_management_list_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "order_management_container":
            template, context = await self.handle_order_management_container_get(
                request
            )
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "create_order_basic":
            template, context = await self.handle_create_order_basic_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_basic_info":
            template, context = await self.handle_update_order_basic_info_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_shipping_info":
            template, context = await self.handle_update_order_shipping_info_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_packing_list_info":
            template, context = await self.handle_update_order_packing_list_info_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "update_order_retrieval_info":
            template, context = await self.handle_update_order_retrieval_info_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "upload_template":
            template, context = await self.handle_upload_template_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "download_template":
            return await self.handle_download_template_post()
        elif step == "order_management_search":
            start_date_eta = request.POST.get("start_date_eta")
            end_date_eta = request.POST.get("end_date_eta")
            start_date_etd = request.POST.get("start_date_etd")
            end_date_etd = request.POST.get("end_date_etd")
            template, context = await self.handle_order_management_list_get(
                start_date_eta, end_date_eta, start_date_etd, end_date_etd
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "export_do":
            return await sync_to_async(export_do)(request)
        elif step == "delete_order":
            template, context = await self.handle_delete_order_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "cancel_notification":
            template, context = await self.handle_cancel_notification(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "check_destination":
            template, context = await self.handle_check_destination(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "check_order_type_destination":
            template, context = await self.handle_check_order_type_destination(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "export_forecast":
            return await self.handle_export_forecast(request)
        elif step == "update_delivery_type_all":
            template, context = await self.handle_update_delivery_type(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_container_delivery_type":
            template, context = await self.handle_update_container_delivery_type(
                request
            )
            return await sync_to_async(render)(request, template, context)

    async def handle_export_forecast(self, request: HttpRequest) -> tuple[Any, Any]:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "warehouse",
            )
            .values(
                "container_number__container_number",
                "customer_name__zem_name",
                "order_type",
                "vessel_id__vessel_eta",
                "vessel_id__vessel_etd",
                "cancel_time",
                "created_at",
                "retrieval_id__retrieval_carrier",
                "vessel_id__destination_port",
                "vessel_id__master_bill_of_lading",
                "warehouse__name",
                "container_number__container_type",
            )
            .filter(models.Q(container_number__container_number__in=selected_orders))
        )
        for order in orders:
            # 由于carrier的内容为中文，导出的文件中为乱码，所以修改编码，但是这段代码并没有解决编码问题，依旧是乱码，没有找到解决方案
            if order.get("retrieval_id__retrieval_carrier"):
                raw_data = order["retrieval_id__retrieval_carrier"]
                raw_data = raw_data.encode("utf-8")
                encoding = chardet.detect(raw_data)["encoding"]
                order["retrieval_id__retrieval_carrier"] = raw_data.decode(encoding)

        df = pd.DataFrame(orders)
        df = df.rename(
            {
                "container_number__container_number": "container",
                "customer_name__zem_name": "customer",
                "vessel_id__master_bill_of_lading": "MBL",
                "vessel_id__destination_port": "destination_port",
                "vessel_id__vessel_eta": "ETA",
                "vessel_id__vessel_etd": "ETD",
                "retrieval_id__retrieval_carrier": "carrier",
                "container_number__container_type": "container_type",
                "order_type": "order_type",
            },
            axis=1,
        )

        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename=forecast.csv"
        df.to_csv(path_or_buf=response, index=False, encoding="utf-8-sig")
        return response

    async def handle_order_basic_info_get(self) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.id for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "container_number__packinglist",
                "retrieval_id ",
            )
            .values(
                "container_number__container_number",
                "customer_name__zem_name",
                "vessel_id",
                "order_type",
                "retrieval_id__retrieval_destination_area",
                "packing_list_updloaded",
                "cancel_notification",
            )
            .filter(
                models.Q(created_at__gte="2024-08-19")
                | models.Q(container_number__container_number__in=ADDITIONAL_CONTAINER)
            )
        )
        unfinished_orders = []
        for o in orders:
            if not o.get("vessel_id") or not o.get("packing_list_updloaded"):
                if not o.get("cancel_notification"):
                    unfinished_orders.append(o)
        context = {
            "customers": customers,
            "order_type": self.order_type,
            "area": self.area,
            "container_type": self.container_type,
            "unfinished_orders": unfinished_orders,
        }
        return self.template_order_create_base, context

    async def handle_order_supplemental_info_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        _, context = await self.handle_order_basic_info_get()
        container_number = request.GET.get("container_number")
        order = await sync_to_async(
            Order.objects.select_related(
                "customer_name",
                "container_number",
                "retrieval_id",
                "vessel_id",
            ).get
        )(container_number__container_number=container_number)
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(
                models.Q(container_number__container_number=container_number)
            )
        )
        try:
            vessel = await sync_to_async(Vessel.objects.get)(
                order__container_number__container_number=container_number
            )
        except:
            vessel = []
        if vessel and order.packing_list_updloaded:
            return await self.handle_order_basic_info_get()
        context["selected_order"] = order
        context["packing_list"] = packing_list
        context["vessel"] = vessel
        context["shipping_lines"] = SHIPPING_LINE_OPTIONS
        context["delivery_options"] = DELIVERY_METHOD_OPTIONS
        context["delivery_types"] = [
            ("", ""),
            ("公仓", "public"),
            ("其他", "other"),
        ]
        context["packing_list_upload_form"] = UploadFileForm()
        return self.template_order_create_supplement, context

    async def handle_order_management_list_get(
        self,
        start_date_eta: str = None,
        end_date_eta: str = None,
        start_date_etd: str = None,
        end_date_etd: str = None,
    ) -> tuple[Any, Any]:
        start_date_eta = (
            (datetime.now().date() + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date_eta and not start_date_etd
            else start_date_eta
        )
        end_date_eta = (
            (datetime.now().date() + timedelta(days=30)).strftime("%Y-%m-%d")
            if not end_date_eta and not end_date_etd
            else end_date_eta
        )
        criteria = None
        if start_date_eta and end_date_eta:
            criteria = models.Q(
                vessel_id__vessel_eta__gte=start_date_eta,
                vessel_id__vessel_eta__lte=end_date_eta,
            ) | models.Q(created_at__gte=start_date_eta, created_at__lte=end_date_eta)
        if start_date_etd:
            if end_date_etd:
                if criteria == None:
                    criteria = models.Q(
                        vessel_id__vessel_etd__gte=start_date_etd,
                        vessel_id__vessel_etd__lte=end_date_etd,
                    )
                else:
                    criteria &= models.Q(
                        vessel_id__vessel_etd__gte=start_date_etd,
                        vessel_id__vessel_etd__lte=end_date_etd,
                    )
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            ).filter(criteria)
        )
        # 判断一下柜子的状态
        for order in orders:
            if order.cancel_notification:
                status = "已取消"
            elif not order.add_to_t49:
                status = "T49待追踪"
            else:
                if not order.retrieval_id.actual_retrieval_timestamp:
                    if not order.retrieval_id.retrieval_carrier:
                        status = "待预约提柜"
                    else:
                        status = "待确认提柜"
                else:
                    if not order.retrieval_id.arrive_at_destination:
                        status = "待确认到仓"
                    else:
                        if not order.offload_id.offload_at:
                            status = "确认拆柜"
                        elif order.offload_id.offload_at:
                            status = "已拆柜"
                        else:
                            status = "未知"
            order.status = status
        context = {
            "orders": orders,
            "start_date_eta": start_date_eta,
            "end_date_eta": end_date_eta,
            "start_date_etd": start_date_etd,
            "end_date_etd": end_date_etd,
        }
        return self.template_order_list, context

    async def handle_order_management_container_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.id for c in customers}
        order = await sync_to_async(
            Order.objects.select_related(
                "customer_name",
                "container_number",
                "retrieval_id",
                "vessel_id",
                "warehouse",
                "offload_id",
                "shipment_id",
            ).get
        )(container_number__container_number=container_number)
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(
                models.Q(container_number__container_number=container_number)
            )
        )
        offload = order.offload_id
        context = {
            "selected_order": order,
            "packing_list": packing_list,
            "vessel": order.vessel_id,
            "retrieval": order.retrieval_id,
            "shipping_lines": SHIPPING_LINE_OPTIONS,
            "delivery_options": DELIVERY_METHOD_OPTIONS,
            "packing_list_upload_form": UploadFileForm(),
            "order_type": self.order_type,
            "container_type": self.container_type,
            "customers": customers,
            "area": self.area,
            "offload_at": offload.offload_at,
            "cancel_access": await sync_to_async(
                request.user.groups.filter(name="create_order").exists
            )(),
        }
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["delivery_types"] = [
            ("", ""),
            ("公仓", "public"),
            ("其他", "other"),
        ]
        non_combina_region = getattr(request, "non_combina_region", 0)
        combina_region = getattr(request, "combina_region", 0)
        abnormal_container = getattr(request, "abnormal_container", 0)
        if combina_region or non_combina_region:
            context["check_destination"] = True
            context["non_combina_region"] = non_combina_region
            context["combina_region"] = combina_region
        else:
            context["check_destination"] = False
            context["non_combina_region"] = non_combina_region
            context["combina_region"] = combina_region
        context["abnormal_container"] = abnormal_container
        return self.template_order_details, context

    async def handle_create_order_basic_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        customer_id = request.POST.get("customer")
        customer = await sync_to_async(Customer.objects.get)(id=customer_id)
        created_at = datetime.now()
        order_type = request.POST.get("order_type")
        area = request.POST.get("area")
        destination = request.POST.get("destination")
        container_number = request.POST.get("container_number")
        try:
            existing_order = await sync_to_async(Order.objects.get)(
                container_number__container_number=container_number
            )
            if existing_order:
                # 如果柜号已经存在，就将旧柜号后面加_年月
                old_created_at = await sync_to_async(
                    lambda: existing_order.created_at
                )()
                year_month = old_created_at.strftime("%Y%m")
                old_container = await sync_to_async(
                    lambda: existing_order.container_number
                )()
                old_container.container_number = (
                    f"{old_container.container_number}_{year_month}"
                )
                await sync_to_async(old_container.save)()
        except ObjectDoesNotExist:
            pass
        if await sync_to_async(list)(
            Order.objects.filter(container_number__container_number=container_number)
        ):
            raise RuntimeError(f"Container {container_number} exists!")
        weight = float(request.POST.get("weight"))
        weight_unit = request.POST.get("weight_unit")
        if weight_unit == "kg":
            weight *= 2.20462
        is_special_container = (
            True if request.POST.get("is_special_container", None) else False
        )
        order_id = str(
            uuid.uuid3(
                uuid.NAMESPACE_DNS,
                str(uuid.uuid4())
                + customer.zem_name
                + created_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
        retrieval_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + container_number))
        offload_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + order_type))

        container_data = {
            "container_number": request.POST.get("container_number").upper().strip(),
            "container_type": request.POST.get("container_type"),
            "weight_lbs": weight,
            "is_special_container": is_special_container,
            "note": request.POST.get("note"),
        }
        container = Container(**container_data)
        retrieval_data = {
            "retrieval_id": retrieval_id,
            "retrieval_destination_area": (
                area if order_type in ("转运", "转运组合") else destination
            ),
        }
        retrieval = Retrieval(**retrieval_data)
        offload_data = {
            "offload_id": offload_id,
            "offload_required": True if order_type in ("转运", "转运组合") else False,
        }
        offload = Offload(**offload_data)
        order_data = {
            "order_id": order_id,
            "customer_name": customer,
            "created_at": created_at,
            "order_type": order_type,
            "container_number": container,
            "retrieval_id": retrieval,
            "offload_id": offload,
            "packing_list_updloaded": False,
        }
        order = Order(**order_data)
        await sync_to_async(container.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(offload.save)()
        await sync_to_async(order.save)()
        return await self.handle_order_basic_info_get()

    async def handle_update_order_basic_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        # check if container number is changed
        input_container_number = request.POST.get("container_number")
        original_container_number = request.POST.get("original_container_number")
        order = await sync_to_async(
            Order.objects.select_related(
                "customer_name",
                "container_number",
                "retrieval_id",
                "vessel_id",
                "offload_id",
            ).get
        )(container_number__container_number=original_container_number)
        container = order.container_number
        retrieval = order.retrieval_id
        offload = order.offload_id
        if input_container_number != original_container_number:
            # check if the input container exists
            new_container = await sync_to_async(list)(
                Container.objects.filter(container_number=input_container_number)
            )
            if new_container:
                raise ValueError(f"container {input_container_number} exists!")
            else:
                container.container_number = input_container_number
        container.container_type = request.POST.get("container_type")
        container.weight_lbs = request.POST.get("weight")
        container.is_special_container = (
            True if request.POST.get("is_special_container", None) else False
        )
        if not request.POST.get("is_special_container", None):
            container.note = ""
        else:
            container.note = request.POST.get("note")

        # check cunstomer
        input_customer_id = request.POST.get("customer")
        original_customer_id = request.POST.get("original_customer")
        if input_customer_id != original_customer_id:
            order.customer_name = await sync_to_async(Customer.objects.get)(
                id=input_customer_id
            )

        # check order_type
        input_order_type = request.POST.get("order_type")
        original_order_type = request.POST.get("original_order_type")
        if input_order_type == original_order_type:
            # order type not changed
            if original_order_type == "直送":
                # update destination
                retrieval.retrieval_destination_area = (
                    request.POST.get("destination").upper().strip()
                )
            else:
                # update retrieval area
                retrieval.retrieval_destination_area = request.POST.get("area")
        else:
            order.order_type = input_order_type
            if original_order_type == "直送":
                # DD to TD
                offload.offload_required = True
                retrieval.retrieval_destination_area = request.POST.get("area")
                order.packing_list_updloaded = True
            else:
                if input_order_type == "直送":
                    # TD/转运组合 to DD
                    offload.offload_required = False
                    retrieval.retrieval_destination_area = (
                        request.POST.get("destination").upper().strip()
                    )

        await sync_to_async(offload.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(container.save)()
        await sync_to_async(order.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container.container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        source = request.POST.get("source")
        if source == "order_management":
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_supplemental_info_get(request)

    async def handle_update_order_shipping_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        if request.POST.get("is_vessel_created").upper().strip() == "YES":
            vessel = await sync_to_async(Vessel.objects.get)(
                models.Q(order__container_number__container_number=container_number)
            )
            vessel.master_bill_of_lading = request.POST.get("mbl").upper().strip()
            vessel.destination_port = request.POST.get("pod").upper().strip()
            vessel.shipping_line = request.POST.get("shipping_line").strip()
            vessel.vessel = request.POST.get("vessel").upper().strip()
            vessel.voyage = request.POST.get("voyage").upper().strip()
            vessel.vessel_eta = request.POST.get("eta")
            vessel.vessel_etd = request.POST.get("etd")
            await sync_to_async(vessel.save)()
        else:
            order = await sync_to_async(Order.objects.get)(
                models.Q(container_number__container_number=container_number)
            )
            vessel_id = str(
                uuid.uuid3(
                    uuid.NAMESPACE_DNS, container_number + request.POST.get("mbl")
                )
            )
            vessel = Vessel(
                vessel_id=vessel_id,
                master_bill_of_lading=request.POST.get("mbl").upper().strip(),
                destination_port=request.POST.get("pod").upper().strip(),
                shipping_line=request.POST.get("shipping_line"),
                vessel=request.POST.get("vessel").upper().strip(),
                voyage=request.POST.get("voyage").upper().strip(),
                vessel_eta=request.POST.get("eta"),
                vessel_etd=request.POST.get("etd"),
            )
            await sync_to_async(vessel.save)()
            order.vessel_id = vessel
            await sync_to_async(order.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        source = request.POST.get("source")
        if source == "order_management":
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_supplemental_info_get(request)

    async def handle_update_order_retrieval_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(Order.objects.select_related("retrieval_id").get)(
            container_number__container_number=container_number
        )
        destination = request.POST.get("retrieval_destination_precise")
        order_type = order.order_type
        if order_type == "转运" or order_type == "转运组合":
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=destination)
            order.warehouse = warehouse
            await sync_to_async(order.save)()
        retrieval = await sync_to_async(Retrieval.objects.get)(
            models.Q(retrieval_id=order.retrieval_id)
        )
        tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
        retrieval.retrieval_carrier = request.POST.get("retrieval_carrier")
        retrieval.retrieval_destination_precise = request.POST.get(
            "retrieval_destination_precise"
        )
        target_retrieval_timestamp = request.POST.get("target_retrieval_timestamp")
        retrieval.target_retrieval_timestamp = (
            self._parse_ts(target_retrieval_timestamp, tzinfo)
            if target_retrieval_timestamp
            else None
        )
        actual_retrieval_timestamp = request.POST.get("actual_retrieval_timestamp")
        retrieval.actual_retrieval_timestamp = (
            self._parse_ts(actual_retrieval_timestamp, tzinfo)
            if actual_retrieval_timestamp
            else None
        )

        arrive_at = request.POST.get("arrive_at")
        retrieval.arrive_at = self._parse_ts(arrive_at, tzinfo) if arrive_at else None
        empty_returned_at = request.POST.get("empty_returned_at")
        retrieval.empty_returned_at = (
            self._parse_ts(empty_returned_at, tzinfo) if empty_returned_at else None
        )

        retrieval.note = request.POST.get("retrieval_note").strip()
        await sync_to_async(retrieval.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        return await self.handle_order_management_container_get(request)

    async def handle_update_container_delivery_type(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        containers = await sync_to_async(list)(Container.objects.all())

        for container in containers:
            pallets = await sync_to_async(list)(
                Pallet.objects.filter(container_number=container)
            )
            if not pallets:
                pallets = await sync_to_async(list)(
                    PackingList.objects.filter(container_number=container)
                )
            types = set(plt.delivery_type for plt in pallets if plt.delivery_type)

            if not types:
                continue
            new_type = types.pop() if len(types) == 1 else "mixed"
            container.delivery_type = new_type
            await sync_to_async(container.save, thread_sensitive=True)()
        return await self.handle_order_management_container_get(request)

    async def handle_update_delivery_type(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:

        # 处理PackingList
        batch_size = 10000
        queryset = PackingList.objects.filter(delivery_type__isnull=True).order_by("id")
        total = await sync_to_async(queryset.count)()

        for start in range(0, total, batch_size):
            batch = await sync_to_async(list)(
                queryset[start : start + batch_size].values("id", "destination")
            )

            public_ids = [
                item["id"]
                for item in batch
                if (
                    re.fullmatch(r"^[A-Za-z]{4}\s*$", str(item["destination"]))
                    or re.fullmatch(r"^[A-Za-z]{3}\s*\d$", str(item["destination"]))
                    or re.fullmatch(
                        r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$", str(item["destination"])
                    )
                    or any(
                        kw.lower() in str(item["destination"]).lower()
                        for kw in {"walmart", "沃尔玛"}
                    )
                )
            ]

            # 批量更新
            if public_ids:
                await sync_to_async(
                    PackingList.objects.filter(id__in=public_ids).update
                )(delivery_type="public")
            other_ids = [item["id"] for item in batch if item["id"] not in public_ids]
            if other_ids:
                await sync_to_async(
                    PackingList.objects.filter(id__in=other_ids).update
                )(delivery_type="other")

        plt_size = 10000
        queryset = Pallet.objects.filter(delivery_type__isnull=True).order_by("id")
        total = await sync_to_async(queryset.count)()

        for start in range(0, total, plt_size):
            batch_plt = await sync_to_async(list)(
                queryset[start : start + plt_size].values("id", "destination")
            )

            public_ids = [
                item["id"]
                for item in batch_plt
                if (
                    re.fullmatch(r"^[A-Za-z]{4}\s*$", str(item["destination"]))
                    or re.fullmatch(r"^[A-Za-z]{3}\s*\d$", str(item["destination"]))
                    or re.fullmatch(
                        r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$", str(item["destination"])
                    )
                    or any(
                        kw.lower() in str(item["destination"]).lower()
                        for kw in {"walmart", "沃尔玛"}
                    )
                )
            ]

            # 批量更新
            if public_ids:
                await sync_to_async(Pallet.objects.filter(id__in=public_ids).update)(
                    delivery_type="public"
                )
            other_ids = [
                item["id"] for item in batch_plt if item["id"] not in public_ids
            ]
            if other_ids:
                await sync_to_async(
                    PackingList.objects.filter(id__in=other_ids).update
                )(delivery_type="other")

        return await self.handle_order_management_container_get(request)

    async def handle_update_order_packing_list_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(
            Order.objects.select_related(
                "container_number", "offload_id", "vessel_id"
            ).get
        )(container_number__container_number=container_number)
        container = order.container_number
        offload = order.offload_id
        if (
            offload.offload_at and "pl_id" in request.POST
        ):  # 原本是offload.offload_at，但是打板后如果是上传的文件，是没有pl_id的
            updated_pl = []
            pl_ids = request.POST.getlist("pl_id")
            pl_id_idx_mapping = {int(pl_ids[i]): i for i in range(len(pl_ids))}
            packing_list = await sync_to_async(list)(
                PackingList.objects.filter(
                    container_number__container_number=container_number
                )
            )
            destination_list = request.POST.getlist("destination")
            for idx, destination in enumerate(destination_list):
                if "WALMART" in destination.upper():
                    parts = destination.split("-")
                    destination_list[idx] = "Walmart-" + parts[1]
                else:
                    destination_list[idx] = destination.upper().strip()
            for pl in packing_list:
                idx = pl_id_idx_mapping[pl.id]
                pl.product_name = request.POST.getlist("product_name")[idx]
                pl.delivery_method = request.POST.getlist("delivery_method")[idx]
                pl.delivery_type = request.POST.getlist("delivery_type")[idx]
                pl.shipping_mark = request.POST.getlist("shipping_mark")[idx].strip()
                pl.fba_id = request.POST.getlist("fba_id")[idx].strip()
                pl.ref_id = request.POST.getlist("ref_id")[idx].strip()
                pl.destination = destination_list[idx]
                pl.contact_name = request.POST.getlist("contact_name")[idx]
                pl.contact_method = request.POST.getlist("contact_method")[idx]
                pl.address = request.POST.getlist("address")[idx]
                pl.zipcode = request.POST.getlist("zipcode")[idx]
                pl.note = request.POST.getlist("note")[idx]
                updated_pl.append(pl)
            await sync_to_async(bulk_update_with_history)(
                updated_pl,
                PackingList,
                fields=[
                    "product_name",
                    "delivery_method",
                    "delivery_type",
                    "shipping_mark",
                    "fba_id",
                    "ref_id",
                    "destination",
                    "contact_name",
                    "contact_method",
                    "address",
                    "zipcode",
                    "note",
                ],
            )
        else:
            await sync_to_async(
                PackingList.objects.filter(
                    container_number__container_number=container_number
                ).delete
            )()
            destination_list = request.POST.getlist("destination")
            for idx, destination in enumerate(destination_list):
                if "WALMART" in destination.upper():
                    parts = destination.split("-")
                    destination_list[idx] = "Walmart-" + parts[1]
                else:
                    destination_list[idx] = destination.upper().strip()
            # Generate PO_ID
            po_ids = []
            po_id_hash = {}
            seq_num = 1
            for dm, sm, fba, dest in zip(
                request.POST.getlist("delivery_method"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                destination_list,
                strict=True,
            ):
                po_id: str = ""
                po_id_seg: str = ""
                po_id_hkey: str = ""
                if dm in ["暂扣留仓(HOLD)", "暂扣留仓"]:
                    po_id_hkey = f"{dm}-{fba}"
                    po_id_seg = (
                        f"H{fba[-4:]}"
                        if fba
                        else f"H{''.join(random.choices(string.ascii_letters.upper() + string.digits, k=4))}"
                    )
                elif dm == "客户自提" or dest == "客户自提":
                    po_id_hkey = f"{dm}-{dest}-{sm}"
                    po_id_seg = (
                        f"S{sm[-4:]}"
                        if sm
                        else f"S{''.join(random.choices(string.ascii_letters.upper() + string.digits, k=4))}"
                    )
                else:
                    po_id_hkey = f"{dm}-{dest}"
                    po_id_seg = f"{DELIVERY_METHOD_CODE.get(dm, 'UN')}{dest.replace(' ', '').split('-')[-1]}"
                if po_id_hkey in po_id_hash:
                    po_id = po_id_hash.get(po_id_hkey)
                else:
                    po_id = f"{container_number[-4:]}{po_id_seg}{seq_num}"
                    po_id = re.sub(r"[\u4e00-\u9fff]", "", po_id)
                    po_id_hash[po_id_hkey] = po_id
                    seq_num += 1
                po_ids.append(po_id)
            del po_id_hash, po_id, po_id_seg, po_id_hkey
            pl_data = zip(
                request.POST.getlist("product_name"),
                request.POST.getlist("delivery_method"),
                request.POST.getlist("delivery_type"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                request.POST.getlist("ref_id"),
                destination_list,
                request.POST.getlist("contact_name"),
                request.POST.getlist("contact_method"),
                request.POST.getlist("address"),
                request.POST.getlist("zipcode"),
                request.POST.getlist("pcs"),
                request.POST.getlist("total_weight_kg"),
                request.POST.getlist("total_weight_lbs"),
                request.POST.getlist("cbm"),
                request.POST.getlist("note"),
                po_ids,
                strict=True,
            )

            pl_to_create = [
                PackingList(
                    container_number=container,
                    product_name=d[0],
                    delivery_method=d[1],
                    delivery_type=d[2],
                    shipping_mark=d[3].strip(),
                    fba_id=d[4].strip(),
                    ref_id=d[5].strip(),
                    destination=d[6],
                    contact_name=d[7],
                    contact_method=d[8],
                    address=d[9],
                    zipcode=d[10],
                    pcs=d[11],
                    total_weight_kg=d[12],
                    total_weight_lbs=d[13],
                    cbm=d[14],
                    note=d[15],
                    PO_ID=d[16],
                )
                for d in pl_data
            ]
            await sync_to_async(bulk_create_with_history)(pl_to_create, PackingList)
            # await sync_to_async(PackingList.objects.bulk_create)(pl_to_create)
            order.packing_list_updloaded = True
            await sync_to_async(order.save)()
        # 查找新建的pl，和现在的pocheck比较，如果内容没有变化，pocheck该记录不变，如果有变化就对应修改

        # 因为上面已经将新的packing_list存到表里，所以直接去pl表查
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(container_number__container_number=container)
        )
        po_checks = await sync_to_async(list)(
            PoCheckEtaSeven.objects.filter(container_number__container_number=container)
        )
        if len(po_checks) == 0:
            # po_check没有这个柜子，直接新建
            for pl in packing_list:
                po_check_dict = {
                    "container_number": container,
                    "vessel_eta": order.vessel_id.vessel_eta,
                    "packing_list": pl,
                    "time_status": True,
                    "destination": pl.destination,
                    "fba_id": pl.fba_id,
                    "ref_id": pl.ref_id,
                    "shipping_mark": pl.shipping_mark,
                    # 其他的字段用默认值
                }
                new_obj = PoCheckEtaSeven(**po_check_dict)
                await sync_to_async(new_obj.save)()
        else:
            for pl in packing_list:
                # flag_num用来表示该pl是否在po_check中找到相同的记录，如果没找到，就在po_check新建这条，如果找到，就让po指向pl
                flag_num = 0
                for po in po_checks:
                    if (
                        (pl.shipping_mark == po.shipping_mark)
                        and (pl.fba_id == po.fba_id)
                        and (pl.ref_id == po.ref_id)
                        and (pl.fba_id == po.fba_id)
                        and (pl.destination == po.destination)
                    ):
                        flag_num = 1
                        # 这里的判断是因为，pl和po判断相同的标准是唛头fba和目的地ref，可能pl中有多条这三个条件相同给的
                        # 但是对于po_check表来说，是用来验证po的ref的，而且po_check只存了这三个关键信息表示pl，所以po_check无所谓指向具体的哪一条pl
                        # 只要唛头fba目的地ref相同就行了，所以这里，每次遇到第一个po和pl相同且po没有指向pl，就令po指向这条pl
                        check_packing_list = sync_to_async(
                            lambda: bool(po.packing_list) == 0
                        )
                        is_empty = await check_packing_list()
                        if is_empty:
                            po.packing_list = pl
                            await sync_to_async(po.save)()
                            break
                if flag_num == 0:
                    # 如果po_check表没有这条po，新建这一条
                    po_check_dict = {
                        "container_number": container,
                        "vessel_eta": order.vessel_id.vessel_eta,
                        "packing_list": pl,
                        "time_status": True,
                        "destination": pl.destination,
                        "fba_id": pl.fba_id,
                        "ref_id": pl.ref_id,
                        "shipping_mark": pl.shipping_mark,
                        # 其他的字段用默认值
                    }
                    new_obj = PoCheckEtaSeven(**po_check_dict)
                    await sync_to_async(new_obj.save)()

            try:
                # 对于po_check没有指向pl的，就删除
                queryset = await sync_to_async(PoCheckEtaSeven.objects.filter)(
                    container_number__container_number=container, packing_list=None
                )
                for obj in await sync_to_async(list)(queryset):
                    # 对每个对象执行删除操作
                    await sync_to_async(obj.delete)()
            except PoCheckEtaSeven.DoesNotExist:
                raise ValueError("不存在")
        # 更新完pl之后，更新container的delivery_type
        types = set(pl.delivery_type for pl in packing_list if pl.delivery_type)
        if not types:
            raise ValueError("缺少派送类型")
        new_type = types.pop() if len(types) == 1 else "mixed"
        container = await sync_to_async(Container.objects.get, thread_sensitive=True)(
            container_number=container_number
        )
        container.delivery_type = new_type
        await sync_to_async(container.save, thread_sensitive=True)()

        source = request.POST.get("source")
        if source == "order_management":
            mutable_get = request.GET.copy()
            mutable_get["container_number"] = container_number
            mutable_get["step"] = "container_info_supplement"
            request.GET = mutable_get
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_basic_info_get()

    def find_matching_regions(
        self, plts_by_destination: dict, combina_fee: dict, container_type
    ) -> dict:
        non_combina_dests = set()
        price_display = defaultdict(set)

        for plts in plts_by_destination:
            dest = plts["destination"]
            dest = dest.replace("沃尔玛", "").split("-")[-1].strip()
            matched = False
            # 遍历所有区域和location
            for region, fee_data_list in combina_fee.items():
                for fee_data in fee_data_list:
                    if dest in fee_data["location"]:
                        price_display[region].add(dest)
                        matched = True

            # 记录匹配结果
            if not matched:
                non_combina_dests.add(dest)  # 未匹配的仓点
        combina_dests = {k: list(v) for k, v in price_display.items()}
        return {
            "combina_dests": combina_dests,
            "non_combina_dests": non_combina_dests,
        }

    async def handle_check_order_type_destination(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        orders = await sync_to_async(list)(
            Order.objects.filter(~models.Q(order_type="直送"))
            .select_related("container_number")  # 优化查询性能
            .values_list("container_number__container_number", flat=True)
            .distinct()  # 确保柜号唯一
        )

        matched_containers = []

        for container_number in orders:
            destinations = await sync_to_async(
                lambda: list(
                    PackingList.objects.filter(
                        container_number__container_number=container_number
                    )
                    .values_list("destination", flat=True)
                    .distinct()
                )
            )()
            if len(destinations) == 1:
                matched_containers.append(container_number)
        request.abnormal_container = matched_containers
        return await self.handle_order_management_container_get(request)

    async def handle_check_destination(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(Order.objects.get)(
            models.Q(container_number__container_number=container_number)
        )
        # 准备参数
        vessel_etd = await sync_to_async(lambda: order.vessel_id.vessel_etd)()
        warehouse = await sync_to_async(
            lambda: order.retrieval_id.retrieval_destination_area
        )()

        container = await sync_to_async(Container.objects.get)(
            container_number=container_number
        )
        container_type = container.container_type
        container_type = 0 if container_type == "40HQ/GP" else 1

        # 找报价表
        matching_quotation = await sync_to_async(
            QuotationMaster.objects.filter(effective_date__lte=vessel_etd)
            .order_by("-effective_date")
            .first
        )()
        if not matching_quotation:
            return ValueError("找不到报价表")
        # 找组合柜报价
        combina_fee_detail = await FeeDetail.objects.aget(
            quotation_id=matching_quotation.id, fee_type=f"{warehouse}_COMBINA"
        )
        combina_fee = combina_fee_detail.details

        plts_by_destination = await sync_to_async(
            lambda: list(
                PackingList.objects.filter(
                    container_number__container_number=container_number
                ).values("destination")
            )
        )()
        matched_regions = self.find_matching_regions(
            plts_by_destination, combina_fee, container_type
        )
        matched_regions["combina_dests"] = matched_regions["combina_dests"]
        matched_regions["non_combina_dests"] = list(
            matched_regions["non_combina_dests"]
        )
        # 非组合柜区域
        request.non_combina_region = matched_regions["non_combina_dests"]
        # 组合柜区域
        request.combina_region = matched_regions["combina_dests"]

        return await self.handle_order_management_container_get(request)

    async def handle_cancel_notification(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        # 查询order表的contain_number
        order = await sync_to_async(Order.objects.get)(
            models.Q(container_number__container_number=container_number)
        )
        order.cancel_notification = True
        order.cancel_time = datetime.now()
        await sync_to_async(order.save)()
        # 如果取消预报了，po_check也要做对应处理，但是怕可能会有取消预报后又不想取消的情况，现在不在po_check表删除，把vessel_eta改成2024/1/2
        orders = await sync_to_async(list)(
            PoCheckEtaSeven.objects.filter(
                container_number__container_number=container_number
            )
        )
        try:
            for o in orders:
                o.vessel_eta = datetime(2024, 1, 2)
                await sync_to_async(o.save)()
        except PoCheckEtaSeven.DoesNotExist:
            pass
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "cancel_notification"
        request.GET = mutable_get
        return await self.handle_order_management_container_get(request)

    async def handle_upload_template_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            df = df.rename(columns=PACKING_LIST_TEMP_COL_MAPPING)
            df = df.dropna(
                how="all",
                subset=[c for c in df.columns if c not in ["delivery_method", "note"]],
            )  # 除了delivery_method和note，删除其他列为空的
            df = df.replace(np.nan, "")
            df = df.reset_index(drop=True)
            if df["cbm"].isna().sum():  # 检查这几个字段是否为空，为空就报错
                raise ValueError(f"cbm number N/A error!")
            if (
                df["total_weight_lbs"].isna().sum()
                and df["total_weight_kg"].isna().sum()
            ):
                raise ValueError(f"weight number N/A error!")
            if df["pcs"].isna().sum():
                raise ValueError(f"boxes number N/A error!")
            for idx, row in df.iterrows():  # 转换单位
                if row["unit_weight_kg"] and not row["unit_weight_lbs"]:
                    df.loc[idx, "unit_weight_lbs"] = round(
                        df.loc[idx, "unit_weight_kg"] * 2.20462, 2
                    )
                if row["total_weight_kg"] and not row["total_weight_lbs"]:
                    df.loc[idx, "total_weight_lbs"] = round(
                        df.loc[idx, "total_weight_kg"] * 2.20462, 2
                    )
            df["destination"] = df["destination"].str.strip()
            # 通过正则判断是否是公仓，公仓就是public，否则就是other
            df["delivery_type"] = df["destination"].apply(
                lambda x: "public" if self.is_public_destination(x) else "other"
            )
            # model_fields获取pl模型的所有字段名
            model_fields = [field.name for field in PackingList._meta.fields]
            col = [c for c in df.columns if c in model_fields]
            pl_data = df[col].to_dict("records")
            packing_list = [PackingList(**data) for data in pl_data]
        else:
            raise ValueError(f"invalid file format!")
        source = request.POST.get("source")
        if source == "order_management":
            container_number = request.POST.get("container_number")
            mutable_get = request.GET.copy()
            mutable_get["container_number"] = container_number
            request.GET = mutable_get
            _, context = await self.handle_order_management_container_get(request)
            context["packing_list"] = packing_list
            return self.template_order_details_pl, context
        else:
            _, context = await self.handle_order_supplemental_info_get(request)
            context["packing_list"] = packing_list
            return self.template_order_create_supplement_pl_tab, context

    def is_public_destination(self, destination):
        if not isinstance(destination, str):
            return False
        pattern1 = r"^[A-Za-z]{3}\s*\d$"
        if re.match(pattern1, destination):
            return True
        pattern2 = r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$"
        if re.match(pattern2, destination):
            return True
        pattern3 = r"^[A-Za-z]{4}\s*$"
        if re.fullmatch(pattern3, destination):
            return True
        keywords = {"walmart", "沃尔玛"}
        destination_lower = destination.lower()
        return any(keyword.lower() in destination_lower for keyword in keywords)

    async def handle_download_template_post(self) -> HttpResponse:
        file_path = (
            Path(__file__)
            .parent.parent.parent.resolve()
            .joinpath("templates/export_file/packing_list_template.xlsx")
        )
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = (
                f'attachment; filename="zem_packing_list_template.xlsx"'
            )
            return response

    async def handle_delete_order_post(self, request: HttpRequest) -> tuple[Any, Any]:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        # 在这里进行订单删除操作，例如：
        for order_number in selected_orders:
            await sync_to_async(
                Order.objects.filter(
                    container_number__container_number=order_number
                ).delete
            )()
        start_date_eta = request.POST.get("start_date_eta")
        end_date_eta = request.POST.get("end_date_eta")
        start_date_etd = (
            request.POST.get("start_date_etd")
            if request.POST.get("start_date_etd")
            and request.POST.get("start_date_etd") != "None"
            else ""
        )
        end_date_etd = (
            request.POST.get("end_date_etd")
            if request.POST.get("end_date_etd")
            and request.POST.get("end_date_etd") != "None"
            else ""
        )
        return await self.handle_order_management_list_get(
            start_date_eta, end_date_eta, start_date_etd, end_date_etd
        )

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    def _parse_tzinfo(self, s: str | None) -> str:
        if not s:
            return "America/New_York"
        elif "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: str, tzinfo: str) -> str:
        ts_naive = datetime.fromisoformat(ts)
        tz = pytz.timezone(tzinfo)
        ts = tz.localize(ts_naive).astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
