import json
import os
import random
import re
import string
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone,date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict
from django.db.models import Sum, Count, FloatField, IntegerField
from django.db.models.functions import Coalesce, Cast, Round 
from itertools import zip_longest

import chardet
import numpy as np
import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Sum
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils.dateparse import parse_date
from django.utils import timezone
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
from warehouse.views.pre_port.pickup_containers_status import ContainerPickupStatus


class OrderCreation(View):
    # template_main = 'pre_port/create_order/01_order_creation_and_management.html'
    template_order_create_base = (
        "pre_port/create_order/02_base_order_creation_status.html"
    )
    template_peer_pallet_create_base = (
        "pre_port/create_order/base_peer_pallet_creation.html"
    )
    template_order_create_supplement = "pre_port/create_order/03_order_creation.html"
    template_order_create_supplement_pl_tab = (
        "pre_port/create_order/03_order_creation_packing_list_tab.html"
    )
    template_order_list = "order_management/order_list.html"
    template_order_details = "order_management/order_details.html"
    template_order_details_pl = "order_management/order_details_pl_tab.html"
    order_type = {"": "", "转运": "转运", "直送": "直送", "转运组合": "转运组合"}
    area = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX", "CA":"CA"}
    container_type = {
        "": "",
        "40HQ/GP": "40HQ/GP",
        "45HQ/GP": "45HQ/GP",
        "20GP": "20GP",
        "53HQ": "53HQ",
    }
    peer_customer = {
        'Vinmax': 'Vinmax',
        'OGR': 'OGR',
        'ARM': 'ARM',
        'X-HUB': 'X-HUB',
        'OL': 'OL',
        'Icejue': 'Icejue'
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
        elif step == "peer_po_creation":
            context = {}
            template, context = await self.handle_peer_po_creation(request,context)
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
        elif step == "peer_upload_template":
            template, context = await self.handle_peer_upload_template_post(request)
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
        elif step == "export_details_by_destination":
            return await self.handle_export_details_by_destination(request)
        elif step == "update_delivery_type_all":
            template, context = await self.handle_update_delivery_type(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_container_unpacking_priority":
            template, context = await self.handle_update_container_unpacking_priority(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step =="peer_download_template":
            return await self.handle_download_peer_template_post(request)
        elif step == "peer_po_save":
            template, context = await self.handle_peer_po_save(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "check_order_status":
            template, context = await self.check_order_status(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "modify_is_combina":
            raise ValueError('暂未开放修改计费权限')
    
    async def handle_download_peer_template_post(self, request: HttpRequest) -> HttpResponse:
        file_path = (
            Path(__file__)
            .parent.parent.parent.resolve()
            .joinpath("templates/export_file/peer_pallet_template.xlsx")
        )
        if not os.path.exists(file_path):
            raise Http404("File does not exist, i DONT KNOW")
        with open(file_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = (
                f'attachment; filename="zem_peer_pallet_template.xlsx"'
            )
            return response

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

    async def handle_export_details_by_destination(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "retrieval_id",
                "warehouse",
                "customer_name",
            )
            .values(
                "container_number__container_number",
                "vessel_id__vessel_etd",
                "retrieval_id__retrieval_destination_area",
                "customer_name__zem_name",
            )
            .filter(models.Q(container_number__container_number__in=selected_orders))
        )
        results = []
        for order in orders:
            container_number = order.get("container_number__container_number")
            vessel_etd = order.get("vessel_id__vessel_etd")
            warehouse = order.get("retrieval_id__retrieval_destination_area")

            # 找报价表
            customer_name = order.get("customer_name__zem_name")
            matching_quotation = await (
                QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=True,
                    exclusive_user=customer_name,
                    quote_type='receivable',
                )
                .order_by("-effective_date")
                .afirst()
            )

            if not matching_quotation:
                matching_quotation = await (
                    QuotationMaster.objects.filter(
                        effective_date__lte=vessel_etd,
                        is_user_exclusive=False,  # 非用户专属的通用报价单
                        quote_type='receivable',
                    )
                    .order_by("-effective_date")
                    .afirst()
                )
            if not matching_quotation:
                raise ValueError("找不到报价表")

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
                plts_by_destination, combina_fee
            )

            combina_keys = "+".join(matched_regions["combina_dests"].keys())
            non_combina_vals = ",".join(matched_regions["non_combina_dests"])
            ups_total_pcs = await sync_to_async(
                lambda: PackingList.objects.filter(
                    container_number__container_number=container_number,
                    destination__icontains="UPS",
                ).aggregate(total=Sum("pcs"))["total"]
            )()
            fxdex_total_pcs = await sync_to_async(
                lambda: PackingList.objects.filter(
                    container_number__container_number=container_number,
                    destination__icontains="FEDEX",
                ).aggregate(total=Sum("pcs"))["total"]
            )()
            results.append(
                {
                    "container_number": container_number,
                    "combina_dests": combina_keys,
                    "non_combina_dests": non_combina_vals,
                    "UPS": ups_total_pcs,
                    "FEDEX": fxdex_total_pcs,
                }
            )

        df = pd.DataFrame(
            results,
            columns=[
                "container_number",
                "combina_dests",
                "non_combina_dests",
                "UPS",
                "FEDEX",
            ],
        )
        df = df.rename(
            {
                "container_number": "柜号",
                "combina_dests": "组合柜的区",
                "non_combina_dests": "非组合柜仓点",
            },
            axis=1,
        )
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = (
            f"attachment; filename=destination_details.csv"
        )
        df.to_csv(path_or_buf=response, index=False, encoding="utf-8-sig")
        return response
    
    async def handle_peer_po_creation(self,request: HttpRequest,context: Dict[str, Any]) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.id for c in customers}
        context.update({
            "customers": customers,
            "order_type": self.order_type,
            "area": self.area,
        })

        return self.template_peer_pallet_create_base, context

    async def handle_order_basic_info_get(self) -> tuple[Any, Any]:
        customers = await sync_to_async(list)(Customer.objects.all())
        original_dict = {c.zem_name: c.id for c in customers}
        customers = {"": ""}
        customers.update(original_dict)
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "container_number__packinglist",
                "retrieval_id",
                "offload_id"
            ).values(
                "container_number__container_number",
                "container_number__weight_lbs",
                "container_number__container_type",
                "customer_name__zem_name",
                "vessel_id",
                "vessel_id__vessel_eta",
                "vessel_id__vessel_etd",
                "vessel_id__vessel",
                "vessel_id__shipping_line",
                "vessel_id__destination_port",
                "vessel_id__master_bill_of_lading",
                "order_type",
                "created_at",
                "retrieval_id__retrieval_destination_area",
                "packing_list_updloaded",
                "cancel_notification",
                "id",
            ).filter(
                (models.Q(created_at__gte=timezone.make_aware(datetime(2024, 8, 19)))
                 | models.Q(container_number__container_number__in=ADDITIONAL_CONTAINER))
                & models.Q(offload_id__offload_at__isnull=True)
            )
        )

        unfinished_orders = [
            o for o in orders
            if
            #基础信息不完整
            not o.get("customer_name__zem_name") or
            not o.get("order_type") or
            not o.get("created_at") or
            not o.get("container_number__container_number") or
            not o.get("vessel_id__master_bill_of_lading") or
            not o.get("vessel_id__vessel_eta") or
            # 航运信息不完整的情况
            not o.get("vessel_id") or
            not o.get("vessel_id__vessel") or
            not o.get("vessel_id__shipping_line") or
            not o.get("vessel_id__destination_port") or
            not o.get("container_number__container_type") or
            not o.get("vessel_id__vessel_etd") or
            # 未上传装箱单的情况
            not o.get("packing_list_updloaded")
        ]
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
        container = await sync_to_async(Container.objects.get)(
            container_number=container_number
        )
        if order.customer_name.zem_name and order.order_type and order.created_at and order.container_number.container_number and vessel.master_bill_of_lading and vessel.vessel_etd and vessel.id and vessel.vessel and vessel.shipping_line and vessel.destination_port and order.packing_list_updloaded and order.container_number.container_type and vessel.vessel_eta:
            order.status = "completed"
            await sync_to_async(order.save)()
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
        context["container_type"] = self.container_type
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
                            status = "待确认拆柜"
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
            container = await sync_to_async(Container.objects.get)(container_number=container_number)
            is_combina = getattr(request, "is_combina", 0)
            context["is_combina"] = is_combina
            context["non_combina_reason"] = container.non_combina_reason
            context["check_destination"] = True
            context["non_combina_region"] = non_combina_region 
            context["combina_region"] = combina_region
            context["quotation_file"] = getattr(request, "quotation_file")
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
        order_type = request.POST.get("order_type")
        area = request.POST.get("area")
        destination = request.POST.get("destination")
        container_number = request.POST.get("container_number")
        mbl = request.POST.get("mbl")
        etd_str = request.POST.get("etd")  # 原始字符串
        eta_str = request.POST.get("eta")  # 原始字符串
        is_expiry_guaranteed = (
            True if request.POST.get("is_expiry_guaranteed", None) else False
        )
        created_at_str = request.POST.get("created_at")

        # 处理日期格式：将纯日期转换为带时间的datetime，空值设为None
        def parse_date(date_str):
            if not date_str:  # 为空时返回None
                return None
            try:
                # 前端传的是日期（YYYY-MM-DD），转换为datetime（YYYY-MM-DD 00:00:00）
                return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.get_current_timezone())
            except ValueError:
                raise RuntimeError(f"无效的日期格式: {date_str}，请使用YYYY-MM-DD")

        # 解析日期（etd可为空，eta和created_at必传）
        etd = parse_date(etd_str)
        eta = parse_date(eta_str)
        created_at = parse_date(created_at_str)

        # 处理已存在的柜号
        try:
            existing_order = await sync_to_async(Order.objects.get)(
                container_number__container_number=container_number
            )
            if existing_order:
                old_created_at = await sync_to_async(lambda: existing_order.created_at)()
                year_month = old_created_at.strftime("%Y%m")
                old_container = await sync_to_async(lambda: existing_order.container_number)()
                old_container.container_number = f"{old_container.container_number}_{year_month}"
                await sync_to_async(old_container.save)()
        except ObjectDoesNotExist:
            pass

        if await sync_to_async(list)(
                Order.objects.filter(container_number__container_number=container_number)
        ):
            raise RuntimeError(f"Container {container_number} exists!")

        # 生成UUID
        order_id = str(
            uuid.uuid3(
                uuid.NAMESPACE_DNS,
                str(uuid.uuid4()) + customer.zem_name + created_at_str,
            )
        )
        retrieval_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + container_number))
        offload_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, order_id + order_type))
        vessel_id = str(
            uuid.uuid3(uuid.NAMESPACE_DNS, container_number + (mbl or ""))
        )

        # 保存集装箱
        container_data = {
            "container_number": request.POST.get("container_number").upper().strip(),
            "is_expiry_guaranteed": is_expiry_guaranteed,
            "note": request.POST.get("note"),
        }
        container = Container(**container_data)
        await sync_to_async(container.save)()

        # 保存船舶信息（允许etd为空）
        vessel_data = {
            "vessel_id": vessel_id,
            "master_bill_of_lading": mbl,
            "vessel_etd": etd,  # 可为None
            "vessel_eta": eta,  # 必传，已通过前端验证
        }
        vessel = Vessel(**vessel_data)
        await sync_to_async(vessel.save)()

        # 保存其他关联表
        retrieval_data = {
            "retrieval_id": retrieval_id,
            "retrieval_destination_area": (
                area if order_type in ("转运", "转运组合") else destination
            ),
        }
        retrieval = Retrieval(**retrieval_data)
        await sync_to_async(retrieval.save)()

        offload_data = {
            "offload_id": offload_id,
            "offload_required": True if order_type in ("转运", "转运组合") else False,
        }
        offload = Offload(**offload_data)
        await sync_to_async(offload.save)()

        # 保存订单
        order_data = {
            "order_id": order_id,
            "customer_name": customer,
            "created_at": created_at,
            "order_type": order_type,
            "container_number": container,
            "retrieval_id": retrieval,
            "offload_id": offload,
            "vessel_id": vessel,
            "packing_list_updloaded": False,
        }
        order = Order(**order_data)
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
        vessel = order.vessel_id
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
        vessel.master_bill_of_lading = request.POST.getlist('mbl')[0]
        weight = request.POST.get("weight")
        if weight:
            weight=float(weight)
            weight *= 2.20462
            container.weight_lbs = weight
        weight_lbs = request.POST.get("weight_lbs", "").strip()
        if weight_lbs in ("", "None"):
            container.weight_lbs = None
        else:
            try:
                container.weight_lbs = float(weight_lbs)
            except ValueError:
                container.weight_lbs = None
        container.is_special_container = (
            True if request.POST.get("is_special_container", None) else False
        )
        is_expiry_guaranteed = (
            True if request.POST.get("is_expiry_guaranteed", None) else False
        )
        container.is_expiry_guaranteed = is_expiry_guaranteed
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
        await sync_to_async(vessel.save)()
        await sync_to_async(order.save)()
        if is_expiry_guaranteed:
            #如果是保时效的，就重新判断一下优先级
            await self._update_container_unpacking_priority(input_container_number)
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container.container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        source = request.POST.get("source")
        if source == "order_management":
            return await self.handle_order_management_container_get(request)
        else:
            return await self.handle_order_supplemental_info_get(request)

    @sync_to_async
    def update_container_by_number(self, container_number, container_type):
        # 所有同步代码在这里执行，与异步上下文完全隔离
        Container.objects.filter(container_number=container_number).update(
            container_type=container_type
        )

    async def handle_update_order_shipping_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        container_type = request.POST.get("container_type")
        if request.POST.get("is_vessel_created").upper().strip() == "YES":
            vessel = await sync_to_async(Vessel.objects.get)(
                models.Q(order__container_number__container_number=container_number)
            )
            vessel.destination_port = request.POST.get("pod").upper().strip()
            vessel.shipping_line = request.POST.get("shipping_line").strip()
            vessel.vessel = request.POST.get("vessel").upper().strip()
            vessel.vessel_eta = request.POST.get("eta")
            vessel.vessel_etd = request.POST.get("etd")
            await sync_to_async(vessel.save)()
            await self.update_container_by_number(container_number, container_type)
        else:
            await self.update_container_by_number(container_number, container_type)
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
                destination_port=request.POST.get("pod").upper().strip(),
                shipping_line=request.POST.get("shipping_line"),
                vessel=request.POST.get("vessel").upper().strip(),
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
        if not empty_returned_at:
            retrieval.empty_returned = False
        retrieval.note = request.POST.get("retrieval_note").strip()
        await sync_to_async(retrieval.save)()
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = container_number
        mutable_get["step"] = "container_info_supplement"
        request.GET = mutable_get
        return await self.handle_order_management_container_get(request)

    #更新5月1之后所有柜子的优先级
    async def handle_update_container_unpacking_priority(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_numbers = await sync_to_async(
            lambda: list(
                Order.objects.filter(
                    created_at__date__gt=date(2025, 7, 1)
                ).values_list("container_number__container_number", flat=True).distinct()
            )
        )()

        for container_number in container_numbers:
            await self._update_container_unpacking_priority(container_number)

        return await self.handle_order_management_container_get(request)
    
    #给定一个柜号，判定这个柜子的优先级
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
        is_expiry_guaranteed = await sync_to_async(
            lambda: Container.objects.filter(
                container_number=container_number,
                is_expiry_guaranteed=True
            ).exists()
        )()
        if has_ups_fedex and is_expiry_guaranteed:
            priority = "P1"
        elif has_ups_fedex or is_expiry_guaranteed:
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

    async def check_order_status(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        order_id = request.POST.get("order_id")
        await Order.objects.filter(id=order_id).aupdate(status="checked")
        return await self.handle_order_management_container_get(request)

    async def handle_peer_po_save(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        customer_name = request.POST.get("customer_name")
        warehouse = request.POST.get("warehouse")
        warehouse_code = 'NJ-07001' if warehouse == 'NJ' else 'SAV-31326' if warehouse == 'SAV' else 'LA-91761' if warehouse == 'LA' else warehouse
        #更新基本信息
        customer = await sync_to_async(Customer.objects.get)(
            models.Q(zem_name=customer_name) | models.Q(accounting_name=customer_name)
        )
        created_at = datetime.now()
        order_type = '转运'
        #检查柜号是否重复，如果重复就重新起柜号名，这里重新起步报错是因为，这本来就是系统给随机起的
        is_modify_con = False
        try:
            existing_order = await sync_to_async(Order.objects.get)(
                container_number__container_number=container_number
            )
            if existing_order:
                container_number = await self._naming_container(customer_name)
                is_modify_con = True
        except ObjectDoesNotExist:
            pass

        weights = sum(float(weight) for weight in request.POST.getlist("total_weight"))
        weights *= 2.20462
        pallets = sum(int(plt) for plt in request.POST.getlist("total_pallet"))
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
            "container_number": container_number,
            "container_type": '40HQ/GP',
            "weight_lbs": weights,
            "is_special_container": 'False',
            "is_expiry_guaranteed": 'False',
            "note": '',
        }
        container = Container(**container_data)
        retrieval_data = {
            "retrieval_id": retrieval_id,
            "retrieval_destination_area": (request.POST.get("warehouse")),
            'retrieval_destination_precise': warehouse_code,
            'retrieval_carrier': '自取',
            'target_retrieval_timestamp': created_at,
            'target_retrieval_timestamp_lower': created_at,
            'scheduled_at': created_at,
            'actual_retrieval_timestamp': created_at,
            'arrive_at': created_at,
            'arrive_at_destination': 'True',
            'empty_returned': 'True',
            'empty_returned_at': created_at,
        }
        retrieval = Retrieval(**retrieval_data)
        offload_data = {
            "offload_id": offload_id,
            "offload_at": created_at,
            "offload_required": True ,
            'total_pallet': pallets,
        }
        offload = Offload(**offload_data)
        warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=warehouse_code)
        order_data = {
            "order_id": order_id,
            "customer_name": customer,
            "created_at": created_at,
            "order_type": order_type,
            "container_number": container,
            "retrieval_id": retrieval,
            "offload_id": offload,
            'warehouse': warehouse,
            'add_to_t49': 'True',
            "packing_list_updloaded": True,
            "unpacking_priority": 'P4',  #因为这是同行的货，都是卡派，默认就是P4，等到录完数据有约时，会自动判断改成P3
        }
        order = Order(**order_data)
        await sync_to_async(container.save)()
        await sync_to_async(retrieval.save)()
        await sync_to_async(offload.save)()
        await sync_to_async(order.save)()
        #然后要构建Packinglist和Pallet表
        order = await sync_to_async(
            Order.objects.select_related(
                "container_number", "offload_id", "vessel_id"
            ).get
        )(container_number__container_number=container_number)
        await self._create_packinglist(request,container_number,warehouse_code,order)
        context = {
            'is_modify_con':False,
            'is_save_con': True,
            'container_number': container_number,
        }
        return await self.handle_peer_po_creation(request,context)

    async def _create_packinglist(self, request:HttpRequest, container_number:str,warehouse_code:str,order:Order) -> None:
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
        for dm, sm, fba, dest in zip_longest(
                request.POST.getlist("delivery_method"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                destination_list,
                fillvalue='',
        ):
            po_id: str = ""
            po_id_seg: str = ""
            po_id_hkey: str = ""
            if dm in ["暂扣留仓(HOLD)", "暂扣留仓"]:
                po_id_hkey = f"{dm}-{dest}-{fba}"
                po_id_seg = (
                    f"H{fba[-4:]}{sm[-4:]}"
                    if fba
                    else f"H{sm[-4:] if sm else ''.join(random.choices(string.ascii_letters.upper() + string.digits, k=4))}"
                )
            elif dm == "客户自提" or dest == "客户自提":
                po_id_hkey = f"{dm}-{dest}-{fba}"
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
                random.seed(container_number[-4:])
                po_id = f"{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}{po_id_seg}{seq_num}"
                po_id = re.sub(r"[\u4e00-\u9fff]", "", po_id)
                po_id_hash[po_id_hkey] = po_id
                seq_num += 1
            po_ids.append(po_id)
        del po_id_hash, po_id, po_id_seg, po_id_hkey
        
        fields = [
            request.POST.getlist("delivery_method"),
            request.POST.getlist("shipping_mark") or [''],
            request.POST.getlist("fba_id"),
            request.POST.getlist("ref_id"),
            destination_list,
            request.POST.getlist("total_pcs"),
            request.POST.getlist("total_weight"),
            [str(float(weight) * 2.20462) if weight and weight.strip() else '' for weight in request.POST.getlist("total_weight")],
            request.POST.getlist("total_cbm"),
            request.POST.getlist("total_pallet"),
            po_ids,
            request.POST.getlist("delivery_window_start") or [''],
            request.POST.getlist("delivery_window_end") or [''],
            ["公仓"] * max(len(request.POST.getlist("delivery_method") or []), 1)
        ]
        #填充空列和最长的长度相同
        max_length = max(len(field) for field in fields)
        padded_fields = []
        for field in fields:
            if len(field) < max_length:
                padded_fields.append(field + [''] * (max_length - len(field)))
            else:
                padded_fields.append(field)
        pl_data = zip(*padded_fields, strict=True)
        pl_data = list(zip(*padded_fields, strict=True))
        pl_to_create = [
            PackingList(
                container_number=order.container_number,
                delivery_method=d[0],
                shipping_mark=d[1].strip(),
                fba_id=d[2].strip(),
                ref_id=d[3].strip(),
                destination=d[4],
                pcs=int(float(d[5])),
                total_weight_kg=d[6],
                total_weight_lbs=d[7],
                cbm=d[8],
                PO_ID=d[10],
                delivery_window_start = d[11] if d[11].strip() else None,
                delivery_window_end = d[12] if d[12].strip() else None,
                delivery_type=d[13],
            )
            for d in pl_data
        ]
        await sync_to_async(bulk_create_with_history)(pl_to_create, PackingList)

        #建完packinglist之后，再建pallet记录
        pallet_data = []
        for p in pl_data:
            pallet_data += await self._peer_split_pallet(order,int(p[5]),int(p[9]),float(p[8]),float(p[7]),p[4],p[0],p[13],p[1],p[2],p[3],p[10],warehouse_code,p[11],p[12])

        pallet_instances = [Pallet(**d) for d in pallet_data]
        await sync_to_async(bulk_create_with_history)(pallet_instances, Pallet)

    async def _peer_split_pallet(
        self,
        order: Order,
        p_a: int, #箱数
        n: int,  #板数
        c: float,
        w: float,
        destination: str,
        delivery_method: str,
        delivery_type: str,
        shipping_mark: str,
        fba_id: str,
        ref_id: str,
        po_id: str, 
        warehouse_code: str,
        dw_st:date | None = None,
        dw_end:date | None = None,      
        seed: int = 0,
    ) -> list[dict[str, Any]]:
        pallet_ids = [
            str(
                uuid.uuid3(
                    uuid.NAMESPACE_DNS, str(uuid.uuid4())  + str(i) + str(seed)
                )
            )
            for i in range(n)
        ]

        pallet_data_list = []
        pallet_pcs = [p_a // n for _ in range(n)]
        for i in range(p_a % n):
            pallet_pcs[i] += 1
        for i in range(n):
            cbm_loaded = c * pallet_pcs[i] / p_a
            weight_loaded = w * pallet_pcs[i] / p_a
            pallet_data_list.append(
                {
                    "container_number": order.container_number,
                    "destination": destination,
                    "delivery_method": delivery_method,
                    "delivery_type": delivery_type,
                    "pallet_id": pallet_ids[i],
                    "pcs": pallet_pcs[i],
                    "cbm": cbm_loaded,
                    "weight_lbs": weight_loaded,
                    "shipping_mark": shipping_mark if shipping_mark else "",
                    "fba_id": fba_id if fba_id else "",
                    "ref_id": ref_id if ref_id else "",
                    "abnormal_palletization": 'False',
                    "location": warehouse_code,
                    "PO_ID": po_id,
                    **({"delivery_window_start": dw_st} if dw_st else {}),
                    **({"delivery_window_end": dw_end} if dw_end else {})
                }
            )

        return pallet_data_list

            
    async def handle_update_delivery_type(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:

        # 处理PackingList
        batch_size = 10000
        queryset = PackingList.objects.filter(
            models.Q(delivery_type__isnull=True)
            | models.Q(delivery_type=None)
            | models.Q(delivery_type="None")
        ).order_by("id")
        total = await sync_to_async(queryset.count)()

        for start in range(0, total, batch_size):
            batch = await sync_to_async(list)(
                queryset[start : start + batch_size].values("id", "destination")
            )

            public_ids = [
                item["id"]
                for item in batch
                if (
                    re.fullmatch(r"^[A-Za-z]{4}\s*$", str(item["destination"]).strip())
                    or re.fullmatch(r"^[A-Za-z]{3}\s*\d$", str(item["destination"]).strip())
                    or re.fullmatch(
                        r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$", str(item["destination"]).strip()
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
        queryset = Pallet.objects.filter(
            models.Q(delivery_type__isnull=True)
            | models.Q(delivery_type=None)
            | models.Q(delivery_type="None")
        ).order_by("id")
        total = await sync_to_async(queryset.count)()

        for start in range(0, total, plt_size):
            batch_plt = await sync_to_async(list)(
                queryset[start : start + plt_size].values("id", "destination")
            )

            public_ids = [
                item["id"]
                for item in batch_plt
                if (
                    re.fullmatch(r"^[A-Za-z]{4}\s*$", str(item["destination"]).strip())
                    or re.fullmatch(r"^[A-Za-z]{3}\s*\d$", str(item["destination"]).strip())
                    or re.fullmatch(
                        r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$", str(item["destination"]).strip()
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
                pl.delivery_method = request.POST.getlist("delivery_method")[idx]
                pl.delivery_type = request.POST.getlist("delivery_type")[idx]
                pl.shipping_mark = request.POST.getlist("shipping_mark")[idx].strip()
                pl.fba_id = request.POST.getlist("fba_id")[idx].strip()
                pl.ref_id = request.POST.getlist("ref_id")[idx].strip()
                pl.destination = destination_list[idx]
                pl.address = request.POST.getlist("address")[idx]
                pl.note = request.POST.getlist("note")[idx]
                long = request.POST.getlist("long")[idx]
                pl.long = Decimal(long) if long else None
                width = request.POST.getlist("width")[idx]
                pl.width = Decimal(width) if width else None

                height = request.POST.getlist("height")[idx]
                pl.height = Decimal(height) if height else None

                pl.express_number = request.POST.getlist("express_number")[idx]
                start_date_str = request.POST.getlist("delivery_window_start")[
                    idx
                ].strip()
                pl.delivery_window_start = (
                    parse_date(start_date_str) if start_date_str else None
                )
                end_date_str = request.POST.getlist("delivery_window_end")[idx].strip()
                pl.delivery_window_end = (
                    parse_date(end_date_str) if end_date_str else None
                )
                updated_pl.append(pl)
            await sync_to_async(bulk_update_with_history)(
                updated_pl,
                PackingList,
                fields=[
                    "delivery_method",
                    "delivery_type",
                    "shipping_mark",
                    "fba_id",
                    "ref_id",
                    "destination",
                    "address",
                    "note",
                    "long",
                    "width",
                    "height",
                    "express_number",
                    "delivery_window_start",
                    "delivery_window_end",
                ],
            )
        else:
            # 没打板的，才考虑，判断是否有快递，然后修改为P1等级
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
                    po_id_hkey = f"{dm}-{dest}-{fba}"
                    po_id_seg = (
                        f"H{fba[-4:]}{sm[-4:]}"
                        if fba
                        else f"H{sm[-4:] if sm else ''.join(random.choices(string.ascii_letters.upper() + string.digits, k=4))}"
                    )
                elif dm == "客户自提" or dest == "客户自提":
                    po_id_hkey = f"{dm}-{dest}-{fba}"
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
                    random.seed(container_number[-4:])
                    po_id = f"{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}{po_id_seg}{seq_num}"
                    po_id = re.sub(r"[\u4e00-\u9fff]", "", po_id)
                    po_id_hash[po_id_hkey] = po_id
                    seq_num += 1
                po_ids.append(po_id)
            del po_id_hash, po_id, po_id_seg, po_id_hkey
            total_weight_lbs_list = []
            for lbs_str in request.POST.getlist("total_weight_lbs"):
                if lbs_str.strip():  # 跳过空值
                    try:
                        total_weight_lbs_list.append(float(lbs_str))
                    except ValueError:
                        raise RuntimeError(f"无效的重量值: {lbs_str}，请输入数字")

            total_weight_lbs_sum = sum(total_weight_lbs_list)
            container = await sync_to_async(Container.objects.get)(
                container_number=container_number
            )
            container.weight_lbs = total_weight_lbs_sum  # 假设Container模型有weight_lbs字段
            await sync_to_async(container.save)()
            pl_data = zip(
                request.POST.getlist("delivery_method"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                request.POST.getlist("ref_id"),
                destination_list,
                request.POST.getlist("address"),
                request.POST.getlist("pcs"),
                request.POST.getlist("total_weight_kg"),
                request.POST.getlist("total_weight_lbs"),
                request.POST.getlist("cbm"),
                request.POST.getlist("note"),
                request.POST.getlist("long"),
                request.POST.getlist("width"),
                request.POST.getlist("height"),
                request.POST.getlist("express_number"),
                po_ids,
                request.POST.getlist("delivery_window_start"),
                request.POST.getlist("delivery_window_end"),
                request.POST.getlist("delivery_type"),
                strict=True,
            )

            def parse_decimal(value):
                if not value or str(value).strip() == "":
                    return None
                try:
                    return Decimal(str(value).strip())
                except InvalidOperation:
                    raise ValueError(f"无效的数值格式: {value}")

            pl_to_create = [
                PackingList(
                    container_number=container,
                    delivery_method=d[0],
                    shipping_mark=d[1].strip(),
                    fba_id=d[2].strip(),
                    ref_id=d[3].strip(),
                    destination=d[4],
                    address=d[5],
                    pcs=int(float(d[6])),
                    total_weight_kg=d[7],
                    total_weight_lbs=d[8],
                    cbm=d[9],
                    note=d[10],
                    long=parse_decimal(d[11]),
                    width=parse_decimal(d[12]),
                    height=parse_decimal(d[13]),
                    express_number=d[14],
                    PO_ID=d[15],
                    delivery_window_start=d[16] if d[16].strip() else None,
                    delivery_window_end=d[17] if d[17].strip() else None,
                    delivery_type=d[18],
                )
                for d in pl_data
            ]

            await sync_to_async(bulk_create_with_history)(pl_to_create, PackingList)
            # await sync_to_async(PackingList.objects.bulk_create)(pl_to_create)
            order.packing_list_updloaded = True
            await sync_to_async(order.save)()
            #每次更新pl清单，就判断柜子优先级
            await self._update_container_unpacking_priority(container_number)
        # 查找新建的pl，和现在的pocheck比较，如果内容没有变化，pocheck该记录不变，如果有变化就对应修改

        # 因为上面已经将新的packing_list存到表里，所以直接去pl表查
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(container_number__container_number=container)
        )
        po_checks = await sync_to_async(list)(
            PoCheckEtaSeven.objects.filter(container_number__container_number=container)
        )
        #如果这个柜子的pl都删了，那么pocheck也要都删掉 
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
                        "vessel_eta": (
                            order.vessel_id.vessel_eta.date()
                            if order.vessel_id.vessel_eta
                            else None
                        ),
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
                criteria = models.Q(container_number__container_number=container)
                if packing_list:
                    criteria &= models.Q(packing_list=None)
                # 对于po_check没有指向pl的，就删除
                queryset = await sync_to_async(PoCheckEtaSeven.objects.filter)(criteria)
                for obj in await sync_to_async(list)(queryset):
                    # 对每个对象执行删除操作
                    await sync_to_async(obj.delete)()
            except PoCheckEtaSeven.DoesNotExist:
                raise ValueError("不存在")
        # 更新完pl之后，更新container的delivery_type
        # await self._confirm_delivery_type(container_number)
        types = set(pl.delivery_type for pl in packing_list if pl.delivery_type)
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

    async def _confirm_delivery_type(self, container_number: str) -> None:
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(
                container_number__container_number=container_number
            )
        )
        public_ids = []
        other_ids = []

        for item in packing_list:
            destination = str(item.destination)
            dest_clean = str(destination).strip()
            is_public = (
                re.fullmatch(r"^[A-Za-z]{4}\s*$", dest_clean)
                or re.fullmatch(r"^[A-Za-z]{3}\s*\d$", dest_clean)
                or re.fullmatch(r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$", dest_clean)
                or any(
                    kw.lower() in dest_clean.lower() for kw in {"walmart", "沃尔玛"}
                )
            )

            if is_public:
                public_ids.append(item.id)
            else:
                other_ids.append(item.id)
        if public_ids:
            await sync_to_async(PackingList.objects.filter(id__in=public_ids).update)(
                delivery_type="public"
            )

        if other_ids:
            await sync_to_async(PackingList.objects.filter(id__in=other_ids).update)(
                delivery_type="other"
            )

    def _find_compre_matching_regions(
        self,
        plts_by_destination: dict,
        combina_fee: dict,
        container_type,
        total_cbm_sum: float,
        combina_threshold: int,
    ) -> dict:
        matching_regions = defaultdict(float)  # 各区的cbm总和
        destination_matches = set()           # 组合柜的仓点
        non_combina_dests = set()             # 非组合柜的仓点
        dest_cbm_list = []                    # 临时存储初筛组合柜内的cbm和匹配信息
        sum_des = set()
        price_display = defaultdict(set)
        for plts in plts_by_destination:
            sum_des.add(plts["destination"])
            if "UPS" in plts["destination"]:
                non_combina_dests.add("UPS")
                continue

            destination = plts["destination"]
            dest = destination.replace("沃尔玛", "").split("-")[-1].strip()
            cbm = plts["total_cbm"]
            matched = False

            # 遍历所有区域和location
            for region, fee_data_list in combina_fee.items():
                for fee_data in fee_data_list:
                    if dest in fee_data["location"]:
                        price_display[region].add(dest)
                        matching_regions[region] += cbm
                        matched = True
                        # 记录下来，方便后续排序
                        dest_cbm_list.append({"dest": dest, "cbm": cbm})
                        destination_matches.add(dest)

            if not matched:
                non_combina_dests.add(dest)
        # 阈值处理：如果组合柜仓点超过限制，就只保留前 N 个 cbm 最大的
        if len(destination_matches) > combina_threshold:
            sorted_dests = sorted(dest_cbm_list, key=lambda x: x["cbm"], reverse=True)
            destination_matches = {item["dest"] for item in sorted_dests[:combina_threshold]}
            for item in sorted_dests[combina_threshold:]:
                non_combina_dests.add(item["dest"])
        combina_dests = {}
        for region, dests_in_region in price_display.items():
            # 取交集：只保留既在该区域又在 destination_matches 中的仓点
            matched_dests = dests_in_region.intersection(destination_matches)
            if matched_dests:  # 只添加非空的区域
                combina_dests[region] = list(matched_dests)
        # 返回精简后的结构
        return {
            "matching_regions": matching_regions,     # 各区的CBM总和
            "combina_dests": combina_dests,     # 组合柜仓点 set
            "non_combina_dests": non_combina_dests,   # 非组合柜仓点 set
        }

    def find_matching_regions(
        self, plts_by_destination: dict, combina_fee: dict
    ) -> dict:
        non_combina_dests = set()
        price_display = defaultdict(set)

        for plts in plts_by_destination:
            if "UPS" in plts["destination"]:
                # 如果包含UPS，不需要显示细节，就显示UPS就可以了，张楠提
                non_combina_dests.add("UPS")
                continue
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
        return  {
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
        # 准备参数
        # 找组合柜报价
        matched_regions = await self._is_combina(container_number)
        # 非组合柜区域
        request.non_combina_region = matched_regions["non_combina_dests"]
        # 组合柜区域
        request.combina_region = matched_regions["combina_dests"]
        request.is_combina = matched_regions["is_combina"]
        request.quotation_file = matched_regions["quotation_file"]
        return await self.handle_order_management_container_get(request)

    async def _is_combina(self, container_number: str) -> bool:
        container = await sync_to_async(Container.objects.get)(container_number=container_number)
        order = await sync_to_async(
            lambda: Order.objects.select_related("retrieval_id", "vessel_id","customer_name")
                .get(container_number__container_number=container_number)
        )()
        customer = order.customer_name
        customer_name = customer.zem_name
        # 从报价表找+客服录的数据
        warehouse = order.retrieval_id.retrieval_destination_area
        vessel_etd = order.vessel_id.vessel_etd

        container_type = container.container_type
        is_combina = True
        has_pallet = True
        #  基础数据统计
        plts = await sync_to_async(
            lambda: Pallet.objects.filter(
                container_number__container_number=container_number
            ).aggregate(
                unique_destinations=Count("destination", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
                total_pallets=Count("id"),
            )
        )()
        if plts['total_pallets'] == 0:
            has_pallet = False
            plts = await sync_to_async(
                lambda: PackingList.objects.filter(
                    container_number__container_number=container_number
                ).aggregate(
                    unique_destinations=Count("destination", distinct=True),
                    total_weight=Sum("total_weight_lbs"),
                    total_cbm=Sum("cbm"),
                    total_pallets=Coalesce(
                        Round(
                            Cast(Sum("cbm"), output_field=FloatField()) / 1.8,
                            output_field=IntegerField()
                        ),
                        0  # 默认值，当Sum("cbm")为None时设为0
                    )
                )
            )()
        plts["total_cbm"] = round(plts["total_cbm"], 2)
        plts["total_weight"] = round(plts["total_weight"], 2)
        # 获取匹配的报价表
        matching_quotation = await sync_to_async(
            lambda: QuotationMaster.objects.filter(
                effective_date__lte=vessel_etd,
                is_user_exclusive=True,
                exclusive_user=customer_name,
                quote_type='receivable',
            ).order_by("-effective_date").first()
        )()
        
        if not matching_quotation:
            matching_quotation = await sync_to_async(
                lambda: QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=False,
                    quote_type='receivable',
                ).order_by("-effective_date").first()
            )()
        if matching_quotation:
            quotation_file = matching_quotation.filename
        else:
            quotation_file = '未找到对应报价表'
        
        # 获取费用详情
        stipulate = await sync_to_async(
            lambda: FeeDetail.objects.get(
                quotation_id=matching_quotation.id, fee_type="COMBINA_STIPULATE"
            ).details
        )()
        
        combina_fee = await sync_to_async(
            lambda: FeeDetail.objects.get(
                quotation_id=matching_quotation.id, fee_type=f"{warehouse}_COMBINA"
            ).details
        )()
        if isinstance(combina_fee, str):
            combina_fee = json.loads(combina_fee)
        # 看是否超出组合柜限定仓点,NJ/SAV是14个
        default_combina = stipulate["global_rules"]["max_mixed"]["default"]
        exceptions = stipulate["global_rules"]["max_mixed"].get("exceptions", {})
        combina_threshold = exceptions.get(warehouse, default_combina) if exceptions else default_combina

        default_threshold = stipulate["global_rules"]["bulk_threshold"]["default"]
        exceptions = stipulate["global_rules"]["bulk_threshold"].get("exceptions", {})
        uncombina_threshold = exceptions.get(warehouse, default_threshold) if exceptions else default_threshold
        if plts["unique_destinations"] > uncombina_threshold:
            container.account_order_type = "转运"
            container.non_combina_reason = (
                f"总仓点超过{uncombina_threshold}个"
            )
            await sync_to_async(container.save)()
            is_combina = False # 不是组合柜

        # 按区域统计
        if has_pallet:
            plts_by_destination = await sync_to_async(
                lambda: list(Pallet.objects.filter(
                    container_number__container_number=container_number
                ).values("destination").annotate(total_cbm=Sum("cbm")))
            )()
        else:
            plts_by_destination = await sync_to_async(
                lambda: list(PackingList.objects.filter(
                    container_number__container_number=container_number
                ).values("destination").annotate(total_cbm=Sum("cbm")))
            )()
        total_cbm_sum = sum(item["total_cbm"] for item in plts_by_destination)
        # 区分组合柜区域和非组合柜区域
        container_type_temp = 0 if container_type == "40HQ/GP" else 1
        matched_regions = self._find_compre_matching_regions(
            plts_by_destination, combina_fee, container_type_temp, total_cbm_sum, combina_threshold
        )
        
        # 判断是否混区，False表示满足混区条件
        is_mix = await self.is_mixed_region(
            matched_regions["matching_regions"], warehouse, vessel_etd
        )
        if is_mix:
            container.account_order_type = "转运"
            container.non_combina_reason = "混区不符合标准"
            await sync_to_async(container.save)()
            is_combina = False
        # 非组合柜区域
        non_combina_region_count = matched_regions["non_combina_dests"]
        # 组合柜区域
        combina_region_count = matched_regions["combina_dests"]

        
        if len(non_combina_region_count) > (
            uncombina_threshold
            - combina_threshold
        ):
            # 当非组合柜的区域数量超出时，不能按转运组合
            container.account_order_type = "转运"
            container.non_combina_reason = "非组合柜区的数量不符合标准"
            await sync_to_async(container.save)()
            is_combina = False
        return {
            "combina_dests": combina_region_count,
            "non_combina_dests": non_combina_region_count,
            "is_combina": is_combina,
            "quotation_file": quotation_file,
        }
    
    async def is_mixed_region(self, matched_regions, warehouse, vessel_etd) -> bool:
        regions = list(matched_regions.keys())
        # LA仓库的特殊规则：CDEF区不能混
        if warehouse == "LA":
            if vessel_etd.month > 7 or (
                vessel_etd.month == 7 and vessel_etd.day >= 15
            ):  # 715之后没有混区限制
                return False
            if len(regions) <= 1:  # 只有一个区，就没有混区的情况
                return False
            if set(regions) == {"A区", "B区"}:  # 如果只有A区和B区，也满足混区规则
                return False
            return True
        # 其他仓库无限制
        return False
    
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
    
    async def handle_peer_upload_template_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        #同行的货，上传文件，包括收货地址、柜号、州代码、板数、FBA、REF、PO、重量、CBM、件数、客户名
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            df = df.rename(columns=PACKING_LIST_TEMP_COL_MAPPING)
            # 删除空行
            df = df.dropna(how='all') 
            #除柜号外，缺值报错并提醒，不核实FBA是因为沃尔玛的可以没有FBA
            columns_to_check = ['收货地址', '板数', 'PO号码', '重量(kg)', 'CBM', '件数']
            #缺了这些列也报错
            missing_columns = [col for col in columns_to_check if col not in df.columns]
            if missing_columns:
                raise ValueError(f"数据表中缺少以下必需列：{missing_columns}。请检查数据文件格式。")
            null_mask = df[columns_to_check].isnull().any(axis=1)
            if null_mask.any():
                # 找出有空值的行
                error_rows = df[null_mask]
                error_count = len(error_rows)
                
                # 构建详细的错误信息
                error_details = []
                for index, row in error_rows.iterrows():
                    empty_cells = []
                    for col in columns_to_check:
                        if pd.isnull(row[col]):
                            empty_cells.append(f"第{index + 1}行『{col}』")
                    
                    if empty_cells:
                        error_details.append("、".join(empty_cells))
                
                error_message = (
                    f"发现 {error_count} 行数据存在空值：\n"
                    f"{chr(10).join(error_details)}\n"
                    f"请补充完整数据后再继续处理。"
                )
                raise ValueError(error_message)
            df = df.replace(np.nan, '')
            
            #字符串类型的去掉前后空格
            for col in df.columns:
                try:
                    df[col] = df[col].str.strip()
                except AttributeError:
                    continue
            df = df.rename(columns={
                '接货仓库': 'warehouses',
                '柜号': 'container_number',
                '客户名': 'customer_name', 
                '件数': 'total_pcs',
                '重量(kg)': 'total_weight',
                'CBM': 'total_cbm',
                'FBA号码': 'fba_id',
                'PO号码': 'ref_id',
                '收货地址': 'destination',
                '板数': 'total_pallet',
                '最早派送时间': 'delivery_window_start',
                '最晚派送时间': 'delivery_window_end',
            })
            warehouses = df['warehouses'].replace('', pd.NA).dropna().unique()
            warehouses = warehouses.tolist()
            if len(warehouses) > 1:
                #值多不行，现在模板一次就建一个客户一个仓库的
                raise ValueError(f"'接货仓库'列有多个不同的值：{warehouses}。请确保所有行的柜号相同。")
            elif len(warehouses) == 1:
                warehouse = warehouses[0].split('-')[0]
            else:
                #说明没有提供值，那就默认为NJ
                warehouse = 'NJ'

            customer_names = df['customer_name'].dropna().unique()
            if len(customer_names) > 1:
                raise ValueError(f"'客户名'列有多个不同的值：{customer_names}。请确保所有行的客户名相同。")
            elif len(customer_names) == 1:
                customer_name = customer_names[0]
            else:
                #客户名不提供就在前端自己选，给定选项
                customer_name = None
            
            container_numbers = df['container_number'].replace('', pd.NA).dropna().unique()
            container_numbers = container_numbers.tolist()
            if len(container_numbers) > 1:
                raise ValueError(f"'柜号'列有多个不同的值：{container_numbers}。请确保所有行的柜号相同。")
            elif len(container_numbers) == 1:
                container_number = container_numbers[0]
                if await Container.objects.filter(container_number=container_number).aexists():
                    raise ValueError('柜号重复，请核实')
                
            else:
                #如果没提供柜号，就自动给一个，是客户名+7个数字，如果没给客户名，就在前端选完客户名之后再加这7个数字
                container_number = await self._naming_container(customer_name)
            orders = df.to_dict('records')
            for order in orders:
                for col in ['delivery_window_start', 'delivery_window_end']:
                    if col in order and pd.isna(order[col]):
                        order[col] = None
            context = {
                'orders': orders,
                'container_number': container_number,
                'customer_name': customer_name,
                'warehouse': warehouse,
                'peer_customer': self.peer_customer,
            }
            return await self.handle_peer_po_creation(request,context)
        else:
            raise ValueError(f"invalid file format!")
        
    async def _naming_container(self,customer_name:str) -> str:
        max_attempts=10
        attempt = 0
        while attempt < max_attempts:
            random_num = ''.join(random.choices('0123456789', k=7))
            if customer_name:
                name_str = str(customer_name).strip()
                prefix = (name_str[:4].upper().replace(' ', '').replace('-', '')).ljust(4, 'X')              
                container_number = prefix + random_num
            else:
                container_number = random_num
            if not await Container.objects.filter(container_number=container_number).aexists():
                return  container_number
            attempt += 1
        else:
            raise ValueError('尝试十次，柜号始终重复')

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
            for data in pl_data:
                if pd.isna(data["delivery_window_start"]):
                    data["delivery_window_start"] = None
                if pd.isna(data["delivery_window_end"]):
                    data["delivery_window_end"] = None
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
        dest_clean = str(destination).strip()
        if not isinstance(dest_clean, str):  # 没有地址是私仓
            return False
        if "自提" in dest_clean:
            return False
        pattern1 = r"^[A-Za-z]{3}\s*\d$"
        if re.match(pattern1, dest_clean):  # 3个字母+空格+1个数字是公仓
            return True
        pattern2 = r"^[A-Za-z]{3}\s*\d\s*[A-Za-z]$"  # 3个字母+空格+1个数字+字母是公仓
        if re.match(pattern2, dest_clean):
            return True
        pattern3 = r"^[A-Za-z]{4}\s*$"  # 包含4个字母是公仓
        if re.fullmatch(pattern3, dest_clean):
            return True
        keywords = {"walmart", "沃尔玛", "UPS", "FEDEX"}
        destination_lower = dest_clean.lower()
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
