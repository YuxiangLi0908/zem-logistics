import os
import string
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from itertools import zip_longest
from pathlib import Path
import random
from typing import Any
import re

import numpy as np
import pandas as pd
from asgiref.sync import sync_to_async
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render
from django.utils.dateparse import parse_date
from django.views import View
from simple_history.utils import bulk_update_with_history, bulk_create_with_history

from warehouse.forms.upload_file import UploadFileForm
from django.utils import timezone

from warehouse.models.container import Container
from warehouse.models.container_pickup_carrier import ContainerPickupCarrier
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.retrieval import Retrieval
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import SHIPPING_LINE_OPTIONS, DELIVERY_METHOD_OPTIONS, ADDITIONAL_CONTAINER, \
    PACKING_LIST_TEMP_COL_MAPPING, DROPSHIPPING_PACKING_LIST_TEMP_COL_MAPPING, DELIVERY_METHOD_CODE


class Dropshipping(View):
    template_order_create_supplement = "dropshipping/03_order_creation.html"
    template_order_create_base = "dropshipping/02_base_order_creation_status.html"
    template_order_details = "dropshipping/order_details.html"
    template_order_details_pl = "dropshipping/order_details_pl_tab.html"
    template_order_create_supplement_pl_tab = "dropshipping/03_order_creation_packing_list_tab.html"

    container_type = {
        "": "",
        "40HQ/GP": "40HQ/GP",
        "45HQ/GP": "45HQ/GP",
        "20GP": "20GP",
        "53HQ": "53HQ",
    }

    order_type = {"一件代发": "一件代发"}
    area = {"NJ": "NJ", "SAV": "SAV", "LA": "LA",}

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_order_basic_info_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "container_info_supplement":
            template, context = await self.handle_order_supplemental_info_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.POST.get("step")
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
        elif step == "upload_template":
            template, context = await self.handle_upload_template_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "download_template":
            return await self.handle_download_template_post()
        elif step == "update_order_packing_list_info":
            template, context = await self.handle_update_order_packing_list_info_post(
                request
            )
            return await sync_to_async(render)(request, template, context)

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
                & models.Q(cancel_notification=False)
                & models.Q(order_type='一件代发')
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
            ("一件代发", "一件代发"),
        ]
        context["container_type"] = self.container_type
        context["packing_list_upload_form"] = UploadFileForm()
        return self.template_order_create_supplement, context

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
                area if order_type in ("一件代发") else destination
            ),
        }
        retrieval = Retrieval(**retrieval_data)
        await sync_to_async(retrieval.save)()

        offload_data = {
            "offload_id": offload_id,
            "offload_required": True if order_type in ("一件代发") else False,
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
        retrieval.retrieval_destination_area = request.POST.get("area")
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
        context["carrier_options"] = await sync_to_async(list)(
            ContainerPickupCarrier.objects
            .filter(is_active=True)
            .order_by("name")
            .values_list("name", "name")
        )
        context["warehouse_options"] = await sync_to_async(list)(
            ZemWarehouse.objects
            .order_by("name")
            .values_list("name", "name")
        )
        context["delivery_types"] = [
            ("一件代发", "一件代发"),
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

    @sync_to_async
    def update_container_by_number(self, container_number, container_type):
        # 所有同步代码在这里执行，与异步上下文完全隔离
        Container.objects.filter(container_number=container_number).update(
            container_type=container_type
        )

    async def handle_upload_template_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            df = df.rename(columns=DROPSHIPPING_PACKING_LIST_TEMP_COL_MAPPING)
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
            df["dropshipping_item_name"] = df["dropshipping_item_name"].str.strip()
            df["dropshipping_item_model_number"] = df["dropshipping_item_model_number"].str.strip()
            df["delivery_type"] = "一件代发"
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

    async def handle_download_template_post(self) -> HttpResponse:
        file_path = (
            Path(__file__)
            .parent.parent.resolve()
            .joinpath("templates/export_file/dropshipping_packing_list_template.xlsx")
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

    async def handle_update_order_packing_list_info_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        '''更新Packing List信息'''
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
            for pl in packing_list:
                idx = pl_id_idx_mapping[pl.id]
                pl.delivery_method = request.POST.getlist("delivery_method")[idx]
                pl.delivery_type = request.POST.getlist("delivery_type")[idx]
                pl.shipping_mark = request.POST.getlist("shipping_mark")[idx].strip()
                pl.fba_id = request.POST.getlist("fba_id")[idx].strip()
                pl.ref_id = request.POST.getlist("ref_id")[idx].strip()
                pl.dropshipping_item_name = request.POST.getlist("dropshipping_item_name")[idx].strip()
                pl.dropshipping_item_model_number = request.POST.getlist("dropshipping_item_model_number")[idx].strip()
                pl.address = request.POST.getlist("address")[idx]
                pl.note = request.POST.getlist("note")[idx]
                pl.office_note = request.POST.getlist("office_note")[idx]
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
                    "dropshipping_item_name",
                    "dropshipping_item_model_number",
                    "address",
                    "note",
                    "office_note",
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
            # Generate PO_ID
            po_ids = []
            po_id_hash = {}
            seq_num = 1
            for dm, sm, dest in zip_longest(
                    request.POST.getlist("delivery_method"),
                    request.POST.getlist("mark"),
                    request.POST.getlist("dropshipping_item_model_number"),
                    fillvalue='',
            ):
                po_id: str = ""
                po_id_seg: str = ""
                po_id_hkey: str = ""
                if dm in ["暂扣留仓(HOLD)", "暂扣留仓"]:
                    po_id_hkey = f"{container_number}-{dm}-{dest}-{sm}"
                    po_id_seg = (
                        f"H{sm[-4:] if sm else ''.join(random.choices(string.ascii_letters.upper() + string.digits, k=6))}"
                    )
                elif dm == "客户自提" or dest == "客户自提":
                    po_id_hkey = f"{container_number}-{dm}-{dest}"
                    po_id_seg = (
                        f"S{sm[-4:]}"
                        if sm
                        else f"S{''.join(random.choices(string.ascii_letters.upper() + string.digits, k=6))}"
                    )
                else:
                    po_id_hkey = f"{container_number}-{dm}-{dest}"
                    po_id_seg = f"{DELIVERY_METHOD_CODE.get(dm, 'UN')}{dest.replace(' ', '').split('-')[-1]}"
                if po_id_hkey in po_id_hash:
                    po_id = po_id_hash.get(po_id_hkey)
                else:
                    container_tag = f"{container_number[:2].upper()}{container_number[-4:]}"

                    random.seed(container_number[-4:])
                    po_id = (
                        f"{container_tag}"
                        f"{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
                        f"{po_id_seg}"
                        f"{seq_num}"
                    )
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
            pl_data = zip_longest(
                request.POST.getlist("delivery_method"),
                request.POST.getlist("shipping_mark"),
                request.POST.getlist("fba_id"),
                request.POST.getlist("ref_id"),
                request.POST.getlist("dropshipping_item_name"),
                request.POST.getlist("dropshipping_item_model_number"),
                request.POST.getlist("address"),
                request.POST.getlist("pcs"),
                request.POST.getlist("total_weight_kg"),
                request.POST.getlist("total_weight_lbs"),
                request.POST.getlist("cbm"),
                request.POST.getlist("note"),
                request.POST.getlist("office_note"),
                request.POST.getlist("long"),
                request.POST.getlist("width"),
                request.POST.getlist("height"),
                request.POST.getlist("express_number"),
                po_ids,
                request.POST.getlist("delivery_window_start"),
                request.POST.getlist("delivery_window_end"),
                request.POST.getlist("delivery_type"),
                fillvalue=""
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
                    dropshipping_item_name=d[4],
                    dropshipping_item_model_number=d[5],
                    address=d[6],
                    pcs=int(float(d[7])),
                    total_weight_kg=d[8],
                    total_weight_lbs=d[9],
                    cbm=d[10],
                    note=d[11],
                    office_note=d[12],
                    long=parse_decimal(d[13]),
                    width=parse_decimal(d[14]),
                    height=parse_decimal(d[15]),
                    express_number=d[16],
                    PO_ID=d[17],
                    delivery_window_start=d[18] if d[18].strip() else None,
                    delivery_window_end=d[19] if d[19].strip() else None,
                    delivery_type=d[20],
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
