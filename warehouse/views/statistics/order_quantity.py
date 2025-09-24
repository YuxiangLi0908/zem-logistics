import json
import random
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np
from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.apps import apps
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Case, Count, FloatField, Model, Q, Sum, F
from django.db.models.functions import Cast, TruncMonth
from django.http import Http404, HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.views import View

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice import Invoice, InvoiceItem
from warehouse.models.invoice_details import InvoiceDelivery
from warehouse.models.order import Order
from warehouse.models.pallet import Pallet
from warehouse.models.retrieval import HistoricalRetrieval, Retrieval
from warehouse.utils.constants import MODEL_CHOICES


class OrderQuantity(View):
    template_shipment = "statistics/order_quantity.html"
    template_historical = "statistics/historical.html"
    template_profit = "statistics/profit.html"
    template_delivery_cost_status = "statistics/delivery_cost_status.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA"}

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "historical_query":
            if not await self._validate_user_manage(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            context = {"model_choices": MODEL_CHOICES}
            return await sync_to_async(render)(
                request, self.template_historical, context
            )
        elif step == "profit_analysis":
            if not await self._validate_user_profit(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            customers = await sync_to_async(list)(Customer.objects.all())
            customers = {c.zem_name: c.id for c in customers}
            customers["----"] = None
            customers = {"----": None, **customers}
            context = {"area_options": self.area_options, "customers": customers}
            return await sync_to_async(render)(request, self.template_profit, context)
        elif step == "profit_analysis_personalized":
            if not await self._validate_user_profit(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            customers = await sync_to_async(list)(Customer.objects.all())
            customers = {c.zem_name: c.id for c in customers}
            customers["----"] = None
            customers = {"----": None, **customers}
            context = {"area_options": self.area_options, "customers": customers}
            return await sync_to_async(render)(request, self.template_delivery_cost_status, context)
        else:
            if not await self._validate_user_group(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            customers = await sync_to_async(list)(Customer.objects.all())
            customers = {c.zem_name: c.id for c in customers}
            customers["----"] = None
            customers = {"----": None, **customers}
            context = {"area_options": self.area_options, "customers": customers}
            return await sync_to_async(render)(request, self.template_shipment, context)

    async def post(self, request: HttpRequest) -> HttpResponse:

        step = request.POST.get("step")
        if step == "selection":
            template, context = await self.handle_order_quantity_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "historical_selection":
            template, context = await self.handle_order_historical_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "profit_selection":
            template, context = await self.handle_order_profit_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delivery_st_selection":
            template, context = await self.handle_delivery_st_selection(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "caculate_all_expense":
            template, context = await self.handle_caculate_all_expense(request)
            return await sync_to_async(render)(request, template, context)

    async def _get_orders(self, request) -> list:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        today = datetime.today()
        six_months_ago_first_day = (today + relativedelta(months=-6)).replace(day=1)
        last_month_last_day = today + relativedelta(months=-1, day=31)

        start_date = (
            six_months_ago_first_day.strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            last_month_last_day.strftime("%Y-%m-%d") if not end_date else end_date
        )

        customers = await sync_to_async(
            lambda: {"----": None, **{c.zem_name: c.id for c in Customer.objects.all()}}
        )()
        customer_idlist = request.POST.getlist("customer")
        warehouse_list = request.POST.getlist("warehouse")

        date_type = request.POST.get("date_type")

        if date_type == "eta":
            criteria = Q(
                Q(vessel_id__vessel_eta__gte=start_date),
                Q(vessel_id__vessel_eta__lte=end_date),
                ~Q(order_type="直送"),
            )
        else:
            criteria = Q(
                Q(vessel_id__vessel_etd__gte=start_date),
                Q(vessel_id__vessel_etd__lte=end_date),
                ~Q(order_type="直送"),
            )
        if warehouse_list:
            criteria &= Q(retrieval_id__retrieval_destination_area__in=warehouse_list)
        if customer_idlist:
            customer_list = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist).values("zem_name")
            )
            customer_idlist = [item["zem_name"] for item in customer_list]
            criteria &= Q(customer_name__zem_name__in=customer_idlist)
        # 展示财务确认的账单
        criteria &= Q(receivable_status__stage="confirmed")
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "customer_name",
                "warehouse",
                "retrieval_id",
                "payable_status",
                "container_number",
            )
            .filter(criteria)
            .annotate(count=Count("id"))
        )
        return orders
    
    async def handle_caculate_all_expense(self, request) -> tuple[str, dict[str, Any]]:
        fleet_pallets = await sync_to_async(list)(
            FleetShipmentPallet.objects.select_related('container_number')
            .filter(expense__isnull=False,is_recorded=False)
            .exclude(PO_ID__isnull=True)
            .exclude(PO_ID='')
        )
        expense_groups = {}
        fleet_record_mapping = {}
        for fp in fleet_pallets:
            if not fp.container_number or not fp.container_number.container_number:
                continue
                
            key = (fp.PO_ID, fp.container_number.container_number)
            if key not in expense_groups:
                expense_groups[key] = {
                    'po_id': fp.PO_ID,
                    'container_number': fp.container_number.container_number,
                    'total_expense': 0,
                    'fleet_record_ids': []
                }
                fleet_record_mapping[key] = []
            
            expense_groups[key]['total_expense'] += fp.expense or 0
            expense_groups[key]['fleet_record_ids'].append(fp.id)
            fleet_record_mapping[key].append(fp.id)

        for key, group_data in expense_groups.items():
            po_id, container_number = key
            total_expense = group_data['total_expense']
            fleet_record_ids = group_data['fleet_record_ids']
            
            if total_expense == 0 and fleet_record_ids:
                #客户自提这种，成本为0的，不用后续处理
                await sync_to_async(FleetShipmentPallet.objects.filter(
                    id__in=fleet_record_ids
                ).update)(
                    is_recorded=True  # 标记为已记录
                )  
            # 查找对应的Pallet记录
            pallets = await sync_to_async(list)(
                Pallet.objects.select_related('container_number')
                .filter(
                    PO_ID=po_id,
                    container_number__container_number=container_number
                ).distinct('PO_ID','container_number__container_number')
            )        

            if not pallets:
                continue
            
            # 通过Pallet找到对应的Order和Invoice
            for pallet in pallets:
                if not pallet.container_number:
                    continue
                
                # 查找Order
                order = await sync_to_async(Order.objects.select_related('invoice_id').get)(container_number=pallet.container_number)
                
                invoice = order.invoice_id
                if not invoice:
                    continue
                old_amount = invoice.payable_delivery_amount or 0
                new_amount = old_amount + total_expense
                # 异步更新Invoice
                await sync_to_async(Invoice.objects.filter(
                    id=invoice.id
                ).update)(
                    payable_delivery_amount=new_amount
                )     
                if fleet_record_ids:
                    await sync_to_async(FleetShipmentPallet.objects.filter(
                        id__in=fleet_record_ids
                    ).update)(
                        is_recorded=True  # 标记为已记录
                    )    
        return await self.handle_delivery_st_selection(request)          

    async def handle_delivery_st_selection(self, request) -> tuple[str, dict[str, Any]]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        today = datetime.today()
        six_months_ago_first_day = today - timedelta(days=7)
        last_month_last_day = today + timedelta(days=7)

        start_date = (
            six_months_ago_first_day.strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            last_month_last_day.strftime("%Y-%m-%d") if not end_date else end_date
        )

        customers = await sync_to_async(
            lambda: {"----": None, **{c.zem_name: c.id for c in Customer.objects.all()}}
        )()
        customer_idlist = request.POST.getlist("customer")
        warehouse_list = request.POST.getlist("warehouse")

        date_type = request.POST.get("date_type")

        if date_type == "eta":
            criteria = Q(
                Q(vessel_id__vessel_eta__gte=start_date),
                Q(vessel_id__vessel_eta__lte=end_date),
                ~Q(order_type="直送"),
            )
        else:
            criteria = Q(
                Q(vessel_id__vessel_etd__gte=start_date),
                Q(vessel_id__vessel_etd__lte=end_date),
                ~Q(order_type="直送"),
            )
        if warehouse_list:
            criteria &= Q(retrieval_id__retrieval_destination_area__in=warehouse_list)
        if customer_idlist:
            customer_list = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist).values("zem_name")
            )
            customer_idlist = [item["zem_name"] for item in customer_list]
            criteria &= Q(customer_name__zem_name__in=customer_idlist)
        # 展示财务确认的账单
        #criteria &= Q(receivable_status__stage="confirmed")
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "customer_name",
                "warehouse",
                "retrieval_id",
                "payable_status",
                "container_number",
                "vessel_id",
                "invoice_id",
            )
            .filter(criteria)
            .exclude(invoice_id__isnull=True)
            .annotate(count=Count("id"))
        )
        container_numbers = [order.container_number.container_number for order in orders]
        pallets = await sync_to_async(list)(
            Pallet.objects.select_related('container_number')
            .filter(container_number__container_number__in=container_numbers)
            .exclude(PO_ID__isnull=True)
            .exclude(PO_ID='')
        )
        fleet_pallets = await sync_to_async(list)(
            FleetShipmentPallet.objects.select_related('container_number')
            .filter(container_number__container_number__in=container_numbers)
            .exclude(PO_ID__isnull=True)
            .exclude(PO_ID='')
        )
        results = []
        all_destinations = set()
        for order in orders:
            container_number = order.container_number.container_number
            container_pallets = [p for p in pallets if p.container_number.container_number == container_number]
            record_expense = order.invoice_id.payable_delivery_amount

            # 按 PO 分组
            po_groups = {}
            for pallet in container_pallets:
                if pallet.PO_ID:
                    if pallet.PO_ID not in po_groups:
                        po_groups[pallet.PO_ID] = {
                            "destination": pallet.destination,
                            "delivery_type": pallet.delivery_type,
                            "pallets": [],
                        }
                    po_groups[pallet.PO_ID]["pallets"].append(pallet)

            total_expense = 0
            missing_public = []
            missing_private = []
            has_exp_des = []

            for po_id, po_data in po_groups.items():
                destination = po_data["destination"] or "未知目的地"
                delivery_type = po_data["delivery_type"] or "未知类型"

                # 查找费用
                expense_records = [
                    fp for fp in fleet_pallets
                    if fp.container_number.container_number == container_number
                    and fp.PO_ID == po_id
                ]
                po_expense = sum(fp.expense or 0 for fp in expense_records)

                if po_expense > 0:
                    total_expense += po_expense
                    has_exp_des.append(destination)
                else:
                    # 没有费用 → 分类记录
                    if delivery_type == "public":
                        missing_public.append(destination)
                    else:
                        missing_private.append(destination)
            
            order_data = {
                "container_number": container_number,
                "customer_name": order.customer_name.zem_name,
                "warehouse": order.warehouse,
                "eta": order.vessel_id.vessel_eta if order.vessel_id else None,
                "total_pallets": len(container_pallets),
                "total_expense": total_expense,
                "record_expense": record_expense,
                "has_exp_des": ", ".join(set(has_exp_des)),
                "missing_public": ", ".join(set(missing_public)),
                "missing_private": ", ".join(set(missing_private)),
            }
            
            results.append(order_data)
        context = {
            'results': results,
            'all_destinations': all_destinations,
            'start_date': start_date,
            'end_date': end_date,
            'date_type': date_type,
            'warehouse_list': request.POST.getlist("warehouse"),
            'customer_list': request.POST.getlist("customer"),
            'customers': customers,
            'area_options': self.area_options,
        }
        
        return self.template_delivery_cost_status, context

    async def handle_order_historical_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        table_name = request.POST.get("model").strip()
        search_field = request.POST.get("search_field").strip()  # 界面上的查询字段
        search_value = request.POST.get("search_value").strip()  # 查询值

        model_info = MODEL_CHOICES.get(table_name)
        original_model_name = model_info["model"]
        original_model = apps.get_model("warehouse", original_model_name)
        search_process = model_info["search_process"]
        has_foreignKey = model_info["has_foreignKey"]
        # 处理外键查询，就是不能直接通过界面的查询字段查到结果
        transfer_table = model_info["transfer_table"]
        if transfer_table:  # 如果需要一张表中转，比如提拆柜账单，需要invoice表中转
            filter_kwargs = {search_process: search_value}
            transfer_model = apps.get_model("warehouse", transfer_table)
            transfer_table = await sync_to_async(transfer_model.objects.get)(
                **filter_kwargs
            )

            history_records = (
                original_model.objects.filter(invoice_number__id=transfer_table.id)
                .select_related("history_user")
                .order_by("-history_date")
            )
        else:
            if search_field == "container_number":
                filter_kwargs = {search_process: search_value}
                if has_foreignKey:  # 有container_number外键，可以直接查找
                    history_records = (
                        original_model.objects.filter(**filter_kwargs)
                        .select_related("history_user")
                        .order_by("-history_date")
                    )
                else:  # 没有外键，先找到原始表，再找历史表
                    origin_table = await sync_to_async(original_model.objects.get)(
                        **filter_kwargs
                    )
                    historical_table = "Historical" + original_model_name
                    original_model = apps.get_model("warehouse", historical_table)
                    history_records = (
                        original_model.objects.filter(id=origin_table.id)
                        .select_related("history_user")
                        .order_by("-history_date")
                    )
            else:
                filter_kwargs = {search_field: search_value}
                history_records = (
                    original_model.objects.filter(**filter_kwargs)
                    .select_related("history_user")
                    .order_by("-history_date")
                )

        # 转换为同步查询
        history_records = await sync_to_async(list)(history_records)

        # 准备显示数据
        records_data = []
        user_ids = {r.history_user_id for r in history_records if r.history_user_id}

        users = await sync_to_async(
            lambda: User.objects.filter(id__in=user_ids).in_bulk()
        )()

        for record in history_records:
            # 基础信息
            record_data = {
                "date": record.history_date,
                "user": users.get(record.history_user_id),
                "display_type": (
                    "创建"
                    if record.history_type == "+"
                    else "删除" if record.history_type == "-" else "修改"
                ),
                "type": record.history_type,  # 保留原始类型标识
                "record": record,  # 保留原始记录对象
            }

            # 异步获取所有字段值
            get_fields = sync_to_async(
                lambda r: {
                    f.name: (
                        getattr(r, f.name, None)  # 安全获取属性，不存在时返回None
                        if not f.is_relation  # 如果不是关系字段
                        else (
                            (
                                getattr(r, f.name + "_id", None)  # 先尝试获取外键ID
                                if getattr(r, f.name + "_id", None)
                                is not None  # 如果有外键ID
                                else None  # 没有外键ID则返回None
                            )
                            if f.many_to_one or f.one_to_one  # 只处理多对一和一对一关系
                            else [obj.pk for obj in getattr(r, f.name).all()]
                        )  # 处理多对多关系
                    )
                    for f in r.__class__._meta.get_fields()
                    if f.name
                    not in [
                        "history_id",
                        "history_date",
                        "history_user",
                        "history_type",
                        "history_change_reason",
                    ]
                },
                thread_sensitive=True,
            )
            all_fields = await get_fields(record)

            # 获取字段中文映射
            def get_field_display_name(field_name):
                # 从model_info的mapping中查找中文名
                display_name = next(
                    (
                        k
                        for k, v in model_info.get("mapping", {}).items()
                        if v == field_name
                    ),
                    field_name,
                )
                return (
                    display_name[0] if isinstance(display_name, tuple) else display_name
                )

            # 处理所有字段的中文显示
            display_fields = {}
            for field_name, value in all_fields.items():

                field_cn = get_field_display_name(field_name)

                # 处理布尔值
                if isinstance(value, bool):
                    display_value = self.get_bool_display(field_cn, None, value)
                else:
                    display_value = await self.safe_get_attr(record, field_name)
                    # display_value = str(value) if value is not None else '空'

                display_fields[field_cn] = display_value

            # 处理固定字段(station_fields)的中文名
            station_fields = [
                get_field_display_name(f) for f in model_info.get("station_field", [])
            ]

            # 添加到记录数据
            record_data.update(
                {"station_fields": station_fields, "all_fields": display_fields}
            )

            if record.history_type == "~":  # 修改记录需要特殊处理
                changes = []

                # 获取上一条记录
                get_prev_record = sync_to_async(
                    lambda r: original_model.objects.filter(
                        id=r.id, history_date__lt=r.history_date
                    )
                    .order_by("-history_date")
                    .first(),
                    thread_sensitive=True,
                )
                prev_record = await get_prev_record(record)

                if prev_record:
                    prev_fields = await get_fields(prev_record)

                    for field_name, current_value in all_fields.items():
                        if field_name in prev_fields:
                            previous_value = prev_fields[field_name]
                            if self.is_value_changed(previous_value, current_value):
                                field_cn = get_field_display_name(field_name)

                                # 处理显示文本
                                if isinstance(current_value, bool) or isinstance(
                                    previous_value, bool
                                ):
                                    display_text = self.get_bool_display(
                                        field_cn, previous_value, current_value
                                    )
                                else:
                                    old_val = (
                                        str(previous_value)
                                        if previous_value is not None
                                        else "空"
                                    )
                                    new_val = (
                                        str(current_value)
                                        if current_value is not None
                                        else "空"
                                    )
                                    display_text = f"{old_val} → {new_val}"

                                changes.append(
                                    {
                                        "field": field_cn,
                                        "change_text": display_text,
                                        "raw_field": field_name,
                                        "old_value": previous_value,
                                        "new_value": current_value,
                                        "is_changed": self.is_value_changed(
                                            previous_value, current_value
                                        ),  # 标记实际变化的字段
                                    }
                                )

                # 添加必要字段(未变化的)
                for field in model_info.get("station_field", []):
                    if field in all_fields and field not in [
                        change["raw_field"] for change in changes
                    ]:
                        field_cn = get_field_display_name(field)
                        value = all_fields.get(field_cn, "空")

                        changes.append(
                            {
                                "field": field_cn,
                                "change_text": value,
                                "raw_field": field,
                                "old_value": value,
                                "new_value": value,
                                "is_changed": False,  # 标记为未变化
                            }
                        )

                record_data["changes"] = changes

            records_data.append(record_data)
        context = {
            "model_choices": MODEL_CHOICES,
            "table_name": table_name,
            "search_field": search_field,
            "search_value": search_value,
            "records": records_data,
        }
        return self.template_historical, context

    async def safe_get_attr(self, obj, attr_name):
        # 同步处理函数
        def _sync_get_attr(obj, attr_name):
            try:
                # 先尝试直接获取外键ID而不触发查询
                field_id = getattr(obj, f"{attr_name}_id", None)

                # 如果外键ID存在，再尝试获取对象
                if field_id is not None:
                    try:
                        value = getattr(obj, attr_name)
                        if isinstance(value, Model):
                            return str(value)
                    except ObjectDoesNotExist:
                        return f"关联对象(ID:{field_id})不存在"

                # 处理普通字段或空值
                value = getattr(obj, attr_name, None)
                if value is None:
                    return "空"

                # 处理ManyToMany等关系
                if hasattr(value, "all"):
                    return ", ".join(str(item) for item in value.all()[:5])

                return str(value)

            except Exception as e:
                return f"错误: {str(e)}"

        return await sync_to_async(_sync_get_attr, thread_sensitive=True)(
            obj, attr_name
        )

    # 智能判断值是否真正发生变化
    def is_value_changed(self, old_val, new_val):
        if old_val is None and new_val is None:
            return False
        if old_val is None or new_val is None:
            return True

        # 比较日期有没有改变
        if hasattr(old_val, "isoformat") and hasattr(new_val, "isoformat"):
            return old_val.isoformat() != new_val.isoformat()

        old_str = str(old_val.pk) if hasattr(old_val, "pk") else str(old_val)
        new_str = str(new_val.pk) if hasattr(new_val, "pk") else str(new_val)
        if str(old_str).strip() == str(new_str).strip():
            return False

        return old_val != new_val

    # 统一处理布尔类型字段的界面显示
    def get_bool_display(self, field_name_cn, old_value, new_value):
        if old_value is None and new_value is True:
            return f"确认{field_name_cn}"
        elif old_value is None and new_value is False:
            return f"取消{field_name_cn}"
        elif old_value == new_value:
            return f"{'已' if new_value else '未'}{field_name_cn}"
        elif old_value is True and new_value is False:
            return f"取消{field_name_cn}"
        elif old_value is False and new_value is True:
            return f"确认{field_name_cn}"
        else:
            return f"{'已' if new_value else '未'}{field_name_cn}"

    async def handle_order_profit_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        today = datetime.today()
        six_months_ago_first_day = (today + relativedelta(months=-6)).replace(day=1)
        last_month_last_day = today + relativedelta(months=-1, day=31)

        start_date = (
            six_months_ago_first_day.strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            last_month_last_day.strftime("%Y-%m-%d") if not end_date else end_date
        )

        customers = await sync_to_async(
            lambda: {"----": None, **{c.zem_name: c.id for c in Customer.objects.all()}}
        )()
        customer_idlist = request.POST.getlist("customer")
        warehouse_list = request.POST.getlist("warehouse")

        date_type = request.POST.get("date_type")

        if date_type == "eta":
            criteria = Q(
                Q(vessel_id__vessel_eta__gte=start_date),
                Q(vessel_id__vessel_eta__lte=end_date),
                ~Q(order_type="直送"),
            )
        else:
            criteria = Q(
                Q(vessel_id__vessel_etd__gte=start_date),
                Q(vessel_id__vessel_etd__lte=end_date),
                ~Q(order_type="直送"),
            )
        if warehouse_list:
            criteria &= Q(retrieval_id__retrieval_destination_area__in=warehouse_list)
        if customer_idlist:
            customer_list = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist).values("zem_name")
            )
            customer_idlist = [item["zem_name"] for item in customer_list]
            criteria &= Q(customer_name__zem_name__in=customer_idlist)
        # 展示财务确认的账单
        criteria &= Q(receivable_status__stage="confirmed")
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "customer_name",
                "warehouse",
                "retrieval_id",
                "payable_status",
                "container_number",
            )
            .filter(criteria)
            .annotate(count=Count("id"))
        )

        results = []
        total_income = 0
        total_expense = 0
        total_profit = 0
        profit_values = []
        total_preport_receivable = 0
        total_preport_payable = 0
        for order in orders:
            container_number = order.container_number.container_number
            customer_name = order.customer_name.zem_name
            # 因为都是财务确认的账单，如果是早期的账单或者当前的组合柜账单，都是以invoice_item表为准的，否则是以这三个表为准
            invoice = await sync_to_async(
                Invoice.objects.select_related("customer", "container_number").get
            )(container_number__container_number=container_number)

            has_items = await sync_to_async(
                InvoiceItem.objects.filter(
                    invoice_number__invoice_number=invoice.invoice_number
                ).exists
            )()

            if not has_items:  # 没有说明是以三个表为准
                receivable_preport = (
                    float(invoice.receivable_preport_amount)
                    if invoice.receivable_preport_amount is not None
                    else 0.0
                )  # 港前提拆
                receivable_warehouse = (
                    float(invoice.receivable_warehouse_amount)
                    if invoice.receivable_warehouse_amount is not None
                    else 0.0
                )  # 库内
                receivable_delivery = (
                    float(invoice.receivable_delivery_amount)
                    if invoice.receivable_delivery_amount is not None
                    else 0.0
                )  # 派送
                other_fees = 0
            else:
                items = await sync_to_async(list)(
                    InvoiceItem.objects.filter(
                        invoice_number__invoice_number=invoice.invoice_number
                    )
                )
                categorized = self.categorize_invoice_items(items)
                receivable_preport = categorized["preport_receivable"]  # 港前提拆
                receivable_warehouse = categorized["warehouse_fee"]  # 库内
                receivable_delivery = categorized["delivery_fee"]  # 派送
                other_fees = categorized["other_fees"]

            # 港前应付
            payable_preport = (
                float(invoice.payable_preport_amount)
                if invoice.payable_preport_amount is not None
                else 0.0
            )  
            # 库内应付
            payable_warehouse = (
                float(invoice.payable_warehouse_amount)
                if invoice.payable_warehouse_amount is not None
                else 0.0
            )  
            #派送应付
            payable_delivery = (
                float(invoice.payable_delivery_amount)
                if invoice.payable_delivery_amount is not None
                else 0.0
            )

            total_preport_receivable += receivable_preport
            total_preport_payable += payable_preport
            # 总收入
            total_income_per_container = (
                receivable_preport
                + receivable_warehouse
                + receivable_delivery
                + other_fees
            )
            # 提拆的成本
            total_expense_per_container = payable_preport

            # 柜子的利润
            profit_per_container = (
                total_income_per_container
                - total_expense_per_container
                - payable_delivery
            )
            # 利润率
            profit_margin = (
                (profit_per_container / total_income_per_container * 100)
                if total_income_per_container
                else 0
            )
            # 总收入
            total_income += total_income_per_container
            # 总支出
            total_expense += total_expense_per_container
            total_expense += payable_delivery
            # 总利润
            total_profit += profit_per_container
            # 所有柜子的利润
            profit_values.append(profit_per_container)

            results.append(
                {
                    "container_number": container_number,
                    "customer_name": customer_name,
                    "receivable_preport": receivable_preport,
                    "receivable_warehouse": receivable_warehouse,
                    "receivable_delivery": receivable_delivery,
                    "payable_preport": payable_preport,
                    "payable_warehouse": payable_warehouse,
                    "payable_delivery": payable_delivery,
                    "total_income": total_income_per_container,
                    "total_expense": total_expense_per_container,
                    "profit": profit_per_container,
                    "profit_margin": profit_margin,
                }
            )
        if profit_values and len(profit_values) > 1:
            max_profit = max(profit_values)
            min_profit = min(profit_values)
        else:
            max_profit = min_profit = 0
        # 总利润率
        total_profit_margin = (total_profit / total_income * 100) if total_income else 0
        if total_preport_receivable != 0:
            preport_profit_margin = (
                total_preport_receivable - total_preport_payable
            ) / total_preport_receivable
        else:
            preport_profit_margin = 0
        delivery_profit_margin = 0
        context = {
            "results": results,
            "total_income": total_income,
            "total_expense": total_expense,
            "total_profit": total_profit,
            "total_profit_margin": total_profit_margin,
            "preport_profit_margin": preport_profit_margin,
            "delivery_profit_margin": delivery_profit_margin,
            "customers": customers,
            "warehouse_list": warehouse_list,
            "area_options": self.area_options,
            "max_profit": max_profit,
            "min_profit": min_profit,
            "start_date": start_date,
            "end_date": end_date,
            "date_type": date_type,
        }
        return self.template_profit, context

    def categorize_invoice_items(self, items: Any) -> dict:
        # 定义分类规则（使用正则表达式匹配）
        category_patterns = {
            "preport_receivable": [
                r"滞[港箱]费",
                r"等待费",
                r"等候费",
                r"提拆",
                r"提柜",
                r"车架",
                r"查验费",
                r"超重",
                r"操作费",
                r"拥堵费",
                r"还空",
                r"跑空费",
                r"升降机费",
                r"加急提柜",
                r"预提费",
                r"码头直送",
                r"打托费",
                r"代付手续费",
                r"拖车费",
                r"底盘分离费",
                r"拆柜费",
                r"PNCT",
                r"GD",
                r"NYCT Toll Fee",
                r"SOC",
                r"LA码头直送",
                r"DG",
            ],
            "warehouse_fee": [
                r"FBA",
                r"贴标",
                r"仓储",
                r"仓租",
                r"标签",
                r"覆盖",
                r"拍照",
                r"拍视频",
                r"分拣",
                r"清点",
                r"拣货",
                r"修补",
                r"修复",
                r"销毁",
                r"开封",
                r"打板",
                r"打托",
                r"缠胶",
                r"出库",
                r"拦截",
                r"指定贴标",
                r"内外箱",
                r"托盘标签",
                r"重复操作",
                r"重新打板",
                r"重新打托",
                r"货品清点",
            ],
            "delivery_fee": [
                r"派送",
                r"超[板区仓]",
                r"PO激活",
                r"激活",
                r"提货费",
                r"改派",
                r"加班送仓",
                r"自提",
                r"外配费用",
                r"随仓单费",
                r"MGE3",
                r"GA 30518",
            ],
        }

        # 初始化分类结果
        categorized = {
            "preport_receivable": 0,
            "warehouse_fee": 0,
            "delivery_fee": 0,
            "other_fees": 0,
        }

        for item in items:
            description = item.description.strip()
            amount = item.amount
            # 标记是否已分类
            classified = False

            # 检查每个分类
            for category, patterns in category_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, description, re.IGNORECASE):
                        categorized[category] += amount
                        classified = True
                        break
                if classified:
                    break

            # 未匹配任何分类的项
            if not classified:
                categorized["other_fees"] += amount

        return categorized

    async def handle_order_quantity_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        today = datetime.today()
        six_months_ago_first_day = (today + relativedelta(months=-6)).replace(day=1)
        last_month_last_day = today + relativedelta(months=-1, day=31)

        start_date = (
            six_months_ago_first_day.strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            last_month_last_day.strftime("%Y-%m-%d") if not end_date else end_date
        )

        customers = await sync_to_async(
            lambda: {"----": None, **{c.zem_name: c.id for c in Customer.objects.all()}}
        )()
        customer_idlist = request.POST.getlist("customer")
        warehouse_list = request.POST.getlist("warehouse")
        order_type = request.POST.get("order_type")
        date_type = request.POST.get("date_type")
        trunc_m = "vessel_id__vessel_eta" if date_type == "eta" else "created_at"

        if order_type == "直送":
            if date_type == "eta":
                criteria = Q(
                    Q(vessel_id__vessel_eta__gte=start_date),
                    Q(vessel_id__vessel_eta__lte=end_date),
                    Q(order_type=order_type),
                )
            else:
                criteria = Q(
                    Q(created_at__gte=start_date),
                    Q(created_at__lte=end_date),
                    Q(order_type=order_type),
                )
        else:
            if date_type == "eta":
                criteria = Q(
                    Q(vessel_id__vessel_eta__gte=start_date),
                    Q(vessel_id__vessel_eta__lte=end_date),
                    ~Q(order_type="直送"),
                )
            else:
                criteria = Q(
                    Q(created_at__gte=start_date),
                    Q(created_at__lte=end_date),
                    ~Q(order_type="直送"),
                )
            if warehouse_list:
                criteria &= Q(
                    retrieval_id__retrieval_destination_area__in=warehouse_list
                )
        if customer_idlist:
            customer_list = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist).values("zem_name")
            )
            customer_idlist = [item["zem_name"] for item in customer_list]
            criteria &= Q(customer_name__zem_name__in=customer_idlist)
        # 柱状图
        labels, legend, orders = await self._get_bar_chart(criteria, trunc_m)
        # 表格
        table = await self._get_table_chart(criteria, labels, trunc_m)
        # 饼图
        customer_labels, customer_data, month_labels, month_data = (
            await self._get_pie_chart(criteria, trunc_m)
        )
        # 折线图
        line_chart_data = await self._get_line_chart(criteria, trunc_m)
        context = {
            "area_options": self.area_options,
            "customers": customers,
            "customer_list": customer_idlist,
            "start_date": start_date,
            "end_date": end_date,
            "order_type": order_type,
            "warehouse_list": warehouse_list,
            "labels": labels,
            "orders": orders,
            "legend": legend,
            "table": table,
            "customer_labels": customer_labels,
            "customer_data": customer_data,
            "month_labels": month_labels,
            "month_data": month_data,
            "line_chart_data": line_chart_data,
            "date_type": date_type,
        }

        return self.template_shipment, context

    async def _get_line_chart(self, criteria, truc_m) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth(truc_m))  # 将 created_at 截断到月份
            .values("customer_name__zem_name", "month")
            .annotate(count=Count("id"))
            .order_by("customer_name__zem_name", "month")
        )
        customer_month_orders = defaultdict(lambda: defaultdict(int))
        for order in orders:
            customer = order["customer_name__zem_name"]
            month = order["month"].strftime("%Y年%m月")  # 格式化月份
            customer_month_orders[customer][month] += order["count"]

        # 获取所有月份
        all_months = sorted(
            set(
                month
                for customer in customer_month_orders
                for month in customer_month_orders[customer]
            )
        )

        # 构建折线图数据
        line_chart_data = {"labels": all_months, "datasets": []}  # 横轴：所有月份

        # 为每个客户生成一条线
        for i, (customer, month_orders) in enumerate(customer_month_orders.items()):
            data = [month_orders.get(month, 0) for month in all_months]
            line_chart_data["datasets"].append(
                {
                    "label": customer,
                    "data": data,
                    "borderColor": f"#{random.randint(0, 0xFFFFFF):06x}",
                    "fill": False,
                }
            )

        line_chart_data_json = json.dumps(line_chart_data)
        return line_chart_data_json

    async def _get_pie_chart(self, criteria, truc_m) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth(truc_m))  # 将 created_at 截断到月份
            .values("customer_name__zem_name", "month")
            .annotate(count=Count("id"))
            .order_by("customer_name__zem_name", "month")
        )
        # 以客户为分类处理订单
        customer_orders = defaultdict(int)
        for order in orders:
            customer_orders[order["customer_name__zem_name"]] += order["count"]
        customer_labels = list(customer_orders.keys())
        customer_data = list(customer_orders.values())

        # 以月份为分类处理订单
        month_orders = defaultdict(int)
        for order in orders:
            month_key = order["month"].strftime("%Y年%m月")
            month_orders[month_key] += order["count"]
        month_labels = list(month_orders.keys())
        month_data = list(month_orders.values())
        return [customer_labels, customer_data, month_labels, month_data]

    async def _get_bar_chart(self, criteria, truc_m) -> list:
        order_list = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth(truc_m))  # 将 created_at 截断到月份
            .values("month")
            .annotate(count=Count("id"))
            .order_by("month")
        )
        labels = [order["month"].strftime("%Y年%m月") for order in order_list]
        if not labels:
            return None, None, None
        legend = [
            int((labels[0][5:7]).lstrip("0")),
            int((labels[-1][5:7]).lstrip("0")),
        ]
        orders = [order["count"] for order in order_list]
        return labels, legend, orders

    async def _get_table_chart(self, criteria, direct_labels, truc_m) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth(truc_m))  # 将 created_at 截断到月份
            .values("customer_name__zem_name", "month")
            .annotate(count=Count("id"))
            .order_by("customer_name__zem_name", "month")
        )
        d_orders = {}
        for item in orders:
            customer_name = item["customer_name__zem_name"]
            month_str = item["month"].strftime("%Y年%m月")
            count = item["count"]
            if customer_name not in d_orders:
                d_orders[customer_name] = {}
            d_orders[customer_name][month_str] = count
        # 填充缺失的月份
        for customer_name, data in d_orders.items():
            for month in direct_labels:
                if month not in data:
                    data[month] = 0

        for k, v in d_orders.items():
            max_value = max(v.values(), default=0)  # 每个客户的订单量最大值
            previous_value = None
            for month, current_value in sorted(v.items()):
                if previous_value is not None:  # 非首列
                    if previous_value == current_value:
                        growth_percentage = 0
                    else:
                        if previous_value == 0:
                            if current_value > 0:
                                growth_percentage = "+"
                            else:
                                growth_percentage = "异常"
                        else:
                            if current_value == 0:
                                growth_percentage = "-"
                            else:
                                growth_percentage = np.round(
                                    (current_value - previous_value)
                                    / previous_value
                                    * 100,
                                    2,
                                )
                    is_max = 1 if current_value == max_value else 0
                    d_orders[k][month] = [current_value, growth_percentage, is_max]
                    previous_value = current_value
                else:
                    is_max = 1 if current_value == max_value else 0
                    d_orders[k][month] = [current_value, 0, is_max]
                    previous_value = current_value
        return d_orders

    async def _validate_user_group(self, user: User) -> bool:
        is_staff = await sync_to_async(lambda: user.is_staff)()
        if is_staff:
            return True
        return await sync_to_async(
            lambda: user.groups.filter(name="leaders").exists()
        )()

    async def _validate_user_manage(self, user: User) -> bool:
        is_staff = await sync_to_async(lambda: user.is_staff)()
        if is_staff:
            return True
        return await sync_to_async(
            lambda: user.groups.filter(name="history_search").exists()
        )()

    async def _validate_user_profit(self, user: User) -> bool:
        is_staff = await sync_to_async(lambda: user.is_staff)()
        if is_staff:
            return True
        return await sync_to_async(
            lambda: user.groups.filter(name="order_profit").exists()
        )()
