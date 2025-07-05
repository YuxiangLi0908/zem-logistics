import json
import random
from collections import defaultdict
from datetime import datetime
from typing import Any

import numpy as np
import re
from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.apps import apps
from django.contrib.auth.models import User
from django.db.models import Count, Q, Sum, FloatField, Case, When
from django.db.models.functions import TruncMonth, Cast
from django.http import Http404, HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.views import View

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.invoice import Invoice, InvoiceItem
from warehouse.models.order import Order
from warehouse.models.retrieval import HistoricalRetrieval, Retrieval
from warehouse.utils.constants import MODEL_CHOICES


class OrderQuantity(View):
    template_shipment = "statistics/order_quantity.html"
    template_historical = "statistics/historical.html"
    template_profit = "statistics/profit.html"
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
            if not await self._validate_user_manage(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )      
            customers = await sync_to_async(list)(Customer.objects.all())
            customers = {c.zem_name: c.id for c in customers}
            customers["----"] = None
            customers = {"----": None, **customers}
            context = {"area_options": self.area_options, "customers": customers}
            return await sync_to_async(render)(
                request, self.template_profit, context
            )
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
                            getattr(
                                r, f.name + "_id", None
                            )  # 如果是关系字段，获取外键ID
                            if getattr(r, f.name) is None  # 如果外键对象为None
                            else getattr(r, f.name).pk  # 否则获取关联对象的主键
                        )
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
        value = getattr(obj, attr_name)
        if hasattr(value, "_meta"):  # 如果是模型实例
            return await sync_to_async(str)(value)  # 异步转字符串
        return str(value) if value is not None else "空"

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
            criteria &= Q(
                retrieval_id__retrieval_destination_area__in=warehouse_list
            )
        if customer_idlist:
            customer_list = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist).values("zem_name")
            )
            customer_idlist = [item["zem_name"] for item in customer_list]
            criteria &= Q(customer_name__zem_name__in=customer_idlist)
        #展示财务确认的账单
        criteria &= Q(payable_status__stage="confirmed")
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id","payable_status")
            .filter(criteria)
            .annotate(count=Count("id"))
        )
        container_numbers = await sync_to_async(lambda: [
            order.container_number.container_number 
            for order in orders 
            if order.container_number
        ])()

        invoices = await sync_to_async(list)(
            Invoice.objects.filter(container_number__container_number__in=container_numbers)
            .values('container_number__container_number')
            .annotate(
                preport_receivable=Sum(Cast('receivable_preport_amount', FloatField())),
                warehouse_fee=Sum(Cast('receivable_warehouse_amount', FloatField())),
                delivery_fee=Sum(Cast('receivable_delivery_amount', FloatField())),
                total_income=Sum(Cast('receivable_total_amount', FloatField())),
                preport_payable=Sum(Cast('payable_total_amount', FloatField()))
            )
        )
        
        invoice_dict = {inv['container_number__container_number']: inv for inv in invoices}
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
            #因为都是财务确认的账单，如果是早期的账单或者当前的组合柜账单，都是以invoice_item表为准的，否则是以这三个表为准
            invoice = await sync_to_async(
                Invoice.objects.select_related("customer", "container_number").get
                )(container_number__container_number=container_number)
            
            has_items = await sync_to_async(
                InvoiceItem.objects.filter(
                    invoice_number__invoice_number=invoice.invoice_number
                ).exists
            )()
            invoice_data = invoice_dict.get(container_number, {})    
            if not has_items: #没有说明是以三个表为准                  
                preport_receivable = invoice_data.get('preport_receivable', 0) or 0  #港前提拆
                warehouse_fee = invoice_data.get('warehouse_fee', 0) or 0       #库内
                delivery_fee = invoice_data.get('delivery_fee', 0) or 0         #派送
                preport_payable = invoice_data.get('preport_payable', 0) or 0   #应付
                other_fees = 0
            else:
                items = await sync_to_async(list)(
                    InvoiceItem.objects.filter(
                        invoice_number__invoice_number=invoice.invoice_number
                    )
                )
                categorized = self.categorize_invoice_items(items)
                preport_receivable = categorized['preport_receivable']  #港前提拆
                warehouse_fee = categorized['warehouse_fee']       #库内
                delivery_fee = categorized['delivery_fee']         #派送
                other_fees = categorized['other_fees']
                preport_payable = invoice_data.get('preport_payable', 0) or 0   #应付
            total_preport_receivable += preport_receivable
            total_preport_payable += preport_payable
            total_income_per_container = preport_receivable + warehouse_fee + delivery_fee + other_fees
            total_expense_per_container = preport_payable
            profit_per_container = total_income_per_container - total_expense_per_container
            profit_margin = (profit_per_container / total_income_per_container * 100) if total_income_per_container else 0
            
            total_income += total_income_per_container
            total_expense += total_expense_per_container
            total_profit += profit_per_container
            profit_values.append(profit_per_container)
            
            results.append({
                'container_number': container_number,  
                "customer_name":customer_name,           
                'preport_receivable': preport_receivable,
                'preport_payable': preport_payable,
                'warehouse_fee': warehouse_fee,
                'delivery_fee': delivery_fee,
                'total_income': total_income_per_container,
                'total_expense': total_expense_per_container,
                'profit': profit_per_container,
                'profit_margin': profit_margin
            })
        if profit_values and len(profit_values) > 1:
            max_profit = max(profit_values)
            min_profit = min(profit_values)
        else:
            max_profit = min_profit = 0
        #总利润率
        total_profit_margin = (total_profit / total_income * 100) if total_income else 0
        if total_preport_receivable != 0:
            preport_profit_margin = (total_preport_receivable - total_preport_payable)/total_preport_receivable
        else:
            preport_profit_margin = O
        delivery_profit_margin = 0
        context = {
            'results': results,
            'total_income': total_income,
            'total_expense': total_expense,
            'total_profit': total_profit,
            'total_profit_margin': total_profit_margin,
            "preport_profit_margin": preport_profit_margin,
            "delivery_profit_margin": delivery_profit_margin,
            'customers': customers,
            "warehouse_list":warehouse_list,
            "area_options": self.area_options,
            'max_profit': max_profit,
            'min_profit': min_profit,
        }
        return self.template_profit,context

    def categorize_invoice_items(self, items:Any) -> dict:
        # 定义分类规则（使用正则表达式匹配）
        category_patterns = {
            'preport_receivable': [
                r'滞[港箱]费', r'等待费', r'等候费', r'提拆', r'提柜', r'车架', 
                r'查验费', r'超重', r'操作费', r'拥堵费', r'还空', r'跑空费',
                r'升降机费', r'加急提柜', r'预提费', r'码头直送', r'打托费',
                r'代付手续费', r'拖车费', r'底盘分离费', r'拆柜费', r'PNCT',
                r'GD', r'NYCT Toll Fee', r'SOC', r'LA码头直送', r'DG'
            ],
            'warehouse_fee': [
                r'FBA', r'贴标', r'仓储', r'仓租', r'标签', r'覆盖', 
                r'拍照', r'拍视频', r'分拣', r'清点', r'拣货', r'修补',
                r'修复', r'销毁', r'开封', r'打板', r'打托', r'缠胶',
                r'出库', r'拦截', r'指定贴标', r'内外箱', r'托盘标签',
                r'重复操作', r'重新打板', r'重新打托', r'货品清点'
            ],
            'delivery_fee': [
                r'派送', r'超[板区仓]', r'PO激活', r'激活', r'提货费',
                r'改派', r'加班送仓', r'自提', r'外配费用', r'随仓单费',
                r'MGE3', r'GA 30518'
            ]
        }

        # 初始化分类结果
        categorized = {
            'preport_receivable': 0,
            'warehouse_fee': 0,
            'delivery_fee': 0,
            'other_fees': 0
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
                categorized['other_fees'] += amount
        
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
