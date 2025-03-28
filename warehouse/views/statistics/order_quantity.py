import json
import random
from collections import defaultdict
from datetime import datetime
from typing import Any

import numpy as np
from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.contrib.auth.models import User
from django.db.models import Count, Q
from django.db.models.functions import TruncMonth
from django.http import Http404,HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.apps import apps
from django.views import View
from warehouse.utils.constants import (
    MODEL_CHOICES
)

from warehouse.models.customer import Customer
from warehouse.models.order import Order
from warehouse.models.container import Container
from django.contrib.auth.models import User



class OrderQuantity(View):
    template_shipment = "statistics/order_quantity.html"
    template_historical = "statistics/historical.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA"}

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.GET.get("step", None)
        if step =="historical_query":
            if not await self._validate_user_manage(request.user):
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )           
            context = {'model_choices':MODEL_CHOICES}
            return await sync_to_async(render)(request, self.template_historical, context) 
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
        if not await self._validate_user_group(request.user):
            return HttpResponseForbidden(
                "You are not authenticated to access this page!"
            )
        step = request.POST.get("step")
        if step == "selection":
            template, context = await self.handle_order_quantity_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "historical_selection":
            template, context = await self.handle_order_historical_get(request)
            return await sync_to_async(render)(request, template, context)

    async def handle_order_historical_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        table_name = request.POST.get("model").strip()
        search_field = request.POST.get("search_field").strip()
        search_value = request.POST.get("search_value").strip()

        model_info = MODEL_CHOICES.get(table_name)
        print('model_info',model_info["warehouse"])
        #try:
        original_model_name = model_info['model']
        original_model = apps.get_model('warehouse', original_model_name)
        
        # 处理外键查询
        if search_field == 'container_number' and hasattr(original_model, 'container_number'):
            container = await Container.objects.filter(container_number=search_value).afirst()
            if not container:
                raise Http404(f"找不到柜号 {search_value}")
            
            history_records = original_model.objects.filter(
                container_number_id=container.id
            ).select_related('history_user').order_by('-history_date')
        else:
            history_records = original_model.objects.filter(
                **{f"{search_field}__icontains": search_value}
            ).select_related('history_user').order_by('-history_date')
        
        # 转换为同步查询
        history_records = await sync_to_async(list)(history_records)
        
        # 准备显示数据
        records_data = []
        user_ids = {r.history_user_id for r in history_records if r.history_user_id}
        users = await sync_to_async(lambda: User.objects.filter(id__in=user_ids).in_bulk())()
        
        for record in history_records:
            changes = []
            prev_record = await sync_to_async(getattr)(record, 'prev_record', None)
            
            if prev_record:
                diff = await sync_to_async(getattr)(record, 'diff_prev', {})
                for field, (old_val, new_val) in diff.items():
                    changes.append({
                        'field': field,
                        'old_value': old_val,
                        'new_value': new_val
                    })
            
            records_data.append({
                'date': record.history_date,
                'user': users.get(record.history_user_id),
                'type': '创建' if record.history_type == '+' else 
                       '修改' if record.history_type == '~' else 
                       '删除',
                'changes': changes,
                'record': record
            })
        
        
        context = {
            'model_choices':MODEL_CHOICES,
            'table_name':table_name,
            'search_field':search_field,
            'search_value':search_value,
            'records': records_data,
        }
        # except Exception as e:
        #     context = {
        #         'error': f'查询错误: {str(e)}',
        #         'model_choices': MODEL_CHOICES
        #     }
        #await sync_to_async(print)(records_data)
        return self.template_historical, context



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

        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.id for c in customers}
        customers["----"] = None
        customers = {"----": None, **customers}
        customer_idlist = request.POST.getlist("customer")
        warehouse_list = request.POST.getlist("warehouse")

        order_type = request.POST.get("order_type")
        date_type = request.POST.get("date_type")
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
        labels, legend, orders = await self._get_bar_chart(criteria)
        # 表格
        table = await self._get_table_chart(criteria, labels)
        # 饼图
        customer_labels, customer_data, month_labels, month_data = (
            await self._get_pie_chart(criteria)
        )
        # 折线图
        line_chart_data = await self._get_line_chart(criteria)
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
            "date_type":date_type
        }

        return self.template_shipment, context

    async def _get_line_chart(self, criteria) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth("created_at"))  # 将 created_at 截断到月份
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

    async def _get_pie_chart(self, criteria) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth("created_at"))  # 将 created_at 截断到月份
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

    async def _get_bar_chart(self, criteria) -> list:
        order_list = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth("created_at"))  # 将 created_at 截断到月份
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

    async def _get_table_chart(self, criteria, direct_labels) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name", "warehouse", "retrieval_id")
            .filter(criteria)
            .annotate(month=TruncMonth("created_at"))  # 将 created_at 截断到月份
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
            lambda: user.groups.filter(name="manages").exists()
        )()
