from typing import Any
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.utils.decorators import method_decorator
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View
from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from django.db.models import Q,Count
from django.db.models.functions import TruncMonth
from collections import defaultdict

from warehouse.forms.order_form import OrderForm
from warehouse.models.order import Order
from warehouse.models.customer import Customer

class OrderQuantity(View):
    template_shipment = "statistics/order_quantity.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA":"LA"}
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761":"LA-91761",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        customers["----"] = None 
        customers = {"----": None, **customers} 
        context = {
            "warehouse_options": self.warehouse_options,
            "customers":customers
        }
        return await sync_to_async(render)(request, self.template_shipment, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.POST.get("step")
        if step == "selection":
            template, context = await self.handle_order_quantity_get(request)
            return await sync_to_async(render)(request, template, context)
    
    async def handle_order_quantity_get(self, request: HttpRequest) ->  tuple[str, dict[str, Any]]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        today = datetime.today()
        six_months_ago_first_day = (today + relativedelta(months=-6)).replace(day=1)
        last_month_last_day = today + relativedelta(months=-1, day=31)
        
        start_date = six_months_ago_first_day.strftime('%Y-%m-%d') if not start_date else start_date
        end_date = last_month_last_day.strftime('%Y-%m-%d') if not end_date else end_date

        customers = await sync_to_async(list)(Customer.objects.all())
        customers = { c.zem_name: c.id for c in customers}
        customers["----"] = None 
        customers = {"----": None, **customers} 
        customer_id = request.POST.get("customer")

        warehouse = request.POST.get('warehouse')

        criteria = Q(
            Q(created_at__gte=start_date),
            Q(created_at__lte=end_date)
        )
        if customer_id and customer_id.lower() != "none":
            customer = await sync_to_async(Customer.objects.get)(id=customer_id)
            criteria &= Q(customer_name__zem_name=customer)     
        if warehouse:
            criteria = Q(warehouse__name=warehouse)
        #直送和转运的柱状图
        transfer_labels,transfer_legend,transfer_orders = await self._transfer_bar_chart(criteria)
        
        direct_labels,direct_legend,direct_orders = await self._direct_bar_chart(criteria)
        #表格
        transfer_table = await self._transfer_table_chart(criteria,transfer_labels)
        direct_table = await self._direct_table_chart(criteria,direct_labels)
        
        context = {
            "warehouse_options": self.warehouse_options,
            "customers":customers,
            "customer":customer_id,
            "warehouse":warehouse,
            "transfer_labels":transfer_labels,
            "transfer_orders":transfer_orders,
            "direct_labels":direct_labels,
            "direct_orders":direct_orders,           
            "start_date":start_date,
            "end_date":end_date,
            "warehouse":warehouse,
            "transfer_legend":transfer_legend,
            "direct_legend":direct_legend,
            'transfer_table': transfer_table,
            "direct_table":direct_table
        }
        
        return self.template_shipment, context

    async def _transfer_bar_chart(self,criteria) -> list:
        transfers = await sync_to_async(list)(
            Order.objects.select_related("customer_name","warehouse")
            .filter(criteria)
            .filter(~Q(order_type='直送'))
            .annotate(month=TruncMonth('created_at'))  # 将 created_at 截断到月份
            .values('month')  
            .annotate(count=Count('id'))
            .order_by('month') 
        )
        
        transfer_labels = [order['month'].strftime('%Y年%m月') for order in transfers]
        transfer_legend = [int((transfer_labels[0][5:7]).lstrip('0')),int((transfer_labels[-1][5:7]).lstrip('0'))]
        transfer_orders = [order['count'] for order in transfers]
        return transfer_labels,transfer_legend,transfer_orders

    async def _transfer_table_chart(self,criteria,transfer_labels) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name","warehouse")
            .filter(criteria)
            .filter(~Q(order_type='直送'))
            .annotate(month=TruncMonth('created_at'))  # 将 created_at 截断到月份
            .values('customer_name__zem_name', 'month')
            .annotate(count=Count('id'))
            .order_by('customer_name__zem_name', 'month') 
        )
        t_orders = {}
        for item in orders:
            customer_name = item['customer_name__zem_name']
            month_str = item['month'].strftime('%Y年%m月')
            count = item['count']
            if customer_name not in t_orders:
                t_orders[customer_name] = {}
            t_orders[customer_name][month_str] = count
        #填充缺失的月份
        for customer_name, data in t_orders.items():
            for month in transfer_labels:
                if month not in data:
                    data[month] = 0
        
        for k,v in t_orders.items():
            max_value = max(v.values(), default=0)  #每个客户的订单量最大值
            previous_value = None
            for month,current_value in sorted(v.items()):
                if previous_value is not None:  #非首列
                    if previous_value == current_value:
                        growth_percentage = 0
                    else:
                        if previous_value == 0:
                            if current_value > 0:
                                growth_percentage = '+'
                            else:
                                growth_percentage = '异常'
                        else:
                            if current_value == 0:
                                growth_percentage = '-'
                            else:
                                growth_percentage = (current_value - previous_value) / previous_value * 100
                    is_max = 1 if current_value == max_value else 0
                    t_orders[k][month] = [current_value,growth_percentage,is_max]
                    previous_value = current_value
                else:
                    is_max = 1 if current_value == max_value else 0
                    t_orders[k][month] = [current_value,0,is_max]
                    previous_value = current_value
        return t_orders
    
    async def _direct_bar_chart(self,criteria) -> list:
        directs = await sync_to_async(list)(
            Order.objects.select_related("customer_name","warehouse")
            .filter(criteria)
            .filter(Q(order_type='直送'))
            .annotate(month=TruncMonth('created_at'))  # 将 created_at 截断到月份
            .values('month')  
            .annotate(count=Count('id'))
            .order_by('month') 
        )
        direct_labels = [order['month'].strftime('%Y年%m月') for order in directs]
        direct_legend = [int((direct_labels[0][5:7]).lstrip('0')),int((direct_labels[-1][5:7]).lstrip('0'))]
        direct_orders = [order['count'] for order in directs]
        return direct_labels,direct_legend,direct_orders

    async def _direct_table_chart(self,criteria,direct_labels) -> list:
        orders = await sync_to_async(list)(
            Order.objects.select_related("customer_name","warehouse")
            .filter(criteria)
            .filter(Q(order_type='直送'))
            .annotate(month=TruncMonth('created_at'))  # 将 created_at 截断到月份
            .values('customer_name__zem_name', 'month')
            .annotate(count=Count('id'))
            .order_by('customer_name__zem_name', 'month') 
        )
        d_orders = {}
        for item in orders:
            customer_name = item['customer_name__zem_name']
            month_str = item['month'].strftime('%Y年%m月')
            count = item['count']
            if customer_name not in d_orders:
                d_orders[customer_name] = {}
            d_orders[customer_name][month_str] = count
        #填充缺失的月份
        for customer_name, data in d_orders.items():
            for month in direct_labels:
                if month not in data:
                    data[month] = 0
        
        for k,v in d_orders.items():
            max_value = max(v.values(), default=0)  #每个客户的订单量最大值
            previous_value = None
            for month,current_value in sorted(v.items()):
                if previous_value is not None:  #非首列
                    if previous_value == current_value:
                        growth_percentage = 0
                    else:
                        if previous_value == 0:
                            if current_value > 0:
                                growth_percentage = '+'
                            else:
                                growth_percentage = '异常'
                        else:
                            if current_value == 0:
                                growth_percentage = '-'
                            else:
                                growth_percentage = (current_value - previous_value) / previous_value * 100
                    is_max = 1 if current_value == max_value else 0
                    d_orders[k][month] = [current_value,growth_percentage,is_max]
                    previous_value = current_value
                else:
                    is_max = 1 if current_value == max_value else 0
                    d_orders[k][month] = [current_value,0,is_max]
                    previous_value = current_value
        return d_orders

    async def _validate_user_group(self, user: User) -> bool:
        is_staff = await sync_to_async(lambda: user.is_staff)()
        if is_staff:
            return True
        return await sync_to_async(lambda: user.groups.filter(name="leaders").exists())()

    