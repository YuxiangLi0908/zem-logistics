import pytz
import uuid
import pandas as pd
from datetime import datetime, timedelta
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, FloatField, IntegerField, When, Count
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template

from warehouse.models.offload import Offload
from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list


@method_decorator(login_required(login_url='login'), name='dispatch')
class Accounting(View):
    template_pallet_data = "accounting/pallet_data.html"
    allowed_group = "accounting"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")

        step = request.GET.get("step", None)
        if step == "pallet_data":
            template, context = self.handle_pallet_data_get()
            return render(request, template, context)
        else:
            raise ValueError(f"unknow request {step}")
        
    def post(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")

        step = request.POST.get("step", None)
        if step == "pallet_data_search":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            template, context = self.handle_pallet_data_get(start_date, end_date)
            return render(request, template, context)
        elif step == "pallet_data_export":
            return self.handle_pallet_data_export_post(request)
        else:
            raise ValueError(f"unknow request {step}")

    def handle_pallet_data_get(self, start_date: str = None, end_date: str = None) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = (current_date + timedelta(days=30)).strftime('%Y-%m-%d') if not end_date else end_date
        pallet_data = Order.objects.filter(
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) &
            models.Q(eta__gte=start_date) &
            models.Q(eta__lte=end_date)
        ).order_by("offload_id__offload_at")
        context = {
            "pallet_data": pallet_data,
            "start_date": start_date,
            "end_date": end_date,
        }
        return self.template_pallet_data, context
    
    def handle_pallet_data_export_post(self, request: HttpRequest) -> HttpResponse:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet_data = Order.objects.filter(
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) &
            models.Q(eta__gte=start_date) &
            models.Q(eta__lte=end_date)
        ).order_by("offload_id__offload_at")
        data = [
            {
                "货柜号": d.container_number.container_number,
                "客户": d.customer_name.zem_name,
                "入仓仓库": d.warehouse.name,
                "柜型": d.container_number.container_type,
                "拆柜完成时间": d.offload_id.offload_at.strftime('%Y-%m-%d %H:%M:%S'),
                "打板数": d.offload_id.total_pallet,
            } for d in pallet_data
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f"attachment; filename=pallet_data_{start_date}_{end_date}.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response

    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name=self.allowed_group).exists():
            return True
        else:
            return False