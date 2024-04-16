import pytz
from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count

from warehouse.models.customer import Customer
from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.customer_form import CustomerForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_bol

@method_decorator(login_required(login_url='login'), name='dispatch')
class POD(View):
    template_main = "pod.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        pass

    def post(self, request: HttpRequest) -> HttpResponse:
        pass