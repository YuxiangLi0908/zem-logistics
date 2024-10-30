import ast
import uuid
import os,json
import pandas as pd
from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, Max, FloatField, IntegerField, When, Count, Q
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    amazon_fba_locations,
    APP_ENV,
    LOAD_TYPE_OPTIONS,
    SP_USER,
    SP_PASS,
    SP_URL,
    SP_DOC_LIB,
    SYSTEM_FOLDER,
)


class FleetManagement(View):
    template_fleet_warehouse_search = "post_port/shipment/abnormal/01_fleet_management_main.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("fleet_warehouse_search")
        if step == "fleet_info":
            pass
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
        else:
            return await self.get(request)
        
    async def handle_fleet_warehouse_search_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("name")
        fleet = await sync_to_async(list)(
            Fleet.objects.select_related("shipment").filter(
                origin=warehouse,
                is_canceled=False,
            ).values(
                "fleet_number", "appointment_datetime",
            ).annotate(
                shipment_batch_number=StringAgg("shipment__shipment_batch_number", delimiter=",", distinct=True),
                appointment_id=StringAgg("shipment__appointment_id", delimiter=",", distinct=True),
                destination=StringAgg("shipment__destination", delimiter=",", distinct=True),
            ).order_by("appointment_datetime")
        )
        context = {
            "warehouse": warehouse,
            "fleet": fleet,
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse})
        }
        return self.template_fleet_warehouse_search, context

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False