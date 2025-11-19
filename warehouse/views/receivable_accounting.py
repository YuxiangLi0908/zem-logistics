import ast
import io
import json
import math
import os
import re
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, time as datetime_time
from io import BytesIO
from itertools import chain, groupby
from operator import attrgetter

from asgiref.sync import sync_to_async, async_to_sync

from django.db import transaction
from django.db.models.fields.json import KeyTextTransform
from django.core.paginator import Paginator

from typing import Any

import openpyxl
import openpyxl.workbook
import openpyxl.worksheet
import openpyxl.worksheet.worksheet
import pandas as pd
import pytz
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.postgres.aggregates import ArrayAgg, StringAgg
from django.core.paginator import Paginator
from django.db import models, transaction
from django.db.models import (
    BooleanField,
    Case,
    CharField,
    Count,
    Exists,
    F,
    FloatField,
    IntegerField,
    OuterRef,
    Prefetch,
    Subquery,
    Sum,
    Value,
    When,
)
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast, Coalesce
from django.db.models.query import QuerySet
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from openpyxl.utils.dataframe import dataframe_to_rows
from simple_history.utils import bulk_create_with_history, bulk_update_with_history
from sqlalchemy.util import await_only

from warehouse.forms.order_form import OrderForm
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice import (
    Invoice,
    InvoiceItem,
    InvoiceStatement,
    InvoiceStatus,
)
from warehouse.models.invoice_details import (
    InvoiceDelivery,
    InvoicePreport,
    InvoiceWarehouse,
)
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.retrieval import Retrieval
from warehouse.models.transaction import Transaction
from warehouse.views.quote_management import QuoteManagement
from warehouse.forms.order_form import OrderForm
from warehouse.utils.constants import (
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE,
    APP_ENV,
    CARRIER_FLEET,
    CONTAINER_PICKUP_CARRIER,
    SP_DOC_LIB,
    SP_CLIENT_ID,
    SP_DOC_LIB,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    SYSTEM_FOLDER,
)
from warehouse.views.export_file import export_invoice


class ReceivableAccounting(View):
    template_progress_overview = "receivable_accounting/progress_overview.html"
    template_alert_monitoring = "receivable_accounting/alert_monitoring.html"
    template_preport_entry = "receivable_accounting/preport_entry.html"
    template_warehouse_entry = "receivable_accounting/warehouse_entry.html"
    template_delivery_entry = "receivable_accounting/delivery_entry.html"
    template_pending_confirmation = "receivable_accounting/pending_confirmation.html"
    template_completed_bills = "receivable_accounting/completed_bills.html"
    template_financial_statistics = "receivable_accounting/financial_statistics.html"
    template_quotation_management = "receivable_accounting/quotation_management.html"
    
    allowed_group = "accounting"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "LA-91748": "LA-91748",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
        "LA-91789": "LA-91789",
        "直送": "直送",
    }

    def get(self, request: HttpRequest) -> HttpResponse:
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.GET.get("step", None)
        if step == "progress":
            template, context = self.handle_progress_overview_get()
            return render(request, template, context)
        elif step == "alert":
            template, context = self.handle_alert_monitoring_get()
            return render(request, template, context)
        elif step == "preport":
            template, context = self.handle_preport_entry_get(request)
            return render(request, template, context)
        elif step == "warehouse":
            template, context = self.handle_warehouse_entry_get(request)
            return render(request, template, context)
        elif step == "delivery":  # 提拆柜账单录入
            template, context = self.handle_delivery_entry_get(request)
            return render(request, template, context)
        elif step == "confirm":  # 库内账单录入
            template, context = self.handle_pending_confirmation_get(request)
            return render(request, template, context)
        elif step == "completed":
            template, context = self.handle_completed_bills_get(request)
            return render(request, template, context)
        elif step == "finance_stats":
            template, context = self.handle_financial_statistics_get(request)
            return render(request, template, context)
        elif step == "quotation_management":
            quotes = QuotationMaster.objects.filter(quote_type="receivable")
            context = {"order_form": OrderForm(), "quotes": quotes}
            return render(request, self.template_quotation_management, context)  
            
        else:
            raise ValueError(f"unknow request {step}")

    def post(self, request: HttpRequest) -> HttpResponse:
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.POST.get("step", None)
        
    
   
        