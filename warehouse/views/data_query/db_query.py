import csv
import uuid
import os
import pandas as pd
import numpy as np
from asgiref.sync import sync_to_async
from pathlib import Path
from datetime import datetime
from typing import Any

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render, redirect
from django.views import View
from django.db import models, connection
from django.core.cache import cache
from django.db.models import Count
from django.utils.decorators import method_decorator

from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.offload import Offload
from warehouse.models.vessel import Vessel
from warehouse.forms.upload_file import UploadFileForm
from warehouse.utils.constants import (
    PACKING_LIST_TEMP_COL_MAPPING, SHIPPING_LINE_OPTIONS,
    DELIVERY_METHOD_OPTIONS
)

@method_decorator(login_required(login_url='login'), name='dispatch')
class DBConn(View):
    template = "db_connection/custom_sql.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_staff:
            return HttpResponseForbidden("You don't have permission to access this page.")
        context = {}
        return render(request, self.template, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "execute_query":
            return self.execute_sql(request)
        else:
            return self.get(request)
        
    def execute_sql(self, request: HttpRequest) -> HttpResponse:
        sql = request.POST.get("sql")
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            results = cursor.fetchall()
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="exported_data.csv"'
        writer = csv.writer(response)
        writer.writerow(columns)
        for row in results:
            writer.writerow(row)
        return response


        

