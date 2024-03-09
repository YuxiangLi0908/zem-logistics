import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.customer import Customer
from warehouse.forms.customer_form import CustomerForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class BOL(View):
    pass