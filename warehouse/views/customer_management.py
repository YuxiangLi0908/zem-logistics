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
class CustomerManagement(View):
    template_main = "new_customer.html"
    context: dict[str, Any] = {}

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "new":
            return self.handle_new_customer_get(request)
        else:
            raise ValueError(f"{request}")
        return render(request, self.template_main, self.context)
    
    def post(self, request:HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "new":
            return self.handle_new_customer_post(request)
        else:
            raise ValueError(f"{request}")
        return render(request, self.template_main, self.context)
    
    def handle_new_customer_get(self, request: HttpRequest) -> HttpResponse:
        self.context["customer_form"] = CustomerForm()
        return render(request, self.template_main, self.context)
    
    def handle_new_customer_post(self, request: HttpRequest) -> HttpResponse:
        form = CustomerForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('home')
        else:
            raise ValueError(f"{request}")
        
