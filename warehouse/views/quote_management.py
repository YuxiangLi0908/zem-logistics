import string
import random

import pandas as pd
from typing import Any
from datetime import datetime

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.forms import modelformset_factory, formset_factory

from warehouse.models.quote import Quote
from warehouse.forms.quote_form import QuoteForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class QuoteManagement(View):
    template_create = "quote/quote_creation.html"
    template_update = "quote/quote_list.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "new":
            return render(request, self.template_create, self.handle_new_quote_get(request))
        context = {}
        return render(request, self.template_create, context)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "create":
            return render(request, self.template_create, self.handle_create_post(request))
        elif step == "export_single_quote_excel":
            return self.handle_single_excel_export(request)

    def handle_new_quote_get(self, request: HttpRequest) -> dict[str, Any]:
        quote_form = QuoteForm()
        quote_formset = formset_factory(QuoteForm, extra=1)
        context = {
            "quote_form": quote_form,
            "quote_formset": quote_formset,
            "step": "create",
        }
        return context
    
    def handle_create_post(self, request: HttpRequest) -> dict[str, Any]:
        quote_form_p1 = QuoteForm(request.POST)
        timestamp = datetime.now()
        all_quotes = []
        if quote_form_p1.is_valid():
            cleaned_data_p1 = quote_form_p1.cleaned_data
            customer = cleaned_data_p1.get('customer_name')
            customer_id = customer.id if customer else 999
            id_prefix = f"QT{timestamp.strftime('%Y%m%d')}{''.join(random.choices(string.ascii_uppercase, k=2))}{customer_id}"
            parent_id = f"{id_prefix}{''.join(random.choices(string.ascii_uppercase, k=2))}{timestamp.strftime('%H%M')}"
            quote_formset = formset_factory(QuoteForm, extra=1)
            quote_form = quote_formset(request.POST)
            q_valid = all([q.is_valid() for q in quote_form])
            if q_valid:
                cleaned_data_p2 = [q.cleaned_data for q in quote_form]
                i = 1
                for d in cleaned_data_p2:
                    data = {}
                    quote_id = f"{parent_id}{''.join(random.choices(string.ascii_uppercase, k=2))}{i}"
                    data.update(cleaned_data_p1)
                    data["created_at"] = timestamp.date()
                    data["parent_id"] = parent_id
                    data["quote_id"] = quote_id
                    data["load_type"] = d["load_type"]
                    data["is_lift_gate"] = d["is_lift_gate"]
                    data["cost"] = d["cost"]
                    data["price"] = d["price"]
                    quote = Quote(**data)
                    all_quotes.append(quote)
                    quote.save()
                    i += 1
            else:
                raise RuntimeError(f"invalid 报价!")
        else:
            raise RuntimeError(f"invalid 询价信息!")
        context = {}
        context["parent_id"] = parent_id
        context["all_quotes"] = all_quotes
        context["step"] = "review"
        return context
    
    def handle_single_excel_export(self, request: HttpRequest) -> HttpResponse:
        parent_id = request.POST.get("parent_id")
        quote = Quote.objects.filter(
            parent_id=parent_id
        ).values(
            "quote_id", "customer_name__full_name", "created_at", "warehouse__name", "zipcode",
            "address", "load_type", "is_lift_gate", "price"
        ).order_by("-price")
        data = [q for q in quote]
        df = pd.DataFrame.from_records(data)
        df = df.rename({
            "quote_id": "询盘号",
            "customer_name__full_name": "客户",
            "created_at": "询盘日期",
            "warehouse__name": "发货仓库",
            "zipcode": "目的地",
            "address": "详细地址",
            "load_type": "FTL/LTL",
            "is_lift_gate": "是否带尾板",
            "price": "报价($)",
        }, axis=1)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f"attachment; filename={parent_id}.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response