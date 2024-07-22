import string
import random

import pandas as pd
from typing import Any
from datetime import datetime, timedelta

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.forms import formset_factory

from warehouse.models.quote import Quote
from warehouse.forms.quote_form import QuoteForm


@method_decorator(login_required(login_url='login'), name='dispatch')
class QuoteManagement(View):
    template_create = "quote/quote_creation.html"
    template_update = "quote/quote_list.html"
    template_edit = "quote/quote_edit.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "new":
            return render(request, self.template_create, self.handle_new_quote_get(request))
        elif step == "history":
            return render(request, self.template_update, self.handle_history_quote_get(request))
        elif step == "edit":
            return render(request, self.template_edit, self.handle_edit_get(request))
        context = {}
        return render(request, self.template_create, context)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "create":
            return render(request, self.template_create, self.handle_create_post(request))
        elif step == "export_single_quote_excel":
            return self.handle_single_excel_export(request)
        elif step == "search":
            return render(request, self.template_update, self.handle_quote_search_post(request))
        elif step == "update":
            return render(request, self.template_update, self.handle_update_post(request))

    def handle_new_quote_get(self, request: HttpRequest) -> dict[str, Any]:
        quote_form = QuoteForm()
        quote_formset = formset_factory(QuoteForm, extra=1)
        context = {
            "quote_form": quote_form,
            "quote_formset": quote_formset,
            "step": "create",
        }
        return context
    
    def handle_history_quote_get(self, request: HttpRequest) -> dict[str, Any]:
        current_date = datetime.now().date()
        start_date = current_date + timedelta(days=-30)
        end_date = current_date
        context = {
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
        }
        return context
    
    def handle_edit_get(self, request: HttpRequest) -> dict[str, Any]:
        quote_id = request.GET.get("qid")
        quote = Quote.objects.select_related("warehouse", "customer_name").get(quote_id=quote_id)
        quote_form = QuoteForm(instance=quote)
        context = {
            "quote": quote,
            "quote_form": quote_form,
        }
        return context
    
    def handle_create_post(self, request: HttpRequest) -> dict[str, Any]:
        quote_form_p1 = QuoteForm(request.POST)
        timestamp = datetime.now()
        if quote_form_p1.is_valid():
            cleaned_data_p1 = quote_form_p1.cleaned_data
            try:
                cleaned_data_p1["note"] = cleaned_data_p1["note"].strip()
            except:
                pass
            try:
                cleaned_data_p1["zipcode"] = cleaned_data_p1["zipcode"].strip().upper()
            except:
                pass
            try:
                cleaned_data_p1["address"] = cleaned_data_p1["address"].strip()
            except:
                pass
            customer = cleaned_data_p1.get('customer_name')
            customer_id = customer.id if customer else 999
            id_prefix = f"QT{timestamp.strftime('%Y%m%d')}{''.join(random.choices(string.ascii_uppercase, k=2))}{customer_id}"
            parent_id = f"{id_prefix}{timestamp.strftime('%H%M')}"
            quote_formset = formset_factory(QuoteForm, extra=1)
            quote_form = quote_formset(request.POST)
            q_valid = all([q.is_valid() for q in quote_form])
            if q_valid:
                cleaned_data_p2 = [q.cleaned_data for q in quote_form]
                i = 1
                quote_data = []
                for d in cleaned_data_p2:
                    data = {}
                    quote_id = f"{parent_id}{''.join(random.choices(string.ascii_uppercase, k=2))}{i}"
                    data.update(cleaned_data_p1)
                    data["created_at"] = timestamp.date()
                    data["parent_id"] = parent_id
                    data["quote_id"] = quote_id
                    data["warehouse"] = d["warehouse"]
                    data["load_type"] = d["load_type"]
                    data["is_lift_gate"] = d["is_lift_gate"]
                    data["cost"] = d["cost"]
                    data["price"] = d["price"]
                    data["distance_mile"] = d["distance_mile"]
                    try:
                        data["comment"] = d["comment"].strip()
                    except:
                        data["comment"] = d["comment"]
                    quote_data.append(data)
                    i += 1
                all_quotes = Quote.objects.bulk_create([
                    Quote(**d) for d in quote_data
                ])
            else:
                raise RuntimeError(f"invalid 报价!")
        else:
            raise RuntimeError(f"invalid 询价信息!")
        context = {}
        context["parent_id"] = parent_id
        context["all_quotes"] = all_quotes
        context["step"] = "review"
        return context
    
    def handle_quote_search_post(self, request: HttpRequest) -> dict[str, Any]:
        start_date = request.POST.get("start_date", None)
        end_date = request.POST.get("end_date", None)
        if start_date and end_date:
            criteria = models.Q(created_at__gte=start_date) & models.Q(created_at__lte=end_date)
        elif start_date:
            criteria = models.Q(created_at__gte=start_date)
        elif end_date:
            criteria = models.Q(created_at__lte=end_date)
        else:
            default_date = datetime.now().date() + timedelta(days=-30)
            criteria = models.Q(created_at__gte=default_date)
        quote = Quote.objects.select_related("warehouse", "customer_name").filter(criteria)
        context = {
            "start_date": start_date,
            "end_date": end_date,
            "quote": quote
        }
        return context
    
    def handle_single_excel_export(self, request: HttpRequest) -> HttpResponse:
        parent_id = request.POST.get("parent_id")
        quote = Quote.objects.select_related("warehouse", "customer_name").filter(
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
    
    def handle_update_post(self, request: HttpRequest) -> dict[str, Any]:
        quote_id = request.POST.get("quote_id")
        quote = Quote.objects.select_related("warehouse", "customer_name").get(quote_id=quote_id)
        quote_form = QuoteForm(request.POST)
        if quote_form.is_valid():
            data = quote_form.cleaned_data
            for k, v in data.items():
                if v:
                    setattr(quote, k, v)
            quote.save()
        else:
            raise ValueError(f"invalid quote data: {quote_form}")
        context = self.handle_history_quote_get(request)
        mutable_post = request.POST.copy()
        mutable_post.update(context)
        request.POST = mutable_post
        return self.handle_quote_search_post(request)