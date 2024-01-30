import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.forms import formset_factory
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.forms.retrieval_form import RetrievalForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class ScheduleRetrieval(View):
    template_main = 'schedule_pickup.html'
    context: dict[str, Any] = {}

    def get(self, request: HttpRequest) -> HttpResponse:
        self.retrieval_not_scheduled = self._get_retrieval_not_scheduled()
        self.retrieval_scheduled =self._get_retrieval_scheduled()
        self.retrieval_form = RetrievalForm()
        self._set_context()
        return render(request, self.template_main, self.context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        # raise ValueError(f"{request.POST}")
        order_id = request.POST.get('order_id')
        target_retrieval_timestamp = request.POST.get("target_retrieval_timestamp")
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn)
        order = Order.objects.get(order_id=order_id)
        retrieval = order.retrieval_id
        retrieval.scheduled_at = current_time_cn
        retrieval.target_retrieval_timestamp = target_retrieval_timestamp
        retrieval.save()
        return self.get(request)
    
    def _get_retrieval_not_scheduled(self) -> Order:
        return Order.objects.filter(
            models.Q(retrieval_id__retrive_by_zem=True) &
            models.Q(retrieval_id__scheduled_at__isnull=True)
        )
    
    def _get_retrieval_scheduled(self) -> Order:
        return Order.objects.filter(
            models.Q(retrieval_id__retrive_by_zem=True) &
            models.Q(retrieval_id__scheduled_at__isnull=False)
        )
    
    def _set_context(self) -> None:
        self.context["retrieval_not_scheduled"] = self.retrieval_not_scheduled
        self.context["retrieval_scheduled"] = self.retrieval_scheduled
        self.context["retrieval_form"] = self.retrieval_form