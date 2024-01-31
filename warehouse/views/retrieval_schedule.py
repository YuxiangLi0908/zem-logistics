import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

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
        step = request.POST.get("step")
        if step == "schedule":
            self.handle_schedule_post(request)
        elif step == "confirmation":
            self.handle_confirmation_post(request)
        else:
            raise ValueError(f"unknown request step: {step}")
        return self.get(request)
    
    def handle_schedule_post(self, request: HttpRequest) -> None:
        order_id = request.POST.get('order_id')
        target_retrieval_timestamp = request.POST.get("target_retrieval_timestamp")
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn)
        order = Order.objects.get(order_id=order_id)
        retrieval = order.retrieval_id
        retrieval.scheduled_at = current_time_cn
        retrieval.target_retrieval_timestamp = target_retrieval_timestamp
        retrieval.save()

    def handle_confirmation_post(self, request: HttpRequest) -> None:
        order_id = request.POST.get('order_id')
        actual_retrieval_timestamp = request.POST.get("actual_retrieval_timestamp")
        order = Order.objects.get(order_id=order_id)
        retrieval = order.retrieval_id
        retrieval.actual_retrieval_timestamp = actual_retrieval_timestamp
        retrieval.save()
    
    def _get_retrieval_not_scheduled(self) -> Order:
        return Order.objects.filter(
            models.Q(retrieval_id__retrive_by_zem=True) &
            models.Q(retrieval_id__scheduled_at__isnull=True)
        ).order_by("eta")
    
    def _get_retrieval_scheduled(self) -> Order:
        return Order.objects.filter(
            models.Q(retrieval_id__retrive_by_zem=True) &
            models.Q(retrieval_id__scheduled_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
        ).order_by("retrieval_id__target_retrieval_timestamp")
    
    def _set_context(self) -> None:
        self.context["retrieval_not_scheduled"] = self.retrieval_not_scheduled
        self.context["retrieval_scheduled"] = self.retrieval_scheduled
        self.context["retrieval_form"] = self.retrieval_form