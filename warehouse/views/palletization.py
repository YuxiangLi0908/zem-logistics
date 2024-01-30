import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.offload import Offload
from warehouse.models.order import Order

@method_decorator(login_required(login_url='login'), name='dispatch')
class Palletization(View):
    template_main = 'palletization.html'
    context: dict[str, Any] = {}

    def get(self, request: HttpRequest) -> HttpResponse:
        return render(request, self.template_main, self.context)
    
    def _get_order_not_palletized(self) -> Order:
        return Order.objects.filter(
            models.Q(retrieval_id__retrive_by_zem=True) &
            models.Q(retrieval_id__scheduled_at__isnull=True)
        )