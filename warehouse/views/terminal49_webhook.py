from typing import Any
from datetime import datetime

from django.views import View
from django.http import HttpRequest, HttpResponse

from warehouse.models.terminal49_webhook_raw import T49Raw


class T49Webhook(View):
    def get(self, request: HttpRequest) -> Any:
        return HttpResponse("GET request received")
    
    def post(self, request: HttpRequest) -> Any:
        t49_event = T49Raw(
            received_at=datetime.now(),
            ip_address=self._get_client_ip(request),
            header=request.headers,
            body=request.body,
            payload=request.POST,
        )
        t49_event.save()
        return HttpResponse("POST request received")

    def _get_client_ip(self, request: HttpRequest) -> str:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip