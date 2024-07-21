import json
from typing import Any
from datetime import datetime

from django.views import View
from django.http import HttpRequest, HttpResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from warehouse.models.terminal49_webhook_raw import T49Raw

@method_decorator(csrf_exempt, name='dispatch')
class T49Webhook(View):
    def get(self, request: HttpRequest) -> Any:
        return HttpResponse("GET request received")
    
    def post(self, request: HttpRequest) -> Any:
        try:
            body = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            body = request.body.decode('utf-8') 
        t49_event = T49Raw(
            received_at=datetime.now(),
            ip_address=self._get_client_ip(request),
            header=dict(request.headers),
            body=body,
            payload=request.POST.dict(),
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