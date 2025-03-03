from typing import Any

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View

from warehouse.views.terminal49_webhook import T49Webhook


@method_decorator(login_required(login_url="login"), name="dispatch")
class ContainerTracking(View):
    t49_tracking_url = "https://api.terminal49.com/v2/tracking_requests"
    template_main = "container_tracking/main_page.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "":
            pass
        else:
            return render(request, self.template_main)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "tracking_request":
            pass
        else:
            return T49Webhook().post(request)

    def _send_tracking_request(self):
        pass
