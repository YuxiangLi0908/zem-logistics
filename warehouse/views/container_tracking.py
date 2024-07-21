from typing import Any
from django.views import View
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator


@method_decorator(login_required(login_url='login'), name='dispatch')
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

    def _send_tracking_request(self):
        pass        