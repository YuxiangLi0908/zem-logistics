from asgiref.sync import sync_to_async
from datetime import datetime, timedelta
from typing import Any
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.utils.decorators import method_decorator
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View


@method_decorator(login_required(login_url='login'), name='timeout_warning')
class TimeoutWarning(View):
    template_shipment = "post_port/timeout_inventory/timeout_pallet.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA":"LA"}
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761":"LA-91761",
    }

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.GET.get("step")
        if step == "summary_table":
            template, context = await self.handle_summary_table_get(request)
            return render(request, template, context)
        else:
            context = {"warehouse_options": self.warehouse_options}
            return render(request, self.template_shipment, context)
        
    async def post(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "summary_warehouse":
            template, context = await self.handle_summary_warehouse_post(request)
            return render(request, template, context)
    

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
    
    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name="shipmnet_leader").exists():
            return True
        else:
            return False