from asgiref.sync import sync_to_async
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.views import View
from django.shortcuts import redirect, render
from typing import Any, Tuple

from warehouse.models.fleet import Fleet


class WarehouseOperations(View):
    template_warehousing_operation = "warehouse/templates/post_port/warehouse_operations/01_warehousing_operation.html"
    template_upcoming_fleet = "post_port/warehouse_operations/03_upcoming_fleet.html"

    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        pk = kwargs.get("pk", None)
        step = request.GET.get("step", None)
        if step == "upcoming_fleet":
            template, context = await self.handle_upcoming_fleet_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "upcoming_fleet_warehouse":
            template, context = await self.handle_upcoming_fleet_post(request)
            return await sync_to_async(render)(request, template, context)
        
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
    
    async def handle_upcoming_fleet_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_upcoming_fleet, context
    
    async def handle_upcoming_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        fleets = Fleet.objects.filter()
        context = {"warehouse_options": self.warehouse_options}
        return self.template_upcoming_fleet, context