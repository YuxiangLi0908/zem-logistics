from asgiref.sync import sync_to_async
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.views import View


class Dropshipping(View):
    template_create = ""
    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "container_info_supplement":
            template, context = await self.handle_order_supplemental_info_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        step = request.POST.get("step")
        if step == "warehouse":
            pass