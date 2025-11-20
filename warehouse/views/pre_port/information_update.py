from typing import Any, Coroutine

from asgiref.sync import sync_to_async
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect
from django.shortcuts import redirect, render
from django.views import View


class InformationUpdate(View):
    template_main = "pre_port/information_update/01_information_update_all.html"

    async def get(self, request: HttpRequest) -> Any | None:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.information_update_all()
            return render(request, template, context)

    async def post(self, request: HttpRequest) -> Any | None:
        pass

    async def _user_authenticate(self, request: HttpRequest) -> bool:
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def information_update_all(self):
        return self.template_main, {}