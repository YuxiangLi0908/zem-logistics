import asyncio
import time

from asgiref.sync import sync_to_async
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils.decorators import (
    async_only_middleware,
    classonlymethod,
    method_decorator,
)
from django.views import View


class AsyncView(View):
    template = "test/async_view.html"

    # @classonlymethod
    # def as_view(cls, **initkwargs):
    #     view = super().as_view(**initkwargs)
    #     view._is_coroutine = asyncio.coroutines._is_coroutine
    #     return view

    async def get(self, request, *args, **kwargs):
        """Async post"""
        step = request.GET.get("step")
        time.sleep(6)
        context = await self.async_get_data(step)
        return await sync_to_async(render)(request, self.template, context)

    async def async_get_data(self, step):
        # Simulate an async data retrieval process (e.g., database call)
        asyncio.sleep(5)
        return {"message": f"{step} call"}
