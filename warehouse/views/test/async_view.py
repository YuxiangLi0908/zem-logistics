from django.http import HttpRequest, JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from asgiref.sync import sync_to_async
from django.views import View
from django.utils.decorators import async_only_middleware, classonlymethod
from django.shortcuts import render
import asyncio
import time

class AsyncView(View):
    template = "test/async_view.html"

    # @classonlymethod
    # def as_view(cls, **initkwargs):
    #     view = super().as_view(**initkwargs)
    #     view._is_coroutine = asyncio.coroutines._is_coroutine
    #     return view

    async def get(self, request, *args, **kwargs):
        """Async post"""
        time.sleep(6)
        context = await self.async_get_data()
        return await sync_to_async(render)(request, self.template, context)
    
    async def async_get_data(self):
        # Simulate an async data retrieval process (e.g., database call)
        asyncio.sleep(5)
        return {"message": "Async GET request successful"}
    