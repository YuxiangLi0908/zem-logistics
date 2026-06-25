from django.http import HttpRequest, HttpResponse
from django.views import View


class Dropshipping(View):
    template_create = ""
    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "warehouse":
            pass

    def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        step = request.POST.get("step")
        if step == "warehouse":
            pass