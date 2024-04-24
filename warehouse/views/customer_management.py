from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator

from warehouse.models.customer import Customer
from warehouse.forms.customer_form import CustomerForm


@method_decorator(login_required(login_url='login'), name='dispatch')
class CustomerManagement(View):
    template_main = "new_customer.html"

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        customer_name = kwargs.get("name", None)
        step = request.GET.get("step")
        if step =="update":
            return render(request, self.template_main, self.handle_customer_update_get(request, customer_name))
        else:
            return render(request, self.template_main, self.handle_all_customer_get(request))
    
    def post(self, request:HttpRequest, **kwargs) -> HttpResponse:
        step = request.POST.get("step")
        customer_name = kwargs.get("name", None)
        if step == "new":
            return render(request, self.template_main, self.handle_new_customer_post(request))
        elif step == "update":
            self.handle_customer_update_post(request, customer_name)
            return redirect("customer_management")
        
    def handle_all_customer_get(self, request: HttpRequest) -> dict[str, Any]:
        existing_customers = Customer.objects.all().order_by("zem_name")
        context = {
            "existing_customers": existing_customers,
            "customer_form": CustomerForm(),
        }
        return context
    
    def handle_customer_update_get(self, request: HttpRequest, customer_name: str) -> dict[str, Any]:
        customer = Customer.objects.get(zem_name=customer_name)
        customer_update_form = CustomerForm(instance=customer)
        context = {
            "customer_update_form": customer_update_form,
        }
        return context
    
    def handle_new_customer_post(self, request: HttpRequest) -> dict[str, Any]:
        form = CustomerForm(request.POST)
        existing_customers = Customer.objects.all()
        zem_names = [c.zem_name for c in existing_customers]
        full_names = [c.full_name for c in existing_customers]
        if form.is_valid():
            if form.cleaned_data["full_name"] not in full_names and form.cleaned_data["zem_name"] not in zem_names:
                form.save()
                context = self.handle_all_customer_get(request)
                return context
            else:
                context = self.handle_all_customer_get(request)
                context.update({
                    "customer_form": form,
                    "duplicated": True,
                })
                return context
        else:
            raise ValueError(f"{request}")
        
    def handle_customer_update_post(self, request: HttpRequest, customer_name: str) -> None:
        form = CustomerForm(request.POST)
        selected_customer = Customer.objects.get(zem_name=customer_name)
        if form.is_valid():
            selected_customer.zem_name = form.cleaned_data.get("zem_name")
            selected_customer.full_name = form.cleaned_data.get("full_name")
            selected_customer.zem_code = form.cleaned_data.get("zem_code")
            selected_customer.email = form.cleaned_data.get("email")
            selected_customer.phone = form.cleaned_data.get("phone")
            selected_customer.note = form.cleaned_data.get("note")
            selected_customer.save()
        else:
            raise ValueError(f"invalid customer info")
