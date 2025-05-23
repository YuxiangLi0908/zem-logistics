from typing import Any
import os
from django.core.exceptions import ValidationError

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views import View
from datetime import datetime

from django.contrib.auth.models import User
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from warehouse.forms.customer_form import CustomerForm
from warehouse.models.customer import Customer
from warehouse.models.transaction import Transaction
from django.utils import timezone
from warehouse.utils.constants import (
    APP_ENV,
    SP_DOC_LIB,
    SP_PASS,
    SP_URL,
    SP_USER,
    SYSTEM_FOLDER,
)


@method_decorator(login_required(login_url="login"), name="dispatch")
class CustomerManagement(View):
    template_main = "statistics/new_customer.html"
    template_balance = "statistics/balance_customer.html"

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        customer_name = kwargs.get("name", None)
        step = request.GET.get("step")
        if step == "update":
            return render(
                request,
                self.template_main,
                self.handle_customer_update_get(request, customer_name),
            )
        elif step == "customer_balance":
            if self._validate_user_customer_balance(request.user):
                return render(
                    request, self.template_balance, self.handle_balance_customer_get(request)
                )
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            
        else:
            return render(
                request, self.template_main, self.handle_all_customer_get(request)
            )

    def post(self, request: HttpRequest, **kwargs) -> HttpResponse:
        step = request.POST.get("step")
        customer_name = kwargs.get("name", None)
        if step == "new":
            return render(
                request, self.template_main, self.handle_new_customer_post(request)
            )
        elif step == "update_basic_info":
            self.handle_customer_update_basic_info_post(request, customer_name)
            return redirect("customer_management")
        elif step == "update_client_creds":
            self.handle_update_client_creds_post(request)
            return redirect("customer_management")
        elif step == "adjustBalance":
            template, context = self.handle_adjust_balance(request)
            return render(request, template, context)
        elif step == "transaction_history":
            template, context = self.handle_transaction_history(request)
            return render(request, template, context)

    def handle_all_customer_get(self, request: HttpRequest) -> dict[str, Any]:
        existing_customers = Customer.objects.all().order_by("zem_name")
        context = {
            "existing_customers": existing_customers,
            "customer_form": CustomerForm(),
        }
        return context
    
    def handle_customer_update_get(
        self, request: HttpRequest, customer_name: str
    ) -> dict[str, Any]:
        customer = Customer.objects.get(zem_name=customer_name)
        customer_update_form = CustomerForm(instance=customer)
        context = {
            "customer": customer,
            "customer_update_form": customer_update_form,
        }
        return context

    def handle_new_customer_post(self, request: HttpRequest) -> dict[str, Any]:
        form = CustomerForm(request.POST)
        existing_customers = Customer.objects.all()
        zem_names = [c.zem_name for c in existing_customers]
        full_names = [c.full_name for c in existing_customers]
        if form.is_valid():
            if (
                form.cleaned_data["full_name"] not in full_names
                and form.cleaned_data["zem_name"] not in zem_names
            ):
                form.save()
                context = self.handle_all_customer_get(request)
                return context
            else:
                context = self.handle_all_customer_get(request)
                context.update(
                    {
                        "customer_form": form,
                        "duplicated": True,
                    }
                )
                return context
        else:
            raise ValueError(f"{request}")

    def handle_customer_update_basic_info_post(
        self, request: HttpRequest, customer_name: str
    ) -> None:
        form = CustomerForm(request.POST)
        selected_customer = Customer.objects.get(zem_name=customer_name)
        if form.is_valid():
            selected_customer.zem_name = form.cleaned_data.get("zem_name")
            selected_customer.full_name = form.cleaned_data.get("full_name")
            selected_customer.zem_code = form.cleaned_data.get("zem_code")
            selected_customer.accounting_name = form.cleaned_data.get("accounting_name")
            selected_customer.email = form.cleaned_data.get("email")
            selected_customer.phone = form.cleaned_data.get("phone")
            selected_customer.address = form.cleaned_data.get("address")
            selected_customer.note = form.cleaned_data.get("note")
            selected_customer.save()
        else:
            raise ValueError(f"invalid customer info")

    def handle_update_client_creds_post(self, request: HttpRequest) -> None:
        customer_id = int(request.POST.get("customer_id"))
        username = request.POST.get("username")
        password = request.POST.get("password")
        customer = Customer.objects.get(id=customer_id)
        customer.username = username
        customer.set_password(password)
        customer.save()
    
    def handle_balance_customer_get(self, request: HttpRequest) -> dict[str, Any]:
        existing_customers = Customer.objects.all().order_by("zem_name")
        context = {
            "existing_customers": existing_customers,
        }
        return context
    
    def handle_adjust_balance(self, request: HttpRequest) -> tuple[Any, Any]:
        #记录元素
        transaction_type = request.POST.get("transaction_type")
        amount = float(request.POST.get("usdamount"))
        note = request.POST.get("note")
        customer_id = request.POST.get("customerId")
        customer = Customer.objects.get(id=customer_id)
        user = request.user if request.user.is_authenticated else None

        #存储图片到云盘       
        try:
            receipt_image = request.FILES.get('receipt_image')
            if receipt_image:
                valid_extensions = ['.jpg', '.png', '.jpeg']
                ext = os.path.splitext(receipt_image.name)[1].lower()
                if ext not in valid_extensions:
                    raise ValidationError("仅支持JPG/PNG格式图片")
                if receipt_image.size > 5 * 1024 * 1024:  # 5MB
                    raise ValidationError("图片大小不能超过5MB")
        except ValidationError as e:
            raise ValidationError('图片格式错误')
        if receipt_image:
            conn = self._get_sharepoint_auth()
            link = self._upload_image_to_sharepoint(
                    conn, receipt_image
                )       
        else:
            link = ''
        
        transaction = Transaction.objects.create(
            customer=customer,
            amount=amount,
            transaction_type=transaction_type,
            note=note,
            created_by=user,
            created_at= timezone.now(),
            image_link=link
        )
        if transaction_type == "recharge":
            customer.balance = (customer.balance + amount) if customer.balance is not None else amount 
        elif transaction_type == "write_off":
            customer.balance = (customer.balance - amount) if customer.balance is not None else amount 
        customer.save()

        existing_customers = Customer.objects.all().order_by("zem_name")
        context = {
            "existing_customers": existing_customers,
            "customer_form": CustomerForm(),
        }
        return self.template_balance, context
    
    def _upload_image_to_sharepoint(
        self, conn, image
    ) -> None:
        
        image_name = image.name #提取文件名
        file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/transactions/{APP_ENV}")#文档库名称，系统文件夹名称，当前环境
        #上传到SharePoint
        sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(
            f"{image_name}", image
        ).execute_query()
        #生成并获取链接
        link = (
            resp.share_link(SharingLinkKind.OrganizationView)
            .execute_query()
            .value.to_json()["sharingLinkInfo"]["Url"]
        )
        return link

    def _get_sharepoint_auth(self) -> ClientContext:
        return ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))
    
    def handle_transaction_history(self, request: HttpRequest) -> tuple[Any, Any]:
        customer_id = request.POST.get("customerId")
        customer = Customer.objects.get(id=customer_id)
        transaction_history = Transaction.objects.filter(customer=customer)
        
        existing_customers = Customer.objects.all().order_by("zem_name")
        context = {
            "existing_customers": existing_customers,
            "customer_form": CustomerForm(),
            "transaction_history":transaction_history,
        }

        return self.template_balance, context
    
    def _validate_user_customer_balance(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="customer_balance").exists():
            return True
        else:
            return False
    