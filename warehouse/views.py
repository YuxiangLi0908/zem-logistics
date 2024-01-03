import datetime
import uuid
from typing import Any

from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Sum, F
from django.forms import formset_factory
from django.http import HttpRequest, HttpResponse
from django.utils.decorators import method_decorator
from django.views import View
from .forms import (
    ContainerForm,
    PackingListForm,
    UpdatePickupForm,
    UpdatePalletizationForm,
    WarehouseSelectForm,
    ShipmentForm,
)
from .models import *

def user_login(request: HttpRequest) -> HttpResponse:
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')

        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('home')
        else:
            messages.error(request, 'Invalid login credentials.')

    return render(request, 'user_login.html')

def user_logout(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect('login')

@login_required(login_url='login') 
def home(request: HttpRequest) -> HttpResponse:
    return render(request, 'home.html')

@login_required(login_url='login') 
def create_order(request: HttpRequest) -> HttpResponse:
    container_form = ContainerForm()
    packing_list_formsets = formset_factory(PackingListForm, extra=1)
    context = {
        "container_form": container_form,
        "packing_list_form": packing_list_formsets(),
    }
    if request.method == "POST":
        container_form = ContainerForm(request.POST)
        packing_list_form = packing_list_formsets(request.POST)
        pl_valid = all([pl.is_valid() for pl in packing_list_form])
        if container_form.is_valid() and pl_valid:
            container_form.save()
            for pl in packing_list_form:
                for k in pl.cleaned_data.keys():
                    if isinstance(pl.cleaned_data[k], str):
                        pl.cleaned_data[k] = pl.cleaned_data[k].strip()
                    if k == "destination":
                        pl.cleaned_data[k] = pl.cleaned_data[k].upper()
                pl.cleaned_data["container_number"] = Container.objects.get(container_number=container_form.cleaned_data["container_number"])
                PackingList.objects.create(**pl.cleaned_data)
            return redirect("home")
    return render(request, 'create_order.html', context)

@login_required(login_url='login') 
def schedule_pickup(request: HttpRequest) -> HttpResponse:
    containers_unpicked= Container.objects.filter(pickup_scheduled_at__isnull=True).order_by('eta')
    containers_picked = Container.objects.filter(pickup_scheduled_at__isnull=False, palletized_at__isnull=True).order_by('eta')
    if request.method == "POST":
        form = UpdatePickupForm(request.POST)
        if form.is_valid():
            container_number = request.POST.get('record_id')
            appointment = form.cleaned_data['pickup_at']
            current_time = datetime.datetime.now()
            Container.objects.filter(container_number=container_number).update(pickup_appointment=appointment, pickup_scheduled_at=current_time)
        containers_unpicked= Container.objects.filter(pickup_scheduled_at__isnull=True).order_by('eta')
        containers_picked = Container.objects.filter(pickup_scheduled_at__isnull=False, palletized_at__isnull=True).order_by('eta')
        context = {
            'containers_unpicked': containers_unpicked,
            'containers_picked': containers_picked,
            'form': form,
        }
        return render(request, 'schedule_pickup.html', context)
    else:
        form = UpdatePickupForm()
    context = {
        'containers_unpicked': containers_unpicked,
        'containers_picked': containers_picked,
        'form': form,
    }
    return render(request, 'schedule_pickup.html', context)

@login_required(login_url='login') 
def palletize(request: HttpRequest) -> HttpResponse:
    containers_unpalletized = Container.objects.filter(
        pickup_appointment__isnull=False, palletized_at__isnull=True
    )
    containers_palletized = Container.objects.filter(
        pickup_appointment__isnull=False, palletized_at__isnull=False
    )
    context = {
        "containers_unpalletized": containers_unpalletized,
        "containers_palletized": containers_palletized
    }
    return render(request, 'palletization.html', context)

@login_required(login_url='login') 
def packling_list(request: HttpRequest, pk: int) -> HttpResponse:
    packing_lists = PackingList.objects.select_related('container_number').filter(
        container_number__id=pk
    )
    if request.method == "POST":
        Container.objects.filter(pk=pk).update(palletized_at=datetime.datetime.now())
        for pl, n in zip(packing_lists, request.POST.getlist("n_pallet")):
            pl.n_pallet = n
            pl.save()
        containers = Container.objects.filter(
            pickup_appointment__isnull=False, palletized_at__isnull=True
        )
        context = {"containers": containers}
        return palletize(request)
    else:
        forms = []
        for pl in packing_lists:
            forms.append(UpdatePalletizationForm(instance=pl))
        data = zip(forms, packing_lists)
    context = {
        "data": data,
    }
    return render(request, 'palletization_detail.html', context)


@method_decorator(login_required(login_url='login'), name='dispatch')
class ScheduleShipment(View):
    template_main = 'schedule_shipment.html'
    warehouse_form = None
    shipment_form = None
    warehouse_data = None
    packing_list_data = None
    context = {}
    additional_context = {}

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        request_type = request.GET.get("type", None)
        if not request_type:
            self.set_warehouse_form()
        elif request_type == "destination":
            self.handle_destination_get(
                warehouse=request.GET.get("warehouse"),
                destination=request.GET.get("destination"),
            )
        else:
            raise ValueError(f"{request.GET}")
        self.set_context()
        return render(request, self.template_main, self.context)
        
    def post(self, request: HttpRequest, **kwargs) -> HttpResponse:
        request_type = request.POST.get("type", None)
        if request_type == "warehouse":
            warehouse = request.POST.get("warehouse")
            self.handle_main_post(warehouse)
        elif request_type == "appointment":
            ids = request.POST.get("ids").strip('][').split(', ')
            shipment_appointment = request.POST.getlist("shipment_appointment")
            warehouse = request.GET.get("warehouse")
            destination = request.GET.get("destination")
            self.handle_appointment_post(shipment_appointment, ids, warehouse, destination)
        else:
            raise ValueError(f"{request.POST}")
        self.set_context()
        return render(request, self.template_main, self.context)

    def handle_main_get(self, request: HttpRequest) -> None:
        self.set_warehouse_form()
    
    def handle_destination_get(self, warehouse: str, destination: str) -> None:
        warehouse_pl = PackingList.objects.filter(
            container_number__warehouse__name=warehouse,
            n_pallet__isnull=False,
            is_shipment_schduled=False,
        )
        warehouse_pl = warehouse_pl.values('destination').annotate(
            total_cbm=Sum('cbm'),
            total_n_pallet=Sum('n_pallet'),
            total_pcs=Sum('pcs'),
            total_weight_kg=Sum('total_weight_kg'),
            total_weight_lbs=Sum('total_weight_lbs')
        ).order_by('-total_n_pallet')
        packing_lists = PackingList.objects.filter(
            container_number__warehouse__name=warehouse,
            destination=destination,
            n_pallet__isnull=False,
            is_shipment_schduled=False,
        )
        shipment_form = []
        for pl in packing_lists:
            shipment_form.append(ShipmentForm(instance=pl))
        packing_lists = packing_lists.values(
            'id',
            'delivery_method',
            'shipping_mark',
            'fba_id',
            'ref_id',
            'destination',
            'pcs',
            'total_weight_kg',
            'total_weight_lbs',
            'cbm',
            'n_pallet',
            'is_shipment_schduled',
            'shipment_appointment',
            'container_number__container_number',
            customer_name=F('container_number__customer_name__name'),
        )
        ids  = [item['id'] for item in packing_lists]
        self.set_warehouse_form(initial={"warehouse": warehouse})
        self.set_warehouse_data(warehouse_pl)
        self.set_packing_list_data(packing_lists)
        self.set_shipment_form(shipment_form)
        self.set_additional_context(ids=ids)
        
    def handle_main_post(self, warehouse: str) -> None:
        warehouse_pl = PackingList.objects.filter(
            container_number__warehouse__name=warehouse,
            n_pallet__isnull=False,   
            is_shipment_schduled=False,
        )
        warehouse_pl = warehouse_pl.values('destination').annotate(
            total_cbm=Sum('cbm'),
            total_n_pallet=Sum('n_pallet'),
            total_pcs=Sum('pcs'),
            total_weight_kg=Sum('total_weight_kg'),
            total_weight_lbs=Sum('total_weight_lbs')
        ).order_by('-total_n_pallet')
        self.set_warehouse_form(initial={"warehouse": warehouse})
        self.set_warehouse_data(warehouse_pl)

    def handle_appointment_post(
        self,
        appointments: list[Any],
        ids: list[Any],
        warehouse: str,
        destination: str
    ) -> None:
        batch_id = uuid.uuid1()
        for appointment, id in zip(appointments, ids):
            pl = PackingList.objects.get(id=id)
            if appointment:
                pl.shipment_appointment = appointment
                pl.is_shipment_schduled = True
                pl.shipment_batch_number = batch_id
                pl.save()
            else:
                pl.shipment_appointment = None
                pl.is_shipment_schduled = False
                pl.shipment_batch_number = None
                pl.save()
        self.handle_destination_get(warehouse, destination)
    
    def set_warehouse_form(self, initial: dict = None) -> None:
        self.warehouse_form = WarehouseSelectForm(initial=initial)

    def set_warehouse_data(self, warehouse_data: Any = None) -> None:
        self.warehouse_data = warehouse_data
    
    def set_packing_list_data(self, packing_list_data: Any = None) -> None:
        self.packing_list_data = packing_list_data

    def set_shipment_form(self, shipment_form: list[Any]) -> None:
        self.shipment_form = shipment_form

    def set_context(self) -> None:
        self.context = {
            "warehouse_form": self.warehouse_form,
            "warehouse_data": self.warehouse_data,
            "packing_list_data": self.packing_list_data,
            "shipment_form": zip(self.shipment_form, self.packing_list_data) if self.shipment_form and self.packing_list_data else None,
        }
        if self.additional_context:
            self.context = self.context | self.additional_context

    def set_additional_context(self, **kwargs) -> None:
        self.additional_context = self.additional_context | kwargs
