import datetime

from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.forms import formset_factory
from .forms import ContainerForm, PackingListForm, UpdatePickupForm, UpdatePalletizationForm
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
        if container_form.is_valid():
            container_form.save()
            for pl in packing_list_form:
                if pl.is_valid():
                    for k in pl.cleaned_data.keys():
                        if isinstance(pl.cleaned_data[k], str):
                            pl.cleaned_data[k] = pl.cleaned_data[k].strip()
                        if k == "destination":
                            pl.cleaned_data[k] = pl.cleaned_data[k].upper()
                    pl.cleaned_data["container_number"] = Container.objects.get(container_number=container_form.cleaned_data["container_number"])
                    PackingList.objects.create(**pl.cleaned_data)
                else:
                    raise ValueError(f"{pl.is_valid()}")                    
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
        return render(request, 'palletization.html', context)
    else:
        forms = []
        for pl in packing_lists:
            forms.append(UpdatePalletizationForm(instance=pl))
        data = zip(forms, packing_lists)
    context = {
        "data": data,
    }
    return render(request, 'palletization_detail.html', context)
