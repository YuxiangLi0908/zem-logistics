from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.forms import formset_factory
from .forms import ContainerForm, PackingListForm
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

# class CreateOrder()
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
                    pl.cleaned_data["container_id"] = Container.objects.get(container_id=container_form.cleaned_data["container_id"])
                    PackingList.objects.create(**pl.cleaned_data)       
            return redirect("home")
        # else:
        #     return redirect("login")
    return render(request, 'create_order_step_1.html', context)