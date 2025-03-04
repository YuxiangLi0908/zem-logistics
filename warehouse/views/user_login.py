from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group, User
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render


def user_login(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect("home")
        else:
            messages.error(request, "Invalid login credentials.")

    return render(request, "user_login.html")


def user_logout(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("login")


@login_required(login_url="login")
def home(request: HttpRequest) -> HttpResponse:
    return render(request, "home.html")
