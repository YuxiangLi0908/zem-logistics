from django.http import HttpResponse
from django.urls import path

urlpatterns = [path("post_nsop/", lambda request: HttpResponse(), name="post_nsop")]
