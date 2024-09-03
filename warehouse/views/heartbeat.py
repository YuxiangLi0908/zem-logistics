from django.http import HttpRequest, HttpResponse

def get_heartbeat(request: HttpRequest) -> HttpResponse:
    data = {
        "is_alive": True,
    }
    return HttpResponse(data, status=200)