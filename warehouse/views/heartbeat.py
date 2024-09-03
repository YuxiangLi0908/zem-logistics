from django.http import JsonResponse

def get_heartbeat(request):
    data = {
        'status': 'healthy'
    }
    return JsonResponse(data)