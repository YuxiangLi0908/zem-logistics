from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.template.loader import get_template
from xhtml2pdf import pisa
from typing import Any

@method_decorator(login_required(login_url='login'), name='dispatch')
class ExportFile(View):
    template_main = {
        "DO": "export_file/do.html",
        "PL": "export_file/packing_list.html"
    }
    file_name = {
        "DO": "D/O",
        "PL": "拆柜单"
    }

    def get(self, request: HttpRequest) -> HttpResponse:
        name = request.GET.get("name")
        template_path = self.template_main[name]
        template = get_template(template_path)
        context = {'sample_data': 'Hello, this is some sample data!'}
        html = template.render(context)

        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="{self.file_name[name]}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response

def export_bol(context: dict[str, Any]):
    template_path = "export_file/bol_template.html"
    template = get_template(template_path)
    html = template.render(context)
    response = HttpResponse(content_type="application/pdf")
    response['Content-Disposition'] = f'attachment; filename="BOL_{context["batch_number"]}.pdf"'
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    return response

# def generate_pdf(request):
#     # Your HTML template file path
#     template_path = 'export_file/do.html'
#     # raise ValueError(f"{os.listdir(template_path)}")
    
#     # Load the HTML template
#     try:
#         template = get_template(template_path)
#     except:
#         raise ValueError("no template")
    
#     # Context data to be passed to the template
#     context = {'sample_data': 'Hello, this is some sample data!'}
    
#     # Render the template with the context data
#     html = template.render(context)
    
#     # Create a PDF file
#     response = HttpResponse(content_type='application/pdf')
#     response['Content-Disposition'] = 'attachment; filename="output.pdf"'
    
#     # Generate PDF using xhtml2pdf
#     pisa_status = pisa.CreatePDF(html, dest=response)
    
#     # Check if PDF generation was successful
#     if pisa_status.err:
#         raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    
#     return response