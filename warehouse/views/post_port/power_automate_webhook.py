import json
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

from warehouse.models.power_automate_webhook_raw import PowerAutomateWebhookRaw
from warehouse.models.shipment import Shipment
from warehouse.utils.constants import (
    SP_CLIENT_ID,
    SP_DOC_LIB,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
)


@method_decorator(csrf_exempt, name="dispatch")
class PowerAutomateWebhook(View):
    EXPECTED_IDENTIFIER = "zem-power-automate-ltl-bol-label"

    def get(self, request: HttpRequest) -> Any:
        return HttpResponse("GET request received")

    def post(self, request: HttpRequest) -> Any:
        # Validate X-Identifier header
        identifier = request.headers.get("X-Identifier")
        if identifier != self.EXPECTED_IDENTIFIER:
            return HttpResponse(
                f"Invalid or missing X-Identifier header. Expected: {self.EXPECTED_IDENTIFIER}",
                status=403
            )
        
        try:
            body = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            body = request.body.decode("utf-8")
        
        # Save raw request data
        power_automate_event = PowerAutomateWebhookRaw(
            received_at=datetime.now(),
            ip_address=self._get_client_ip(request),
            header=dict[Any, Any](request.headers),
            body=body,
            payload=request.POST.dict(),
        )
        power_automate_event.save()
        
        # Process files from SharePoint
        try:
            response = self._process_sharepoint_files(body)
            # Log response to database
            power_automate_event.response = {
                "content": response.content.decode("utf-8") if response.content else "",
                "status_code": response.status_code,
            }
            power_automate_event.save()
        except Exception as e:
            # Log error response (500 for unexpected errors)
            error_response = HttpResponse(f"Request received but processing error: {str(e)}", status=500)
            power_automate_event.response = {
                "content": error_response.content.decode("utf-8") if error_response.content else "",
                "status_code": error_response.status_code,
                "error": str(e),
            }
            power_automate_event.save()
            return error_response
        
        # return HttpResponse("POST request received", status=200)
        return response

    def _get_client_ip(self, request: HttpRequest) -> str:
        x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
        if x_forwarded_for:
            ip = x_forwarded_for.split(",")[0]
        else:
            ip = request.META.get("REMOTE_ADDR")
        return ip

    def _get_sharepoint_auth(self) -> ClientContext:
        """Authenticate with SharePoint using certificate"""
        ctx = ClientContext(SP_URL).with_client_certificate(
            SP_TENANT,
            SP_CLIENT_ID,
            SP_THUMBPRINT,
            private_key=SP_PRIVATE_KEY,
            scopes=[SP_SCOPE],
        )
        return ctx

    def _parse_filename(self, filename: str) -> dict[str, Any]:
        """
        Parse filename format: "{MM.DD}+{container_number}+{delivery_method}+{shipping_mark}+{BOL/LABEL/BOL+LABEL}+{shipment_batch_number}+{shipment_type}+..."
        Returns dict with shipment_batch_number and file_type (BOL, LABEL, or BOL+LABEL)
        """
        parts = filename.split("+")
        if len(parts) < 6:
            raise ValueError(f"Invalid filename format: {filename}")
        
        # Find the document type (BOL, LABEL, or BOL+LABEL) - usually at index 4
        shipment_batch_number = parts[-2]
        shipment_type = parts[-1].split(".")[0]
        
        # Determine file type
        file_type = None
        if "BOL+LABEL" in filename:
            file_type = "BOL+LABEL"
        elif "BOL" in filename:
            file_type = "BOL"
        elif "LABEL" in filename:
            file_type = "LABEL"
        
        return {
            "shipment_batch_number": shipment_batch_number,
            "file_type": file_type,
            "shipment_type": shipment_type,
        }

    def _get_sharepoint_file_link(self, conn: ClientContext, file_path: str, filename: str) -> str:
        """
        Get a shareable link for a file in SharePoint
        file_path: SharePoint folder path from Power Automate (e.g., "Shared Documents/General/新OP/LA仓BOL指令/2025.11月/1110BOL/")
        filename: Name of the file
        """
        # Construct full server relative URL
        # The path from Power Automate includes "Shared Documents" which is the document library
        # Remove trailing slash if present
        file_path = file_path.rstrip("/")
        
        # If SP_DOC_LIB is set and the path doesn't start with it, use SP_DOC_LIB as base
        # Otherwise, use the path as-is (assuming it's already a complete server-relative path)
        if SP_DOC_LIB and not file_path.startswith(SP_DOC_LIB):
            # Check if path starts with document library name (e.g., "Shared Documents")
            # If so, construct path using SP_DOC_LIB
            if file_path.startswith("Shared Documents"):
                # Remove "Shared Documents" prefix and use SP_DOC_LIB instead
                relative_path = file_path.replace("Shared Documents", "").lstrip("/")
                full_path = f"{SP_DOC_LIB}/{relative_path}/{filename}" if relative_path else f"{SP_DOC_LIB}/{filename}"
            else:
                # Path doesn't start with document library, append to SP_DOC_LIB
                full_path = f"{SP_DOC_LIB}/{file_path}/{filename}" if file_path else f"{SP_DOC_LIB}/{filename}"
        else:
            # Use path as-is, ensure it starts with /
            full_path = f"/{file_path}/{filename}" if file_path else f"/{filename}"
        
        # Get the file
        file = conn.web.get_file_by_server_relative_url(full_path)
        
        # Generate anonymous shareable link
        share_link = file.share_link(SharingLinkKind.AnonymousView).execute_query()
        link_url = share_link.value.to_json()["sharingLinkInfo"]["Url"]
        
        return link_url

    def _process_sharepoint_files(self, body: dict[str, Any]) -> None:
        """Process files from SharePoint and update shipments"""
        if "files" not in body:
            return HttpResponse("No files found", status=400)
        
        # Parse files JSON string
        files_str = body.get("files", "[]")
        try:
            files = json.loads(files_str) if isinstance(files_str, str) else files_str
        except json.JSONDecodeError:
            return HttpResponse(f"Invalid files JSON: {files_str}", status=400)
        
        if not isinstance(files, list):
            return HttpResponse(f"Files must be a list: {files}", status=400)
        
        if len(files) == 0:
            return HttpResponse(f"No files to process: {body}", status=400)
        
        # Connect to SharePoint
        try:
            conn = self._get_sharepoint_auth()
        except Exception as e:
            return HttpResponse(f"Error connecting to SharePoint: {str(e)}", status=500)
        
        processed_count = 0
        errors = []
        
        for file_info in files:
            if not isinstance(file_info, dict):
                errors.append(f"Invalid file info format (not a dict): {file_info}")
                continue
            
            filename = file_info.get("name")
            file_path = file_info.get("path", "")
            
            if not filename:
                errors.append(f"Missing filename in file info: {file_info}")
                continue
            
            # Parse filename to extract shipment_batch_number and file type
            try:
                parsed = self._parse_filename(filename)
                shipment_batch_number = parsed["shipment_batch_number"]
                file_type = parsed["file_type"]
                shipment_type = parsed["shipment_type"]
            except ValueError as e:
                errors.append(f"Invalid filename format: {filename} - {str(e)}")
                continue
            
            if not shipment_batch_number or not file_type:
                errors.append(f"Invalid shipment batch number or file type: {filename}")
                continue
            
            if shipment_type != "LTL":
                errors.append(f"Invalid shipment type: {shipment_type} (expected LTL)")
                continue

            # Find LTL shipment by shipment_batch_number
            try:
                shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
            except Shipment.DoesNotExist:
                errors.append(f"Shipment not found: {shipment_batch_number}")
                continue
            except Shipment.MultipleObjectsReturned:
                # If multiple found, take the first one
                shipment = Shipment.objects.filter(shipment_batch_number=shipment_batch_number).first()
            
            if not shipment:
                errors.append(f"Shipment not found: {shipment_batch_number}")
                continue
            
            # Get shareable link from SharePoint
            try:
                shareable_link = self._get_sharepoint_file_link(conn, file_path, filename)
            except Exception as e:
                errors.append(f"Error getting shareable link for {filename}: {str(e)}")
                continue
            
            # Update shipment with appropriate link
            if file_type == "BOL+LABEL" or file_type == "BOL":
                shipment.ltl_bol_link = shareable_link
            if file_type == "BOL+LABEL" or file_type == "LABEL":
                shipment.ltl_label_link = shareable_link
            
            shipment.save()
            processed_count += 1

        # Return appropriate status based on results
        if processed_count == 0 and len(errors) > 0:
            # All files failed
            error_msg = f"No files processed. Errors: {'; '.join(errors)}"
            return HttpResponse(error_msg, status=400)
        elif len(errors) > 0:
            # Some files processed, some failed
            return HttpResponse(
                f"Partially processed: {processed_count} file(s) processed, {len(errors)} error(s). Errors: {'; '.join(errors)}",
                status=207  # 207 Multi-Status for partial success
            )
        else:
            # All files processed successfully
            return HttpResponse(f"Files processed successfully: {processed_count} file(s)", status=200)