"""ABF tracking and document queries, independent of booking/test mode."""
from django.db import transaction
from django.http import JsonResponse
from django.utils import timezone

from warehouse.utils.abf_followup import DOCUMENT_TYPES, REFERENCE_TYPES, ABFQueryError, track_shipment, retrieve_documents
from warehouse.views.post_port.abf_booking import history_for_user
from warehouse.utils.abf_booking import document_url


def saved_references(history):
    if history is None:
        return []
    abf = history.abf_quotes or {}
    booking = abf.get("booking", {})
    references = []
    # Test confirmations/PROs must not be advertised as real shipment references.
    if booking.get("status") != "test_success" and booking.get("mode") != "test":
        for key, kind in (("pro_number", "A"), ("confirmation", "K"), ("bol_number", "B")):
            value = str(booking.get(key) or (booking.get("request", {}).get("Bol") if key == "bol_number" else "") or "").strip()
            if value and value != "WWW0000000":
                references.append({"type": kind, "number": value})
    return references


def abf_followup_post(request):
    if not request.user.is_authenticated:
        return JsonResponse({"success": False, "message": "请先登录"}, status=403)
    try:
        history = history_for_user(request) if request.POST.get("history_id") else None
    except (ValueError, TypeError):
        return JsonResponse({"success": False, "message": "无效的询价记录编号"}, status=400)
    step = request.POST.get("step")
    abf = (history.abf_quotes or {}) if history else {}
    if step == "abf_shipment_preview":
        booking = abf.get("booking", {})
        generated_documents = []
        for key, label in (("document", "BOL"), ("label_document", "货物标签"), ("pro_label_document", "PRO标签")):
            url = document_url(booking.get(key))
            if url:
                generated_documents.append({"label": label, "url": url})
        return JsonResponse({"success": True, "history_id": history.pk if history else None,
                             "references": saved_references(history), "booking_status": abf.get("booking", {}).get("status", ""),
                             "reference_types": REFERENCE_TYPES, "document_types": DOCUMENT_TYPES,
                             "generated_documents": generated_documents,
                             "tracking": abf.get("tracking_latest"), "documents": abf.get("documents_latest")})
    try:
        ref_type = str(request.POST.get("ref_type") or "").strip().upper()
        ref_num = str(request.POST.get("ref_num") or "").strip()
        if step == "abf_tracking":
            result = track_shipment(ref_type, ref_num)
            key = "tracking_latest"
        elif step == "abf_documents":
            doc_type = request.POST.get("doc_type") or "ALL"
            image_type = request.POST.get("image_type") or "L"
            result = retrieve_documents(ref_type, ref_num, doc_type, image_type)
            result.update(doc_type=doc_type, image_type=image_type)
            key = "documents_latest"
        else:
            raise ValueError("未知 ABF 查询操作")
        result.update(ref_type=ref_type, ref_num=ref_num, queried_at=timezone.now().isoformat())
        if history:
            # Network I/O is outside the transaction. Merge only the queried key;
            # do not overwrite a concurrent booking or the other query's result.
            with transaction.atomic():
                locked = history_for_user(request, lock=True)
                data = dict(locked.abf_quotes or {})
                data[key] = result
                locked.abf_quotes = data
                locked.save(update_fields=["abf_quotes"])
        return JsonResponse({"success": True, "data": result})
    except ABFQueryError as exc:
        codes = {item["code"] for item in exc.errors}
        hint = ""
        if step == "abf_tracking" and "9" in codes:
            hint = "此提货确认号尚未开单，暂未关联 PRO号，可稍后再查，无需重新下单。"
        elif step == "abf_documents" and codes & {"6", "8", "9", "11", "12"}:
            hint = "文件可能尚未生成、未扫描，或货物尚未送达，请核对类型并稍后再查。"
        elif step == "abf_documents" and codes & {"7", "20", "50"}:
            hint = "请核对 API 账号的文件访问权限、付款方资格或接口开通状态。"
        return JsonResponse({"success": False, "message": str(exc) + ("\n" + hint if hint else ""), "errors": exc.errors}, status=400)
    except (ValueError, TypeError, KeyError) as exc:
        return JsonResponse({"success": False, "message": str(exc)}, status=400)
    except ConnectionError as exc:
        return JsonResponse({"success": False, "message": str(exc)}, status=502)
