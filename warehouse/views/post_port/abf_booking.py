"""Manual ABF booking, durably guarded against duplicate external submissions."""
import json
import os
from uuid import uuid4
from datetime import datetime
from zoneinfo import ZoneInfo

from django.db import transaction
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone

from warehouse.models.multi_carrier_quote_history import MultiCarrierQuoteHistory
from warehouse.utils.abf_booking import ABFRejected, booking_mode, prepare_booking, submit_booking


def history_for_user(request, lock=False):
    queryset = MultiCarrierQuoteHistory.objects.all()
    if not request.user.is_staff:
        queryset = queryset.filter(operator_id=request.user.pk)
    if lock:
        queryset = queryset.select_for_update()
    return get_object_or_404(queryset, pk=request.POST.get("history_id"))


def booking_source(history):
    abf = history.abf_quotes or {}
    source = abf.get("booking_source")
    quotes = abf.get("data", {}).get("quotes", [])
    if history.quote_type != "LTL" or abf.get("status") != "success" or not source or not quotes:
        raise ValueError("这条记录没有可用的 ABF 报价及下单快照，请重新手动询价")
    return source, quotes[0]


def public_booking(booking):
    return {key: value for key, value in booking.items() if key not in ("request", "token", "raw_xml")}


def abf_booking_post(request):
    if not request.user.is_authenticated:
        return JsonResponse({"success": False, "message": "请先登录"}, status=403)
    try:
        history = history_for_user(request)
        source, quote = booking_source(history)
        if request.POST.get("step") == "abf_booking_preview":
            return JsonResponse({"success": True, "mode": booking_mode(), "quote": quote,
                                 "form": source["form"], "history_id": history.pk,
                                 "requester": {"name": request.user.get_full_name() or request.user.get_username(), "email": request.user.email},
                                 "booking": public_booking(history.abf_quotes.get("booking", {}))})
        if request.POST.get("step") != "abf_booking_submit":
            raise ValueError("未知下单操作")
        if request.POST.get("confirmed") != "yes":
            raise ValueError("请确认本次下单内容后提交")
        mode = booking_mode()
        if not os.environ.get("ABF_API_KEY", "").strip():
            raise ValueError("服务器未配置 ABF_API_KEY")
        # An old result may be revisited after completion: never submit again.
        existing = history.abf_quotes.get("booking", {})
        if existing.get("status") in ("success", "submitting", "unknown") or (existing.get("status") == "test_success" and mode == "test"):
            return JsonResponse({"success": True, "booking": public_booking(existing)})
        origin_zone = "America/Los_Angeles" if source["form"].get("originState") == "CA" else "America/New_York"
        today = timezone.now().astimezone(ZoneInfo(origin_zone)).date()
        if datetime.strptime(source["form"]["pickupDate"], "%Y-%m-%d").date() < today:
            raise ValueError("报价中的提货日期已过期，请重新询价后下单")
        expiration = quote.get("expirationDate")
        if expiration:
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    expiry = datetime.strptime(expiration, fmt).date()
                except ValueError:
                    continue
                if expiry < today:
                    raise ValueError("ABF 报价已过有效期，请重新询价")
                break
        fields = json.loads(request.POST.get("booking_fields") or "{}")
        params, mode = prepare_booking(source, quote.get("quoteId"), fields, history.pk)
        token = uuid4().hex
        with transaction.atomic():
            history = history_for_user(request, lock=True)
            existing = history.abf_quotes.get("booking", {})
            if existing.get("status") in ("success", "submitting", "unknown") or (existing.get("status") == "test_success" and mode == "test"):
                return JsonResponse({"success": True, "booking": public_booking(existing)})
            history.abf_quotes["booking"] = {"status": "submitting", "mode": mode, "token": token,
                                             "request": params, "started_at": timezone.now().isoformat()}
            history.save(update_fields=["abf_quotes"])
        try:
            result = submit_booking(params, mode)
            result["bol_number"] = result.get("bol_number") or params["Bol"]
            if not result.get("document"):
                result["message"] = "提货请求已确认，但未返回可安全打开的 BOL 文件链接，请联系 ABF 获取文件，勿重复下单。"
            result.update(status="test_success" if mode == "test" else "success", mode=mode)
        except ABFRejected as exc:
            result = {"status": "rejected", "mode": mode, "message": str(exc)}
        except Exception as exc:
            # Transport errors and malformed/incomplete replies cannot prove rejection.
            detail = f"{type(exc).__name__}: {exc}"
            for name in ("ABF_API_KEY", "ABF_APP_ID"):
                if os.environ.get(name):
                    detail = detail.replace(os.environ[name], "[REDACTED]")
            result = {"status": "unknown", "mode": mode,
                      "message": "请求已发出，但未能确认 ABF 是否受理。请向 ABF 核实本次 BOL 和报价编号，勿重复提交。\n原因：" + detail[:500]}
        with transaction.atomic():
            history = history_for_user(request, lock=True)
            booking = history.abf_quotes.get("booking", {})
            if booking.get("token") != token:
                raise ValueError("下单记录状态已发生变化，请刷新核实，不要重复提交")
            booking.update(result, finished_at=timezone.now().isoformat())
            history.save(update_fields=["abf_quotes"])
        return JsonResponse({"success": True, "booking": public_booking(booking)})
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        return JsonResponse({"success": False, "message": str(exc)}, status=400)
