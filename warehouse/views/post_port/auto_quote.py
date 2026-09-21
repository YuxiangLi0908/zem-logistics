import csv
import json
from datetime import timedelta
from zipfile import BadZipFile

from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db import IntegrityError
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from openpyxl.utils.exceptions import InvalidFileException

from warehouse.models.auto_quote import AutoQuoteAddress, AutoQuoteBatch, AutoQuoteWorkerState
from warehouse.models.system_parameter import SystemParameter
from warehouse.utils.auto_quote import (
    batch_summary, check_group, create_batch, import_addresses, read_addresses, stop_batch,
)
from warehouse.utils.multi_carrier_quote import gateway_config
from warehouse.utils.quote_analysis import analysis_options, build_analysis

def visible_batches(user):
    queryset = AutoQuoteBatch.objects.all()
    return queryset if user.is_staff else queryset.filter(operator=user)


def auto_quote_get(request):
    if not request.user.is_authenticated:
        raise PermissionDenied
    if request.GET.get("step") == "auto_quote_history":
        return render(request, "post_port/new_sop/leader_check/auto_quote_history.html")
    if request.GET.get("step") == "auto_quote_analysis":
        return render(request, "post_port/new_sop/leader_check/auto_quote_analysis.html")
    try:
        kind = request.GET.get("kind", "batches")
        if kind == "analysis_options":
            return JsonResponse({"success": True, **analysis_options(visible_batches(request.user))})
        if kind == "analysis":
            return JsonResponse({"success": True, **build_analysis(visible_batches(request.user), request.GET)})
        if kind == "analysis_export":
            report = build_analysis(visible_batches(request.user), request.GET, export=True)
            response = HttpResponse(content_type="text/csv; charset=utf-8")
            response["Content-Disposition"] = f'attachment; filename="price-analysis-{report["profile"]["code"]}.csv"'
            response.write("\ufeff")
            writer = csv.writer(response)
            writer.writerow(["比较编号", "开始日期", "结束日期", "取件提前天数", "币种", "包含部分报价", "地址", "平台", "承运商", "服务", "最新日期", "最新价格",
                             "上次日期", "上次价格", "涨跌金额", "涨跌%", "期间涨跌%", "最低", "最高", "均价", "CV%", "振幅%", "最大相邻涨跌%", "有效日", "采样日", "覆盖率%"])
            def safe_cell(value):
                if value is None:
                    return ""
                if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")):
                    return "'" + value
                return value
            for row in report["rows"]:
                values = [report["profile"]["code"], report["filters"]["start"], report["filters"]["end"], report["filters"]["lead"],
                          report["filters"]["currency"], report["filters"]["include_partial"]]
                values += [row[key] for key in ("address", "platform", "carrier", "service", "latest_date", "latest", "previous_date", "previous", "latest_change", "latest_change_pct",
                                               "period_change_pct", "minimum", "maximum", "mean", "volatility_pct", "range_pct", "max_adjacent_move_pct", "samples", "expected", "coverage_pct")]
                writer.writerow([safe_cell(value) for value in values])
            return response
        if request.GET.get("step") == "auto_quote_export":
            batch = get_object_or_404(visible_batches(request.user), pk=request.GET.get("batch"))
            response = HttpResponse(content_type="text/csv; charset=utf-8")
            response["Content-Disposition"] = f'attachment; filename="auto-quote-{batch.pk}.csv"'
            response.write("\ufeff")
            writer = csv.writer(response)
            writer.writerow(["组", "发货仓", "取件日期", "城市", "州", "邮编", "详细地址", "距离(miles)", "状态", "询价时间", "完成时间", "错误", "报价结果(JSON)"])

            def safe(value):
                text = str(value if value is not None else "")
                return "'" + text if text.startswith(("=", "+", "-", "@", "\t", "\r")) else text

            for item in batch.items.iterator():
                a = item.address_snapshot
                values = [batch.group, batch.parameters.get("originWarehouse"), batch.parameters.get("pickupDate"),
                          a["city"], a["state"], a["zipcode"], a["address"], a.get("distance_miles"),
                          item.status, item.started_at, item.finished_at, item.error,
                          json.dumps(item.result, ensure_ascii=False)]
                writer.writerow([safe(value) for value in values])
            return response
        if kind == "addresses":
            group = check_group(request.GET.get("group"))
            page = Paginator(AutoQuoteAddress.objects.filter(group=group), 50).get_page(request.GET.get("page"))
            return JsonResponse({"success": True, "rows": [a.snapshot() for a in page],
                                 "page": page.number, "pages": page.paginator.num_pages, "total": page.paginator.count})
        if kind == "batch":
            batch = get_object_or_404(visible_batches(request.user), pk=request.GET.get("batch"))
            items = batch.items.all()
            if request.GET.get("status"):
                items = items.filter(status=request.GET["status"])
            page = Paginator(items, 30).get_page(request.GET.get("page"))
            return JsonResponse({"success": True, "batch": batch_summary(batch), "parameters": batch.parameters,
                                 "page": page.number, "pages": page.paginator.num_pages,
                                 "rows": [{"id": item.pk, "address": item.address_snapshot, "status": item.status,
                                           "started_at": item.started_at, "finished_at": item.finished_at,
                                           "error": item.error, "result": item.result, "request_payload": item.request_payload,
                                           "attempts": item.attempts} for item in page]})
        page = Paginator(visible_batches(request.user), 20).get_page(request.GET.get("page"))
        state = AutoQuoteWorkerState.objects.filter(pk=1).first()
        online = bool(state and state.heartbeat_at and state.heartbeat_at > timezone.now() - timedelta(seconds=30))
        return JsonResponse({"success": True, "batches": [batch_summary(batch) for batch in page],
                             "page": page.number, "pages": page.paginator.num_pages, "worker_online": online,
                             "groups": {group: AutoQuoteAddress.objects.filter(group=group).count() for group in ("LA", "SAV", "NJ")}})
    except (ValueError, TypeError) as exc:
        return JsonResponse({"success": False, "message": str(exc)}, status=400)


def auto_quote_post(request):
    if not request.user.is_authenticated:
        raise PermissionDenied
    try:
        step = request.POST.get("step")
        if step == "auto_quote_import":
            group = check_group(request.POST.get("group"))
            upload = request.FILES.get("file")
            if not upload:
                raise ValueError("请选择Excel文件")
            rows, errors = read_addresses(upload)
            if errors:
                return JsonResponse({"success": False, "message": f"发现{len(errors)}行错误，本次未导入，请修正后重传。",
                                     "errors": errors[:100]}, status=400)
            if not rows:
                raise ValueError("Excel中没有地址记录")
            return JsonResponse({"success": True, **import_addresses(group, rows, request.user.pk)})
        if step == "auto_quote_start":
            parameters = json.loads(request.POST.get("quote_payload") or "{}")
            if not isinstance(parameters, dict):
                raise ValueError("询价参数格式错误")
            # Resolve the selected origin on the server, exactly as for the selector.
            origin = next((a for a in SystemParameter.get_zem_warehouse_addresses()
                           if a["warehouse"] == parameters.get("originWarehouse")), None)
            if not origin:
                raise ValueError("请选择有效的发货仓库")
            parameters.update(originCity=origin.get("city"), originState=origin.get("state"),
                              originPostCode=origin.get("postCode"), originDetailAddress=origin.get("detailAddress", ""),
                              originType=1)
            for key in ("destinationWarehouse", "destinationCity", "destinationState", "destinationPostCode", "destinationDetailAddress"):
                parameters.pop(key, None)
            gateway_config()
            batch = create_batch(request.POST.get("group"), parameters, request.user.pk, request.POST.get("submission_id"))
            return JsonResponse({"success": True, "batch": batch_summary(batch)})
        batch = get_object_or_404(visible_batches(request.user), pk=request.POST.get("batch"))
        if step == "auto_quote_stop":
            stop_batch(batch)
            batch.refresh_from_db()
        elif step == "auto_quote_retry":
            gateway_config()
            batch = create_batch(batch.group, batch.parameters, request.user.pk, request.POST.get("submission_id"), parent=batch)
        else:
            raise ValueError("未知操作")
        return JsonResponse({"success": True, "batch": batch_summary(batch)})
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError, BadZipFile, InvalidFileException) as exc:
        return JsonResponse({"success": False, "message": str(exc)}, status=400)
    except IntegrityError:
        return JsonResponse({"success": False, "message": "重复提交，请刷新任务列表查看"}, status=409)
