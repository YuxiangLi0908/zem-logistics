"""Reduce large HTML responses and log preparation time (not socket send time)."""

import logging
import os
from time import perf_counter

from django.middleware.gzip import GZipMiddleware


logger = logging.getLogger("warehouse.response_delivery")


class LargePageResponseMiddleware(GZipMiddleware):
    def process_request(self, request):
        if request.path_info in {"/post_nsop/", "/receivable_accounting/"}:
            request._large_page_started = perf_counter()

    def process_response(self, request, response):
        started = getattr(request, "_large_page_started", None)
        if (
            started is None
            or response.streaming
            or response.has_header("Content-Disposition")
            or response.get("Content-Type", "").split(";", 1)[0] != "text/html"
        ):
            return response

        prepared = perf_counter()
        original_bytes = len(response.content)
        response = super().process_response(request, response)
        logger.info(
            "response_ready pid=%s method=%s path=%s status=%s "
            "app_ms=%.1f compression_ms=%.1f original_bytes=%s "
            "response_bytes=%s encoding=%s",
            os.getpid(), request.method, request.path_info, response.status_code,
            (prepared - started) * 1000, (perf_counter() - prepared) * 1000,
            original_bytes, len(response.content),
            response.get("Content-Encoding", "identity"),
        )
        return response
