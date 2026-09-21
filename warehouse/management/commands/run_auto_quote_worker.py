import asyncio
import logging

from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteItem, AutoQuoteWorkerState
from warehouse.utils.auto_quote import (
    address_payload, claim_item, classify_result, complete_item, save_checkpoint,
)
from warehouse.utils.multi_carrier_quote import execute_quote, gateway_config

logger = logging.getLogger(__name__)


class RequestPacer:
    def __init__(self):
        self.locks = {kind: asyncio.Lock() for kind in ("submit", "poll")}
        self.next_time = {kind: 0 for kind in self.locks}

    async def wait(self, kind):
        async with self.locks[kind]:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0, self.next_time[kind] - loop.time()))
            self.next_time[kind] = loop.time() + (1.0 if kind == "submit" else 0.5)


async def process_item(item, pacer):
    async def checkpoint(result, quote_uuid, payload):
        await sync_to_async(save_checkpoint)(item, result, quote_uuid, payload)

    try:
        form = address_payload(item.batch.parameters, item.address_snapshot)
        if timezone.localdate().isoformat() > form["pickupDate"]:
            raise ValueError("取件日期已过期，请使用新的取件日期创建任务")
        result, _ = await asyncio.wait_for(execute_quote(
            form, automatic=True, checkpoint=checkpoint,
            resume={"result": item.result, "quote_uuid": item.quote_uuid}, before_request=pacer.wait,
        ), timeout=240)
        status, error = classify_result(result)
    except asyncio.TimeoutError:
        saved = await sync_to_async(AutoQuoteItem.objects.get)(pk=item.pk)
        status, _ = classify_result(saved.result)
        status = "partial" if status in ("success", "partial") else "failed"
        error = "单条询价超过4分钟，已保留返回结果，可稍后重试"
    except Exception as exc:
        logger.exception("Auto quote item %s failed", item.pk)
        saved = await sync_to_async(AutoQuoteItem.objects.get)(pk=item.pk)
        status, _ = classify_result(saved.result)
        status = "partial" if status in ("success", "partial") else "failed"
        error = str(exc)[:1000]
    await sync_to_async(complete_item)(item, status, error)


class Command(BaseCommand):
    help = "Run durable automatic quotes (default: three concurrent addresses). Run as a supervised process."

    def add_arguments(self, parser):
        parser.add_argument("--concurrency", type=int, default=3, choices=range(1, 11))
        parser.add_argument("--once", action="store_true", help="Drain currently queued jobs and exit")

    def handle(self, *args, **options):
        try:
            gateway_config()
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(f'Auto quote worker started, concurrency={options["concurrency"]}')
        try:
            asyncio.run(self.run(options["concurrency"], options["once"]))
        except KeyboardInterrupt:
            self.stdout.write("Stopped; unfinished items will resume after their leases expire.")

    async def run(self, concurrency, once):
        pacer = RequestPacer()
        running = set()
        try:
            while True:
                await sync_to_async(close_old_connections)()
                await sync_to_async(AutoQuoteWorkerState.objects.update_or_create)(
                    pk=1, defaults={"heartbeat_at": timezone.now()})
                done = {task for task in running if task.done()}
                for task in done:
                    task.result()
                running -= done
                while len(running) < concurrency:
                    item = await sync_to_async(claim_item)(concurrency)
                    if not item:
                        break
                    running.add(asyncio.create_task(process_item(item, pacer)))
                if once and not running:
                    break
                await asyncio.sleep(2)
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
