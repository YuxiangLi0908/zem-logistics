import asyncio
import logging
import uuid

from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteItem
from warehouse.utils.auto_quote_lifecycle import IDLE_SECONDS, reserve_worker, release_worker, worker_pulse
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
    help = "Run automatic quotes; exit after 30 idle minutes by default."

    def add_arguments(self, parser):
        parser.add_argument("--concurrency", type=int, default=3, choices=range(1, 11))
        parser.add_argument("--once", action="store_true", help="Drain currently queued jobs and exit")
        parser.add_argument("--idle-seconds", type=int, default=IDLE_SECONDS, help="Exit after idle seconds (0 keeps running)")
        parser.add_argument("--run-token", type=uuid.UUID, help="Internal on-demand launch ownership token")

    def handle(self, *args, **options):
        if options["idle_seconds"] < 0:
            raise CommandError("idle-seconds must be nonnegative")
        token = options["run_token"] or reserve_worker(require_work=False)
        if token is None:
            self.stdout.write("An automatic quote worker is already running or starting.")
            return
        error = ""
        try:
            gateway_config()
            self.stdout.write(f'Auto quote worker started, concurrency={options["concurrency"]}, idle_seconds={options["idle_seconds"]}')
            asyncio.run(self.run(options["concurrency"], options["once"], token, options["idle_seconds"]))
        except KeyboardInterrupt:
            self.stdout.write("Stopped; unfinished items will resume after their leases expire.")
        except Exception:
            error = "执行程序异常退出，任务已保留；请检查 auto_quote_worker.log"
            raise
        finally:
            release_worker(token, error)

    async def run(self, concurrency, once, token, idle_seconds):
        pacer = RequestPacer()
        running = set()
        try:
            while True:
                await sync_to_async(close_old_connections)()
                if not await sync_to_async(worker_pulse)(token, idle_seconds):
                    self.stdout.write("Worker stopped: idle timeout or ownership transferred.")
                    break
                done = {task for task in running if task.done()}
                for task in done:
                    task.result()
                running -= done
                while len(running) < concurrency:
                    item = await sync_to_async(claim_item)(concurrency, worker_token=token)
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
