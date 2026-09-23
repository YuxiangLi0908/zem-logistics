"""On-demand detached worker; database fencing covers multiple Web processes/hosts."""
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteItem
from warehouse.utils.auto_quote import queue_lock

logger = logging.getLogger(__name__)
LEASE_SECONDS = 60
IDLE_SECONDS = 1800
_children = []


def has_work():
    return AutoQuoteItem.objects.filter(status="running").exists() or AutoQuoteItem.objects.filter(
        status="pending", batch__stop_requested=False).exists()


@transaction.atomic
def reserve_worker(require_work=True):
    state = queue_lock()
    now = timezone.now()
    if require_work and not has_work():
        return None
    # Also respect a recently active worker from a deployment before this migration.
    busy = (state.lease_until and state.lease_until > now) or (
        state.heartbeat_at and state.heartbeat_at > now - timedelta(seconds=30))
    if busy:
        if not state.last_error:
            state.last_activity_at = now
            state.save(update_fields=["last_activity_at"])
        return None
    state.run_token = uuid.uuid4()
    state.lease_until = now + timedelta(seconds=LEASE_SECONDS)
    state.heartbeat_at = None
    state.last_activity_at = now
    state.last_error = ""
    state.save()
    return state.run_token


@transaction.atomic
def release_worker(token, error=""):
    state = queue_lock()
    if state.run_token != token:
        return
    state.run_token = None
    state.heartbeat_at = None
    state.lease_until = timezone.now() + timedelta(seconds=LEASE_SECONDS) if error else None
    state.last_error = error
    state.save()


@transaction.atomic
def worker_pulse(token, idle_seconds=IDLE_SECONDS):
    state = queue_lock()
    if state.run_token != token:
        return False
    now = timezone.now()
    if has_work():
        state.last_activity_at = now
    elif idle_seconds and state.last_activity_at and (now - state.last_activity_at).total_seconds() >= idle_seconds:
        # The empty-queue check and ownership release share the submission lock.
        state.run_token = None
        state.lease_until = None
        state.heartbeat_at = None
        state.save()
        return False
    state.heartbeat_at = now
    state.lease_until = now + timedelta(seconds=LEASE_SECONDS)
    state.save()
    return True


def spawn_worker(token):
    manage = Path(__file__).resolve().parents[2] / "manage.py"
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
    command = [sys.executable, "-u", str(manage), "run_auto_quote_worker", "--concurrency", "3",
               "--idle-seconds", str(IDLE_SECONDS), "--run-token", str(token)]
    options = {"cwd": str(manage.parent), "env": env, "stdin": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    log_path = Path(getattr(settings, "AUTO_QUOTE_WORKER_LOG", Path(tempfile.gettempdir()) / "auto_quote_worker.log"))
    # Keep stderr out of Web response pipes; preserve startup failures for operators.
    with log_path.open("ab") as output:
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, **options)
    _children.append(child)


def ensure_worker():
    _children[:] = [child for child in _children if child.poll() is None]
    token = reserve_worker()
    if token is None:
        return
    try:
        spawn_worker(token)
    except Exception:
        logger.exception("Unable to launch automatic quote worker")
        release_worker(token, "后台启动失败，任务已保留；请检查执行程序日志及服务器权限")


def schedule_worker():
    # Never expose uncommitted tasks to a new process (also works under ATOMIC_REQUESTS).
    transaction.on_commit(ensure_worker)
