import io
import os
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.db import transaction
from django.test import TransactionTestCase, RequestFactory, override_settings
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem, AutoQuoteWorkerState
from warehouse.utils.auto_quote import claim_item
from warehouse.utils.auto_quote_lifecycle import (
    ensure_worker, reserve_worker, release_worker, schedule_worker, spawn_worker, worker_pulse,
)
from warehouse.views.post_port.auto_quote import auto_quote_post


class WorkerLifecycleTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("worker-owner")
        self.batch = AutoQuoteBatch.objects.create(group="NJ", operator=self.user, submission_id=uuid.uuid4())
        self.item = AutoQuoteItem.objects.create(batch=self.batch, address_snapshot={})

    def test_repeated_submissions_only_launch_one_worker(self):
        with patch("warehouse.utils.auto_quote_lifecycle.spawn_worker") as spawn:
            ensure_worker()
            ensure_worker()
        spawn.assert_called_once()
        token = spawn.call_args.args[0]
        self.assertTrue(worker_pulse(token))
        self.assertIsNone(reserve_worker())

    def test_idle_exit_waits_full_thirty_minutes_and_restarts_for_new_work(self):
        now = timezone.now()
        with patch("warehouse.utils.auto_quote_lifecycle.timezone.now", return_value=now):
            token = reserve_worker()
            self.assertTrue(worker_pulse(token))
        self.item.status = "success"
        self.item.save()
        with patch("warehouse.utils.auto_quote_lifecycle.timezone.now", return_value=now + timedelta(seconds=1799)):
            self.assertTrue(worker_pulse(token))
        with patch("warehouse.utils.auto_quote_lifecycle.timezone.now", return_value=now + timedelta(seconds=1800)):
            self.assertFalse(worker_pulse(token))
        state = AutoQuoteWorkerState.objects.get(pk=1)
        self.assertIsNone(state.run_token)
        self.assertIsNone(state.heartbeat_at)
        self.item.status = "pending"
        self.item.save()
        self.assertNotEqual(reserve_worker(), token)

    def test_pending_and_running_work_prevent_idle_shutdown(self):
        token = reserve_worker()
        for status in ("pending", "running"):
            AutoQuoteWorkerState.objects.update(last_activity_at=timezone.now() - timedelta(hours=1))
            self.item.status = status
            self.item.save()
            self.assertTrue(worker_pulse(token))
            self.assertGreater(AutoQuoteWorkerState.objects.get(pk=1).last_activity_at, timezone.now() - timedelta(seconds=5))

    def test_new_submission_during_idle_keeps_same_worker_and_resets_timer(self):
        token = reserve_worker()
        AutoQuoteWorkerState.objects.update(last_activity_at=timezone.now() - timedelta(minutes=29))
        with patch("warehouse.utils.auto_quote_lifecycle.spawn_worker") as spawn:
            ensure_worker()
        spawn.assert_not_called()
        state = AutoQuoteWorkerState.objects.get(pk=1)
        self.assertEqual(state.run_token, token)
        self.assertGreater(state.last_activity_at, timezone.now() - timedelta(seconds=5))

    def test_expired_owner_cannot_claim_release_or_heartbeat_new_worker(self):
        old = reserve_worker()
        AutoQuoteWorkerState.objects.update(lease_until=timezone.now() - timedelta(seconds=1))
        new = reserve_worker()
        self.assertNotEqual(old, new)
        self.assertFalse(worker_pulse(old))
        self.assertIsNone(claim_item(worker_token=old))
        release_worker(old)
        self.assertEqual(AutoQuoteWorkerState.objects.get(pk=1).run_token, new)
        self.assertIsNotNone(claim_item(worker_token=new))

    def test_launch_is_after_commit_and_rollback_never_launches(self):
        with patch("warehouse.utils.auto_quote_lifecycle.spawn_worker") as spawn:
            with transaction.atomic():
                schedule_worker()
                spawn.assert_not_called()
                transaction.set_rollback(True)
            spawn.assert_not_called()
            with transaction.atomic():
                schedule_worker()
                spawn.assert_not_called()
            spawn.assert_called_once()

    def test_launch_failure_keeps_queue_and_cools_down_retries(self):
        with patch("warehouse.utils.auto_quote_lifecycle.spawn_worker", side_effect=OSError("cannot fork")) as spawn:
            with self.assertLogs("warehouse.utils.auto_quote_lifecycle", level="ERROR"):
                ensure_worker()
            ensure_worker()
        spawn.assert_called_once()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "pending")
        self.assertTrue(AutoQuoteWorkerState.objects.get(pk=1).last_error)

    def test_wake_is_scoped_to_requesting_user(self):
        request = RequestFactory().post("/post_nsop/", {"step": "auto_quote_wake"})
        request.user = User.objects.create_user("other")
        with patch("warehouse.views.post_port.auto_quote.schedule_worker") as schedule:
            auto_quote_post(request)
            schedule.assert_not_called()
            request.user = self.user
            auto_quote_post(request)
            schedule.assert_called_once()

    def test_spawn_uses_current_interpreter_environment_and_detached_stdio(self):
        with override_settings(AUTO_QUOTE_WORKER_LOG="worker.log"), patch("pathlib.Path.open"):
            with patch("warehouse.utils.auto_quote_lifecycle.subprocess.Popen") as popen:
                token = uuid.uuid4()
                spawn_worker(token)
                args, kwargs = popen.call_args
                self.assertIn(str(token), args[0])
                self.assertIn("1800", args[0])
                self.assertTrue(Path(args[0][2]).is_file())
                self.assertNotIn("shell", kwargs)
                self.assertIn("DJANGO_SETTINGS_MODULE", kwargs["env"])
                self.assertIn("creationflags" if os.name == "nt" else "start_new_session", kwargs)

    def test_command_once_exits_cleanly_when_queue_is_empty(self):
        from warehouse.management.commands.run_auto_quote_worker import Command
        self.item.status = "success"
        self.item.save()
        with patch("warehouse.management.commands.run_auto_quote_worker.gateway_config"):
            Command(stdout=io.StringIO()).handle(concurrency=3, once=True, run_token=None, idle_seconds=1800)
        state = AutoQuoteWorkerState.objects.get(pk=1)
        self.assertIsNone(state.run_token)
        self.assertIsNone(state.heartbeat_at)

    def test_command_idle_exit_does_not_launch_carrier_requests(self):
        from warehouse.management.commands.run_auto_quote_worker import Command
        token = reserve_worker()
        self.item.status = "success"
        self.item.save()
        AutoQuoteWorkerState.objects.update(last_activity_at=timezone.now() - timedelta(minutes=31))
        with patch("warehouse.management.commands.run_auto_quote_worker.gateway_config"), patch("warehouse.management.commands.run_auto_quote_worker.execute_quote") as quote:
            Command(stdout=io.StringIO()).handle(concurrency=3, once=False, run_token=token, idle_seconds=1800)
            quote.assert_not_called()
        self.assertIsNone(AutoQuoteWorkerState.objects.get(pk=1).run_token)

    def test_command_releases_ownership_on_exception(self):
        from warehouse.management.commands.run_auto_quote_worker import Command
        command = Command(stdout=io.StringIO())
        with patch("warehouse.management.commands.run_auto_quote_worker.gateway_config", side_effect=ValueError("bad config")):
            with self.assertRaises(ValueError):
                command.handle(concurrency=3, once=False, run_token=None, idle_seconds=1800)
        state = AutoQuoteWorkerState.objects.get(pk=1)
        self.assertIsNone(state.run_token)
        self.assertTrue(state.last_error)
