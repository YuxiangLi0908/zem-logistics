import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from warehouse.utils.kakas_polling import wait_for_kakas_quotes


class KakasPollingTests(unittest.IsolatedAsyncioTestCase):
    async def poll(self, responses, initial=None):
        clock = SimpleNamespace(now=0)
        calls = []

        async def sleep(seconds):
            clock.now += seconds

        class Response:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def text(self):
                value = responses[min(len(calls) - 1, len(responses) - 1)]
                if isinstance(value, Exception):
                    raise value
                return json.dumps(value)

        def get(*args, **kwargs):
            calls.append(clock.now)
            return Response()

        carrier = {"status": "success", "data": initial or {"data": {"uuid": "test"}}}
        fake_asyncio = SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: clock.now),
            sleep=sleep, TimeoutError=asyncio.TimeoutError,
        )
        with patch("warehouse.utils.kakas_polling.asyncio", fake_asyncio):
            raw = await wait_for_kakas_quotes(SimpleNamespace(get=get), "url", {}, "test", carrier)
        return carrier, raw, calls, clock.now

    async def test_partial_rates_wait_until_finish(self):
        partial = {"data": {"finish": False, "rates": [{"totalPrice": 10}]}}
        complete = {"code": "200", "data": {"finish": True, "rates": [1, 2]}, "extra": "kept"}
        carrier, raw, calls, _ = await self.poll([partial, partial, complete])
        self.assertEqual(len(calls), 3)
        self.assertEqual(raw, complete)
        self.assertEqual(carrier["data"], complete)
        self.assertNotIn("warning", carrier)

    async def test_unchanged_partial_times_out_with_prices(self):
        partial = {"data": {"finish": False, "rates": [1]}}
        carrier, raw, _, elapsed = await self.poll([partial])
        self.assertEqual(elapsed, 50)
        self.assertEqual(carrier["data"], partial)
        self.assertIn("已超时", carrier["warning"])

    async def test_new_rates_restart_idle_deadline(self):
        first = {"data": {"finish": False, "rates": [1]}}
        second = {"data": {"finish": False, "rates": [1, 2]}}
        carrier, _, _, elapsed = await self.poll([first] * 30 + [second])
        self.assertEqual(elapsed, 80)
        self.assertEqual(carrier["data"], second)

    async def test_connection_timeout_preserves_partial_prices(self):
        partial = {"data": {"finish": False, "rates": [1]}}
        carrier, _, _, elapsed = await self.poll([partial, asyncio.TimeoutError()])
        self.assertEqual(elapsed, 50)
        self.assertEqual(carrier["data"], partial)
        self.assertIn("warning", carrier)

    async def test_initial_finished_response_needs_no_poll(self):
        complete = {"data": {"finish": True, "rates": []}}
        carrier, raw, calls, _ = await self.poll([], initial=complete)
        self.assertEqual(calls, [])
        self.assertEqual(raw, complete)
        self.assertNotIn("warning", carrier)
