import asyncio
import json

import aiohttp


async def wait_for_kakas_quotes(session, url, headers, quote_uuid, carrier_result):
    """Wait for completion, allowing 50 seconds without a quote update."""
    loop = asyncio.get_running_loop()
    payload = carrier_result.get("data")

    def content(body):
        while isinstance(body, dict):
            if "finish" in body or "rates" in body:
                return body
            body = body.get("data")
        return {}

    def snapshot(body):
        return json.dumps(content(body).get("rates", []), sort_keys=True)

    last_snapshot = snapshot(payload)
    deadline = loop.time() + 50
    while content(payload).get("finish") is not True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            carrier_result["warning"] = "卡卡省未结束全部报价，但已超时"
            break
        try:
            async with session.get(
                url,
                params={"carrier": "kakas", "uuid": quote_uuid},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=remaining),
            ) as response:
                response_text = await response.text()
                try:
                    raw_payload = json.loads(response_text)
                except json.JSONDecodeError:
                    raw_payload = response_text
                payload = raw_payload
                quote_content = content(raw_payload)
                if response.status == 200 and quote_content:
                    carrier_result["data"] = raw_payload
                    current_snapshot = snapshot(raw_payload)
                    if current_snapshot != last_snapshot:
                        last_snapshot = current_snapshot
                        deadline = loop.time() + 50
                    if quote_content.get("finish") is True:
                        break
        except (aiohttp.ClientError, asyncio.TimeoutError):
            # Keep the last usable prices when a later poll fails.
            pass
        await asyncio.sleep(min(1, max(0, deadline - loop.time())))
    return payload
