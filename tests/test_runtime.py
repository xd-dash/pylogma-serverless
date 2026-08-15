"""Behavioral tests for pylogma_serverless.runtime, mirroring the cases in
router/runtime_test.go from xd-dash/logma-serverless (handlePublish's
channel-default-override, empty/{}-payload drop, and claim()'s
CompareAndSwap semantics).

These only exercise the pure control-message handlers and claim/holder
state machine -- no real Redis connection is required since Runtime's
constructor is lazy about connecting.
"""

import asyncio

import pytest

from pylogma_serverless.runtime import Runtime, RuntimeHolder, RuntimeState

pytestmark = pytest.mark.asyncio


async def test_handle_publish_uses_redis_channel_when_payload_omits_it():
    rt = Runtime()
    await rt._handle_publish("dev:global:logs:1", '{"data": {"msg": "hi"}}')
    req = rt.events.get_nowait()
    assert req.channel == "dev:global:logs:1"
    assert req.data == {"msg": "hi"}


async def test_handle_publish_prefers_payload_channel_override():
    rt = Runtime()
    await rt._handle_publish("dev:global:logs:1", '{"channel": "custom", "data": 1}')
    req = rt.events.get_nowait()
    assert req.channel == "custom"
    assert req.data == 1


@pytest.mark.parametrize("payload", ["", "{}"])
async def test_handle_publish_drops_empty_payload(payload):
    rt = Runtime()
    await rt._handle_publish("dev:global:logs:1", payload)
    assert rt.events.qsize() == 0


async def test_handle_publish_drops_invalid_json():
    rt = Runtime()
    await rt._handle_publish("dev:global:logs:1", "not json")
    assert rt.events.qsize() == 0


async def test_handle_add_starts_subscription():
    rt = Runtime()
    rt._handle_add('{"channel": "dev:global:logs:2"}')
    assert "dev:global:logs:2" in rt._subscriptions
    rt._subscriptions["dev:global:logs:2"].task.cancel()


async def test_handle_add_ignores_duplicate_channel():
    rt = Runtime()
    rt._handle_add('{"channel": "dev:global:logs:2"}')
    first_task = rt._subscriptions["dev:global:logs:2"].task
    rt._handle_add('{"channel": "dev:global:logs:2"}')
    assert rt._subscriptions["dev:global:logs:2"].task is first_task
    first_task.cancel()


async def test_handle_add_ignores_missing_channel():
    rt = Runtime()
    rt._handle_add("{}")
    assert rt._subscriptions == {}


def test_claim_is_compare_and_swap():
    rt = Runtime()
    assert rt.state == RuntimeState.IDLE
    assert rt.claim() is True
    assert rt.state == RuntimeState.RUNNING
    assert rt.claim() is False


def test_runtime_holder_mints_fresh_runtime_after_done():
    holder = RuntimeHolder()
    rt1 = holder.claim()
    assert rt1 is not None

    # Still running -> second claim is refused.
    assert holder.claim() is None

    rt1.state = RuntimeState.DONE
    rt2 = holder.claim()
    assert rt2 is not None
    assert rt2 is not rt1
