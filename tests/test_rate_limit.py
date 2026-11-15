"""Rate limiter pacing with a fake clock."""

from __future__ import annotations

import pytest

from rate_limit import RateLimiter


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.mark.asyncio
async def test_per_chat_pacing():
    clock = FakeClock()
    limiter = RateLimiter(per_chat_rps=2.0, global_rps=100.0, clock=clock, sleep=clock.sleep)
    await limiter.acquire("chat-a")
    await limiter.acquire("chat-a")
    assert clock.sleeps and clock.sleeps[0] == pytest.approx(0.5, abs=0.01)


@pytest.mark.asyncio
async def test_global_pacing_across_chats():
    clock = FakeClock()
    limiter = RateLimiter(per_chat_rps=100.0, global_rps=10.0, clock=clock, sleep=clock.sleep)
    await limiter.acquire("a")
    await limiter.acquire("b")
    assert clock.sleeps and clock.sleeps[0] == pytest.approx(0.1, abs=0.01)


@pytest.mark.asyncio
async def test_evicts_stale_chats():
    clock = FakeClock()
    limiter = RateLimiter(per_chat_rps=100.0, global_rps=100.0, clock=clock, sleep=clock.sleep, max_chats=2)
    for i in range(5):
        clock.now += 10
        await limiter.acquire(f"chat-{i}")
    assert len(limiter._next_chat) <= 2
