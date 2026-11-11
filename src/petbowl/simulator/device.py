# src/petbowl/simulator/device.py
"""Per-device state model and the virtual-time event stream."""

import heapq
import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from petbowl.common.events import Event, Kind

TICK_S = 30.0
HEARTBEAT_EVERY = 2  # ticks, so every 60s
SLOW_EVERY = 20  # ticks, so every 10 min: battery + water


class Device:
    def __init__(self, device_id: str, seed: int, start: datetime) -> None:
        self.device_id = device_id
        self.rng = random.Random(f"{seed}:{device_id}")
        r = self.rng

        # static traits
        self.tare_g = r.uniform(80, 200)
        self.fill_g = r.uniform(250, 350)
        self.slots: list[int] = r.choice([[8, 19], [7, 13, 19]])  # meal hours, UTC
        self.drain_per_s = r.uniform(0.8, 1.2) * 100 / (30 * 86400)  # ~30 day battery

        # dynamic state
        self.food_g = self.fill_g
        self.battery = r.uniform(60, 100)
        self.water_ml = 500.0
        self.seq = 0
        self.n = 0  # tick counter
        self.last_t = start
        self.eating = False
        self.meal_left_g = 0.0
        self.eat_rate = 0.0  # g/s
        self.next_meal = self._next_meal_after(start)
        # stable phase offset so devices never tick in lockstep
        self.next_at = start + timedelta(seconds=r.uniform(0, TICK_S))

    def _next_meal_after(self, t: datetime) -> datetime:
        midnight = t.replace(hour=0, minute=0, second=0, microsecond=0)
        for day in (0, 1):
            for h in self.slots:
                jitter = timedelta(minutes=self.rng.uniform(-45, 45))
                cand = midnight + timedelta(days=day, hours=h) + jitter
                if cand > t:
                    return cand
        raise AssertionError("unreachable: day 1 always has a future slot")

    def _advance(self, t: datetime, dt: float) -> None:
        r = self.rng
        self.battery = max(0.0, self.battery - self.drain_per_s * dt)

        if self.eating:
            eaten = min(self.meal_left_g, self.eat_rate * dt)
            self.food_g = max(0.0, self.food_g - eaten)
            self.meal_left_g -= eaten
            if self.meal_left_g <= 0:
                self.eating = False
                self.next_meal = self._next_meal_after(t)
        elif t >= self.next_meal:
            meal_g = min(r.uniform(30, 80), self.food_g)
            self.eating = True
            self.meal_left_g = meal_g
            self.eat_rate = meal_g / r.uniform(300, 600)  # 5 to 10 min

        # owner refills an emptying bowl at some point, never mid-meal
        if not self.eating and self.food_g < 60 and r.random() < 0.02:
            self.food_g = self.fill_g

    def _water(self) -> float:
        self.water_ml -= self.rng.uniform(0, 4)
        if self.water_ml < 100:
            self.water_ml = 500.0
        return self.water_ml

    def _event(self, t: datetime, kind: Kind, value: float | None) -> Event:
        e = Event(device_id=self.device_id, seq=self.seq, ts=t, kind=kind, value=value)
        self.seq += 1
        return e

    def tick(self, t: datetime) -> list[Event]:
        dt = (t - self.last_t).total_seconds()
        self.last_t = t
        self._advance(t, dt)

        weight = max(0.0, self.tare_g + self.food_g + self.rng.gauss(0, 0.5))
        out = [self._event(t, "weight_g", round(weight, 1))]
        if self.n % HEARTBEAT_EVERY == 0:
            out.append(self._event(t, "heartbeat", None))
        if self.n % SLOW_EVERY == 0:
            out.append(self._event(t, "battery_pct", round(self.battery, 1)))
            out.append(self._event(t, "water_ml", round(self._water(), 1)))
        self.n += 1
        return out

    def next_interval(self) -> timedelta:
        return timedelta(seconds=TICK_S + self.rng.uniform(-2, 2))


def make_fleet(n: int, seed: int, start: datetime) -> list[Device]:
    # print("Making fleet")
    # print(f"n: {n}, seed: {seed}, start: {start}")
    # print([Device(f"dev-{i:05d}", seed, start) for i in range(n)])
    devices = [Device(f"dev-{i:05d}", seed, start) for i in range(n)]
    # print([repr(d) for d in devices])
    return devices


def stream(devices: list[Device], end: datetime) -> Iterator[Event]:
    """Yield events from all devices in virtual-time order."""
    heap = [(d.next_at, i) for i, d in enumerate(devices)]
    heapq.heapify(heap)
    while heap:
        t, i = heapq.heappop(heap)
        if t >= end:
            continue
        d = devices[i]
        yield from d.tick(t)
        heapq.heappush(heap, (t + d.next_interval(), i))
