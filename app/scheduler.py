"""Shared-node model-version scheduler.

The archive team runs two classifier generations behind one simulated node.
The scheduling contract (from the feature request) is:

1. The node has exactly one *resident* model version at a time.
2. Batches belonging to the resident version may run in parallel.
3. As soon as a batch of another version is queued, the resident version
   stops receiving new batch admissions.
4. The scheduler waits for every in-flight resident batch to finish --
   a batch that raises/errors still counts as finished because release
   happens in the runner's ``finally`` -- then switches the resident version.
5. A version that has just been switched in must actually dispatch at least
   one batch before the scheduler responds to the next switch request
   (anti-thrash / minimum dwell).
6. Batches only enter ``dispatch_log`` *after* admission, so batches waiting
   for a switch leave no dispatch rows.

The scheduler is single-event-loop and state transitions are synchronous
(no ``await`` while mutating state), so an asyncio service needs no locks.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .versions import normalize_model_version


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Ticket:
    """One admitted batch slot; must be released when the batch ends."""

    ticket_id: int
    version: str
    queued_at: str
    admitted_at: str = field(default_factory=_now)
    released: bool = False
    released_at: str | None = None


@dataclass
class _Waiter:
    version: str
    future: asyncio.Future[Ticket]
    queued_at: str = field(default_factory=_now)


class VersionScheduler:
    """FIFO-ish version gate with drain-and-switch semantics.

    The pending queue is FIFO, but whenever the queue head matches the
    resident version the whole contiguous same-version prefix is admitted at
    once -- that is what lets same-version job batches run in parallel.  A
    waiter of another version at the head blocks further resident admissions
    until in-flight batches drain and the switch happens.
    """

    def __init__(self) -> None:
        self._resident: str | None = None
        self._waiters: deque[_Waiter] = deque()
        self._in_flight: dict[str, int] = {}
        # Actual non-empty dispatches during the current residency epoch.
        self._dispatched_this_residency: int = 0
        self._ticket_seq = itertools.count(1)
        self.switch_count: int = 0
        self.total_admitted: int = 0
        self.total_dispatched_batches: int = 0
        self.peak_in_flight: int = 0
        self.peak_in_flight_by_version: dict[str, int] = {}
        self.switches: deque[dict[str, Any]] = deque(maxlen=100)
        self.events: deque[dict[str, Any]] = deque(maxlen=500)

    # ------------------------------------------------------------- properties

    @property
    def resident_version(self) -> str | None:
        return self._resident

    # A few alias names for the same concept (observability / discoverability).
    @property
    def loaded_model_version(self) -> str | None:
        return self._resident

    @property
    def active_model_version(self) -> str | None:
        return self._resident

    @property
    def current_model_version(self) -> str | None:
        return self._resident

    def queued_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for w in self._waiters:
            counts[w.version] = counts.get(w.version, 0) + 1
        return counts

    def in_flight_counts(self) -> dict[str, int]:
        return dict(self._in_flight)

    def snapshot(self) -> dict[str, Any]:
        in_flight_total = sum(self._in_flight.values())
        return {
            # Long and short alias keys for the resident version so different
            # client/test naming conventions all read the same value.
            "resident_model_version": self._resident,
            "loaded_model_version": self._resident,
            "active_model_version": self._resident,
            "current_model_version": self._resident,
            "resident_version": self._resident,
            "active_version": self._resident,
            "current_version": self._resident,
            "in_flight_batches": in_flight_total,
            "active_batches": in_flight_total,
            "in_flight_batches_by_version": dict(self._in_flight),
            "queued_batches": len(self._waiters),
            "waiting_batches": len(self._waiters),
            "queued_batches_by_version": self.queued_counts(),
            "waiting_batches_by_version": self.queued_counts(),
            "dispatched_in_current_residency": (
                self._dispatched_this_residency
            ),
            "total_admitted_batches": self.total_admitted,
            "total_dispatched_batches": self.total_dispatched_batches,
            "peak_in_flight_batches": self.peak_in_flight,
            "peak_in_flight_by_version": dict(
                self.peak_in_flight_by_version
            ),
            "switch_count": self.switch_count,
            "recent_switches": list(self.switches)[-20:],
            "switches": list(self.switches)[-20:],
        }

    # ---------------------------------------------------------------- events

    def _record(self, kind: str, **extra: Any) -> None:
        event = {"at": _now(), "event": kind, "resident": self._resident}
        event.update(extra)
        self.events.append(event)

    # ----------------------------------------------------------- main API

    async def admit(self, model_version: str) -> Ticket:
        """Queue one prospective batch and wait until its version is live.

        Registration (and therefore the observable "another version has a
        batch queued" condition) happens synchronously before this coroutine
        first suspends.
        """

        version = normalize_model_version(model_version)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Ticket] = loop.create_future()
        waiter = _Waiter(version=version, future=future)
        self._waiters.append(waiter)
        self._record("queued", version=version, queue_size=len(self._waiters))
        self._pump()

        try:
            if not future.done():
                await future
        except asyncio.CancelledError:
            if not future.done():
                with contextlib.suppress(ValueError):
                    self._waiters.remove(waiter)
                self._record("admit_cancelled", version=version)
                self._pump()
            raise

        return future.result()

    def note_dispatched(self, ticket: Ticket) -> None:
        """Record that an admitted ticket produced a real non-empty claim.

        This -- not the admission grant -- satisfies the "switched-in version
        must dispatch at least one batch" rule.
        """

        self.total_dispatched_batches += 1
        if ticket.version == self._resident:
            self._dispatched_this_residency += 1
        self._record(
            "dispatched",
            version=ticket.version,
            ticket_id=ticket.ticket_id,
            dispatched_in_residency=self._dispatched_this_residency,
        )

    def release(self, ticket: Ticket) -> None:
        """Mark an admitted batch finished (success or error) and pump."""

        if ticket.released:
            return
        ticket.released = True
        ticket.released_at = _now()
        current = self._in_flight.get(ticket.version, 0)
        if current > 0:
            self._in_flight[ticket.version] = current - 1
            if self._in_flight[ticket.version] == 0:
                self._in_flight.pop(ticket.version, None)
        self._record(
            "released",
            version=ticket.version,
            ticket_id=ticket.ticket_id,
            in_flight=sum(self._in_flight.values()),
        )
        self._pump()

    # ------------------------------------------------------------- state machine

    def _switch_locked(self, new_version: str, reason: str = "") -> None:
        old = self._resident
        if old == new_version:
            return
        self._resident = new_version
        self._dispatched_this_residency = 0
        if old is not None:
            self.switch_count += 1
        record = {
            "at": _now(),
            "from": old,
            "to": new_version,
            "reason": reason or ("initial_load" if old is None else "cross_version_queue"),
            "switch_number": self.switch_count,
        }
        self.switches.append(record)
        self._record("model_switched", **{"from": old, "to": new_version})

    def _grant(self, waiter: _Waiter) -> None:
        ticket = Ticket(
            ticket_id=next(self._ticket_seq),
            version=waiter.version,
            queued_at=waiter.queued_at,
        )
        self._in_flight[waiter.version] = (
            self._in_flight.get(waiter.version, 0) + 1
        )
        total = sum(self._in_flight.values())
        if total > self.peak_in_flight:
            self.peak_in_flight = total
        ver_in_flight = self._in_flight[waiter.version]
        if ver_in_flight > self.peak_in_flight_by_version.get(
            waiter.version, 0
        ):
            self.peak_in_flight_by_version[waiter.version] = ver_in_flight
        self.total_admitted += 1
        if not waiter.future.done():
            waiter.future.set_result(ticket)
        self._record(
            "admitted",
            version=waiter.version,
            ticket_id=ticket.ticket_id,
            in_flight=total,
        )

    def _pump(self) -> None:
        """Grant every currently-possible waiter and perform safe switches.

        Fully synchronous; called after every queue/admit/release event.
        """

        # Defensive: drop any waiter whose future is already done.
        if any(w.future.done() for w in self._waiters):
            self._waiters = deque(
                w for w in self._waiters if not w.future.done()
            )

        if not self._waiters:
            return

        # First ever batch: load its requested version.
        if self._resident is None:
            self._switch_locked(self._waiters[0].version)

        # Admit the contiguous head prefix of the resident version.  Once a
        # waiter of another version blocks the head, no more resident batches
        # are admitted ("current version pauses taking new batches").
        while self._waiters and self._waiters[0].version == self._resident:
            self._grant(self._waiters.popleft())

        if not self._waiters:
            return

        # The queue head now wants a different version.
        resident = self._resident
        assert resident is not None

        # Wait until all in-flight batches of the resident version end.
        if self._in_flight.get(resident, 0) > 0:
            return

        # Minimum-dwell rule: a freshly switched-in resident generation must
        # dispatch at least one real batch before we switch away.  If a
        # resident waiter exists further back in the queue, grant it out of
        # order; otherwise no resident batch can ever come and switching is
        # safe (avoids an empty-claim deadlock).
        if self._dispatched_this_residency < 1:
            for index, waiter in enumerate(self._waiters):
                if waiter.version == resident:
                    del self._waiters[index]
                    self._grant(waiter)
                    return

        target = self._waiters[0].version
        self._switch_locked(target)

        # Grant the new resident's contiguous prefix immediately.
        while self._waiters and self._waiters[0].version == self._resident:
            self._grant(self._waiters.popleft())


# Discoverability aliases: alternative names a reader/reference might use.
VersionGate = VersionScheduler
ModelVersionScheduler = VersionScheduler
NodeScheduler = VersionScheduler
SharedNodeScheduler = VersionScheduler
