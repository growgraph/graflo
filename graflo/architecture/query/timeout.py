"""Time limits on reads: the error a backend raises, and one deadline per call.

A backend that declares ``supports_query_timeout`` hands the limit to its
database or driver, which cancels the work; the driver's own timeout error is
translated into :class:`QueryTimeoutError`. A call made of several reads (a
breadth-first walk, a chunked lookup) spends one :class:`Deadline` across all of
them: each read is given what is left, and none starts once nothing is.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TypedDict


class QueryTimeoutError(TimeoutError):
    """A read ran past its time limit and the database stopped it.

    Attributes:
        timeout_s: The limit, in seconds, that was exceeded.
    """

    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = timeout_s
        super().__init__(f"the read exceeded its time limit of {timeout_s:g} s")

    def __reduce__(self) -> tuple[type[QueryTimeoutError], tuple[float]]:
        return type(self), (self.timeout_s,)


def check_timeout(timeout_s: float | None) -> float | None:
    """*timeout_s* if it is a usable limit, ``None`` for none.

    Raises:
        ValueError: The limit is not a finite number of seconds above zero.
            Several drivers read zero as "no limit", so it is refused rather
            than passed on.
    """
    if timeout_s is None:
        return None
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError(
            f"timeout_s must be a finite number of seconds above zero, got {timeout_s!r}"
        )
    return float(timeout_s)


class TimeoutKwargs(TypedDict, total=False):
    """The ``timeout_s`` keyword of a read, present only when there is a limit."""

    timeout_s: float


def timeout_kwargs(timeout_s: float | None) -> TimeoutKwargs:
    """``{"timeout_s": timeout_s}``, or nothing when there is no limit.

    Passing the keyword only when it is set keeps a call without a limit
    exactly as it was.
    """
    return {} if timeout_s is None else {"timeout_s": timeout_s}


class Deadline:
    """One time limit spent across every read of a call.

    Args:
        timeout_s: Seconds the whole call may take; ``None`` for no limit.
        clock: Monotonic clock in seconds; :func:`time.monotonic` by default.

    Raises:
        ValueError: As :func:`check_timeout`.
    """

    def __init__(
        self,
        timeout_s: float | None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.timeout_s = check_timeout(timeout_s)
        self._clock = clock or time.monotonic
        self._end = None if self.timeout_s is None else self._clock() + self.timeout_s

    @property
    def bounded(self) -> bool:
        """Whether the call has a limit at all."""
        return self.timeout_s is not None

    def remaining(self) -> float | None:
        """Seconds left for the next read; ``None`` when there is no limit.

        Raises:
            QueryTimeoutError: Nothing is left, so no further read may start.
        """
        if self._end is None or self.timeout_s is None:
            return None
        left = self._end - self._clock()
        if left <= 0:
            raise QueryTimeoutError(self.timeout_s)
        return left

    def kwargs(self) -> TimeoutKwargs:
        """:func:`timeout_kwargs` of :meth:`remaining`, for the next read."""
        return timeout_kwargs(self.remaining())

    @contextmanager
    def spent(self) -> Generator[None]:
        """Report a timeout raised inside the block as this deadline's.

        A read handed the remainder of the deadline times out on that
        remainder; the caller set the whole limit, so that is the one named.
        """
        try:
            yield
        except QueryTimeoutError as error:
            if self.timeout_s is None or error.timeout_s == self.timeout_s:
                raise
            raise QueryTimeoutError(self.timeout_s) from error
