from __future__ import annotations

from contextlib import contextmanager

from pymongo.helpers_shared import whoami
from pymongo.lock import _cond_wait, _create_condition

_IS_SYNC = True


class RWLock:
    """Non re-entrant, no upgrade/downgrade RAII-based rw Lock"""

    def __init__(self, lock):
        self._mutex = lock
        self._read_cond = _create_condition(self._mutex)
        self._write_cond = _create_condition(self._mutex)
        self._active_writer = None
        self._active_readers = set()
        self._waiting_writers = 0

    def acquire_read(self):
        """Acquires the read lock"""
        with self._read_cond:
            if self._active_writer == whoami():
                raise RuntimeError("Lock downgrades not supported")
            if whoami() in self._active_readers:
                raise RuntimeError("Re-entrant read lock call")
            # Give writers preference
            while self._active_writer is not None or self._waiting_writers > 0:
                _cond_wait(self._read_cond, None)

            self._active_readers.add(whoami())

    def release_read(self):
        """Releases the read lock"""
        with self._read_cond:
            if whoami() not in self._active_readers:
                raise RuntimeError("Release called for read lock not being held")

            self._active_readers.remove(whoami())

            # Last reader wakes writers
            if len(self._active_readers) == 0 and self._waiting_writers > 0:
                self._write_cond.notify()

    def acquire_write(self):
        """Acquires the write lock"""
        with self._write_cond:
            if whoami() in self._active_readers:
                raise RuntimeError("Lock upgrades not supported")
            if self._active_writer == whoami():
                raise RuntimeError("Re-entrant write lock call")

            try:
                self._waiting_writers += 1

                while self._active_writer is not None or len(self._active_readers) > 0:
                    _cond_wait(self._write_cond, None)

                self._active_writer = whoami()
            finally:
                self._waiting_writers -= 1
                # if this task was cancelled after incrementing _waiting_writer but
                # before making itself the active writer, notify witers
                if self._active_writer is None:
                    # Give preference to waiting writers
                    if self._waiting_writers > 0:
                        self._write_cond.notify()
                    # if this was the last witer, signal readers
                    else:
                        self._read_cond.notify_all()

    def release_write(self):
        """Releases the write lock"""
        with self._write_cond:
            if self._active_writer != whoami():
                raise RuntimeError("Release called for write lock not held")

            self._active_writer = None

            # Give preference to waiting writers
            if self._waiting_writers > 0:
                self._write_cond.notify()
                return

            # Notify readers if there are no waiting writers
            self._read_cond.notify_all()

    @contextmanager
    def read_lock(self):
        """Context manager for read lock"""
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self):
        """Context manager for write lock"""
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()
