from pymongo.helpers_shared import async_whoami
from pymongo.lock import _async_create_condition, _async_cond_wait
from contextlib import asynccontextmanager


class AsyncRWLock:
    """Non re-entrant, no upgrade/downgrade RAII-based rw Lock"""
    def __init__(self, lock):
        self._mutex = lock
        self._read_cond = _async_create_condition(self._mutex)
        self._write_cond = _async_create_condition(self._mutex)
        self._active_writer = None
        self._active_readers = set()
        self._waiting_writers = 0

    async def acquire_read(self):
        """Acquires the read lock"""
        async with self._read_cond:
            if self._active_writer == async_whoami():
                raise RuntimeError("Lock downgrades not supported")
            if async_whoami() in self._active_readers:
                raise RuntimeError("Re-entrant read lock call")
            # Give writers preference
            while self._active_writer is not None or self._waiting_writers > 0:
                await _async_cond_wait(self._read_cond, None)

            self._active_readers.add(async_whoami())

    async def release_read(self):
        """Releases the read lock"""
        async with self._read_cond:
            if async_whoami() not in self._active_readers:
                raise RuntimeError("Release called for read lock not being held")
            
            self._active_readers.remove(async_whoami())

            # Last reader wakes writers
            if len(self._active_readers) == 0 and self._waiting_writers > 0:
                self._write_cond.notify()

    async def acquire_write(self):
       """Acquires the write lock"""
       async with self._write_cond:
            if async_whoami() in self._active_readers:
                raise RuntimeError("Lock upgrades not supported")
            if self._active_writer == async_whoami():
                raise RuntimeError("Re-entrant write lock call")
            
            try:
                self._waiting_writers += 1

                while self._active_writer is not None or len(self._active_readers) > 0:
                    await _async_cond_wait(self._write_cond, None)

                self._active_writer = async_whoami()
            finally:
                self._waiting_writers -= 1
                # if this task was cancelled after incrementing _waiting_writer but 
                # before making itself the active writer, notify waiters
                if self._active_writer is None:
                    # Give preference to waiting writers
                    if self._waiting_writers > 0:
                        self._write_cond.notify()
                    # if this was the last waiter, signal readers
                    else:
                        self._read_cond.notify_all()


    async def release_write(self):
        """Releases the write lock"""
        async with self._write_cond:
            if self._active_writer != async_whoami():
                raise RuntimeError("Release called for write lock not held")
            
            self._active_writer = None

            # Give preference to waiting writers
            if self._waiting_writers > 0:
                self._write_cond.notify()
                return
            
            # Notify readers if there are no waiting writers
            self._read_cond.notify_all()

    @asynccontextmanager
    async def read_lock(self):
        """Context manager for read lock"""
        await self.acquire_read()
        try:
            yield
        finally:
            await self.release_read()

    @asynccontextmanager
    async def write_lock(self):
        """Context manager for write lock"""
        await self.acquire_write()
        try:
            yield
        finally:
            await self.release_write()
