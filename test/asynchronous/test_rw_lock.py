from __future__ import annotations

import asyncio
import unittest

from pymongo.asynchronous.rwlock import AsyncRWLock
from test.asynchronous import AsyncUnitTest

TIMEOUT = 1.0


class TestRWLock(AsyncUnitTest):
    """Test class for AsyncRWLock"""

    class _TestError(Exception):
        pass

    def setUp(self):
        self.lock = AsyncRWLock()

    def _optional_start_event(self, start_ev):
        """Set a start event if provided"""
        if start_ev is not None:
            start_ev.set()

    async def _reader_task(self, acquire_ev, release_ev, start_ev=None):
        """Task for test readers"""

        self._optional_start_event(start_ev)

        async with self.lock.read_lock():
            acquire_ev.set()
            await release_ev.wait()

    async def _writer_task(self, acquire_ev, release_ev, start_ev=None):
        """Task for test writers"""

        self._optional_start_event(start_ev)

        async with self.lock.write_lock():
            acquire_ev.set()
            await release_ev.wait()

    async def test_multiple_concurrent_readers(self):
        """Tests that multiple concurrent readers can get the lock"""
        reader1_acquire_ev = asyncio.Event()
        reader2_acquire_ev = asyncio.Event()
        release_readers_ev = asyncio.Event()

        reader1 = asyncio.create_task(self._reader_task(reader1_acquire_ev, release_readers_ev))
        reader2 = asyncio.create_task(self._reader_task(reader2_acquire_ev, release_readers_ev))

        # Verify both readers got the lock
        await asyncio.wait_for(reader1_acquire_ev.wait(), timeout=TIMEOUT)
        await asyncio.wait_for(reader2_acquire_ev.wait(), timeout=TIMEOUT)

        # Verify there are two active readers
        self.assertEqual(len(self.lock._active_readers), 2)
        # Release the readers
        release_readers_ev.set()

        await asyncio.gather(reader1, reader2)

    async def test_writer_excludes_readers(self):
        """Tests that an active writer excludes readers"""
        reader_acquire_ev = asyncio.Event()
        reader_started_ev = asyncio.Event()
        writer_acquire_ev = asyncio.Event()
        release_writer = asyncio.Event()
        release_reader = asyncio.Event()

        # Start a writer and make sure they get the lock
        writer = asyncio.create_task(self._writer_task(writer_acquire_ev, release_writer))
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # Start a reader and verify that they don't get the lock while the writer holds it
        reader = asyncio.create_task(
            self._reader_task(reader_acquire_ev, release_reader, reader_started_ev)
        )
        # Verify that the reader has started
        await asyncio.wait_for(reader_started_ev.wait(), timeout=TIMEOUT)
        # Give the reader another chance to run and then verify that it hasn't got the lock
        await asyncio.sleep(0)
        self.assertFalse(reader_acquire_ev.is_set())

        # Release the writer and verify that the reader succeeds
        release_writer.set()
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)
        release_reader.set()

        await asyncio.gather(reader, writer)

    async def test_writer_excludes_writers(self):
        """Tests that an active writer excludes other writers"""
        writer1_acquire_ev = asyncio.Event()
        writer2_acquire_ev = asyncio.Event()
        writer1_release_ev = asyncio.Event()
        writer2_release_ev = asyncio.Event()
        writer2_start_ev = asyncio.Event()

        # Start the first writer and verify it has the lock
        writer1 = asyncio.create_task(self._writer_task(writer1_acquire_ev, writer1_release_ev))
        await asyncio.wait_for(writer1_acquire_ev.wait(), timeout=TIMEOUT)

        #  Start the second writer
        writer2 = asyncio.create_task(
            self._writer_task(writer2_acquire_ev, writer2_release_ev, writer2_start_ev)
        )
        await asyncio.wait_for(writer2_start_ev.wait(), timeout=TIMEOUT)
        # Yield control and give the second writer another potential chance to run
        await asyncio.sleep(0)
        # Verify the second writer didn't get the lock
        self.assertFalse(writer2_acquire_ev.is_set())

        # Release the first writer
        writer1_release_ev.set()
        # Wait for the second writer to get the lock
        await asyncio.wait_for(writer2_acquire_ev.wait(), timeout=TIMEOUT)
        # Release the second writer
        writer2_release_ev.set()

        await asyncio.gather(writer1, writer2)

    async def test_write_after_last_reader_release_succeeds(self):
        """Tests that a write succeeds after the last reader completes"""
        reader_acquire_ev = asyncio.Event()
        writer_acquire_ev = asyncio.Event()
        writer_start_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()

        reader = asyncio.create_task(self._reader_task(reader_acquire_ev, reader_release_ev))
        writer = asyncio.create_task(
            self._writer_task(writer_acquire_ev, writer_release_ev, writer_start_ev)
        )

        # Verify the reader has the lock
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # Verify the writer has started
        await asyncio.wait_for(writer_start_ev.wait(), timeout=TIMEOUT)
        # Yield and give the writer one more chance to run
        await asyncio.sleep(0)
        # Verify the writer was not able to get the lock
        self.assertFalse(writer_acquire_ev.is_set())

        # Signal the reader to release the lock
        reader_release_ev.set()
        # Verify that the writer gets the lock
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)
        # Release the writer
        writer_release_ev.set()

        await asyncio.gather(reader, writer)

    async def test_writer_preference(self):
        """Tests that writers are given preference when readers and writers are waiting"""
        writer1_acquire_ev = asyncio.Event()
        writer2_acquire_ev = asyncio.Event()
        reader1_acquire_ev = asyncio.Event()
        reader2_acquire_ev = asyncio.Event()
        writer2_start_ev = asyncio.Event()
        reader1_start_ev = asyncio.Event()
        reader2_start_ev = asyncio.Event()
        readers_release_ev = asyncio.Event()
        writer_1_release_ev = asyncio.Event()
        writer_2_release_ev = asyncio.Event()

        # Start a writer and verify that it gets the lock
        writer1 = asyncio.create_task(self._writer_task(writer1_acquire_ev, writer_1_release_ev))
        await asyncio.wait_for(writer1_acquire_ev.wait(), timeout=TIMEOUT)

        # Start two more readers
        reader1 = asyncio.create_task(
            self._reader_task(reader1_acquire_ev, readers_release_ev, reader1_start_ev)
        )
        reader2 = asyncio.create_task(
            self._reader_task(reader2_acquire_ev, readers_release_ev, reader2_start_ev)
        )

        # Verify that they start
        await asyncio.wait_for(reader1_start_ev.wait(), timeout=TIMEOUT)
        await asyncio.wait_for(reader2_start_ev.wait(), timeout=TIMEOUT)

        # Verify that they don't get the lock
        self.assertFalse(reader1_acquire_ev.is_set())
        self.assertFalse(reader2_acquire_ev.is_set())

        # Start another writer
        writer2 = asyncio.create_task(
            self._writer_task(writer2_acquire_ev, writer_2_release_ev, writer2_start_ev)
        )
        # Verify that it starts and doesn't get the lock
        await asyncio.wait_for(writer2_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(writer2_acquire_ev.is_set())

        # Release the first writer
        writer_1_release_ev.set()

        # Verify that the second writer gets the lock
        await asyncio.wait_for(writer2_acquire_ev.wait(), timeout=TIMEOUT)
        # Verify that the two readers do not get the lock
        self.assertFalse(reader1_acquire_ev.is_set())
        self.assertFalse(reader2_acquire_ev.is_set())

        # Release writer 2 and verify that the readers get the lock
        writer_2_release_ev.set()
        await asyncio.wait_for(reader1_acquire_ev.wait(), timeout=TIMEOUT)
        await asyncio.wait_for(reader2_acquire_ev.wait(), timeout=TIMEOUT)

        # End the test
        readers_release_ev.set()
        await asyncio.gather(writer1, writer2, reader1, reader2)

    async def test_reentrant_read(self):
        """Tests that an error is raised if a re-entrant read call is made"""
        async with self.lock.read_lock():
            with self.assertRaises(RuntimeError):
                async with self.lock.read_lock():
                    pass

    async def test_reentrant_write(self):
        """Tests that an error is raised if a re-entrant write call is made"""
        async with self.lock.write_lock():
            with self.assertRaises(RuntimeError):
                async with self.lock.write_lock():
                    pass

    async def test_read_write_upgrade(self):
        """Tests that an error is raised if an attempt is made to upgrade from read to write"""
        async with self.lock.read_lock():
            with self.assertRaises(RuntimeError):
                async with self.lock.write_lock():
                    pass

    async def test_write_read_downgrade(self):
        """Tests that an error is raised if an attempt is made to downgrade from write to read"""
        async with self.lock.write_lock():
            with self.assertRaises(RuntimeError):
                async with self.lock.read_lock():
                    pass

    async def test_unheld_read_release(self):
        """Tests that an error is raised if an unheld read lock is released"""
        with self.assertRaises(RuntimeError):
            await self.lock.release_read()

    async def test_wrong_owner_write_release(self):
        """Tests that an error is raised if somebody other than the current owner tries to release"""

        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()

        # Start a writer and wait for it to get the lock
        writer = asyncio.create_task(self._writer_task(writer_acquire_ev, writer_release_ev))
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # Now try to release the lock
        with self.assertRaises(RuntimeError):
            await self.lock.release_write()

        # Release and wait for the writer
        writer_release_ev.set()
        await asyncio.gather(writer)

    async def test_unheld_write_release(self):
        """Tests that an error is raised if an unheld write lock is released"""
        with self.assertRaises(RuntimeError):
            await self.lock.release_write()

    async def test_wrong_owner_read_release(self):
        """Tests that an error is raised if a different reader tries to release"""
        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()

        # Start a reader and wait for it to get the lock
        reader = asyncio.create_task(self._reader_task(reader_acquire_ev, reader_release_ev))
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # Now try to release the read lock
        with self.assertRaises(RuntimeError):
            await self.lock.release_read()

        reader_release_ev.set()
        await asyncio.gather(reader)

    async def test_read_context_manager_release(self):
        """Tests that the lock is released  when using the read context manager"""
        # acquire the read lock with context manager
        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        writer_start_ev = asyncio.Event()
        reader_exit_context_ev = asyncio.Event()

        async def reader_task(acquire_ev, release_ev, exit_context_ev):
            """Task for test readers"""

            async with self.lock.read_lock():
                acquire_ev.set()
                await release_ev.wait()

            exit_context_ev.set()

        # Start a reader and let them get the lock
        reader = asyncio.create_task(
            reader_task(reader_acquire_ev, reader_release_ev, reader_exit_context_ev)
        )
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # Start a writer and verify they started but don't have the lock
        writer = asyncio.create_task(
            self._writer_task(writer_acquire_ev, writer_release_ev, writer_start_ev)
        )
        await asyncio.wait_for(writer_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(writer_acquire_ev.is_set())
        self.assertFalse(reader_exit_context_ev.is_set())

        # Release the reader and verify that they leave the context manager
        reader_release_ev.set()
        await asyncio.wait_for(reader_exit_context_ev.wait(), timeout=TIMEOUT)

        # Now check if the writer gets the lock
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # Release the writer
        writer_release_ev.set()

        await asyncio.gather(reader, writer)

    async def test_write_context_manager_release(self):
        """Tests that the lock is released when using the write context manager"""
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        reader_start_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()
        reader_acquire_ev = asyncio.Event()
        writer_exit_context_ev = asyncio.Event()

        async def writer_task(acquire_ev, release_ev, exit_context_ev):

            async with self.lock.write_lock():
                acquire_ev.set()
                await release_ev.wait()

            exit_context_ev.set()

        # Start a writer and wait for it to get the lock
        writer = asyncio.create_task(
            writer_task(writer_acquire_ev, writer_release_ev, writer_exit_context_ev)
        )
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # While the writer has the lock start a reader
        reader = asyncio.create_task(
            self._reader_task(reader_acquire_ev, reader_release_ev, reader_start_ev)
        )

        # Wait for the reader to start
        await asyncio.wait_for(reader_start_ev.wait(), timeout=TIMEOUT)

        # Verify it didn't get the lock
        self.assertFalse(reader_acquire_ev.is_set())
        # Verify the writer hasn't left the context manager
        self.assertFalse(writer_exit_context_ev.is_set())

        # Release the writer and wait for it to leave the context manager
        writer_release_ev.set()
        await asyncio.wait_for(writer_exit_context_ev.wait(), timeout=TIMEOUT)

        # Verify that the reader gets the lock
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # Release the reader
        reader_release_ev.set()

        await asyncio.gather(reader, writer)

    async def test_read_context_manager_release_on_exception(self):
        """Tests that the lock is released  when using the read context manager"""

        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        writer_start_ev = asyncio.Event()

        async def reader_task(acquire_ev, release_ev):
            async with self.lock.read_lock():
                acquire_ev.set()
                await release_ev.wait()
                raise self._TestError()

        # start a reader and verify it gets the lock
        reader = asyncio.create_task(reader_task(reader_acquire_ev, reader_release_ev))
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # start a writer and verify it starts but doesn't get the lock
        writer = asyncio.create_task(
            self._writer_task(writer_acquire_ev, writer_release_ev, writer_start_ev)
        )

        await asyncio.wait_for(writer_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(writer_acquire_ev.is_set())

        # Release the reader and verify that an exception is thrown
        reader_release_ev.set()
        with self.assertRaises(self._TestError):
            await reader

        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        writer_release_ev.set()

        await writer

    async def test_write_context_manager_release_on_exception(self):
        """Tests that the lock is released when using the write context manager"""
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        reader_start_ev = asyncio.Event()
        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()

        async def writer_task(acquire_ev, release_ev):
            async with self.lock.write_lock():
                acquire_ev.set()
                await release_ev.wait()
                raise self._TestError()

        # Start a writer and wait for them to acquire the lock
        writer = asyncio.create_task(writer_task(writer_acquire_ev, writer_release_ev))

        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # Now start a reader
        reader = asyncio.create_task(
            self._reader_task(reader_acquire_ev, reader_release_ev, reader_start_ev)
        )

        # Confirm the reader started but did not get the lock
        await asyncio.wait_for(reader_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(reader_acquire_ev.is_set())

        # Signal the writer
        with self.assertRaises(self._TestError):
            writer_release_ev.set()
            await writer

        # Confirm that the reader gets the lock
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        # Signal the reader
        reader_release_ev.set()

        # End
        await reader

    async def test_cancelled_write_waiter_no_phantom_waiters(self):
        """Tests that a cancelled write waiter decrements _waiting_writers"""
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        waiter_acquire_ev = asyncio.Event()
        waiter_release_ev = asyncio.Event()
        waiter_start_ev = asyncio.Event()

        # Start a writer and wait for them to get the lock
        writer = asyncio.create_task(self._writer_task(writer_acquire_ev, writer_release_ev))
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        # Verify there are no waiters
        self.assertEqual(self.lock._waiting_writers, 0)

        # Start another writer and verify that they are waiting
        waiter = asyncio.create_task(
            self._writer_task(waiter_acquire_ev, waiter_release_ev, waiter_start_ev)
        )
        await asyncio.wait_for(waiter_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(waiter_acquire_ev.is_set())

        # Give the waiter another chance to run and verify the number of writers is incremented
        await asyncio.sleep(0)
        self.assertEqual(self.lock._waiting_writers, 1)

        # Now cancel the waiter
        waiter.cancel()
        await asyncio.sleep(0)

        with self.assertRaises(asyncio.CancelledError):
            await waiter

        # Verify that there are no dangling waiters
        self.assertEqual(self.lock._waiting_writers, 0)

        # Signal the writer holder
        writer_release_ev.set()
        await writer

    async def test_cancelled_write_holder(self):
        """Tests that a cancelled write task releases the lock"""
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        reader_start_ev = asyncio.Event()
        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()

        writer = asyncio.create_task(self._writer_task(writer_acquire_ev, writer_release_ev))
        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        reader = asyncio.create_task(
            self._reader_task(reader_acquire_ev, reader_release_ev, reader_start_ev)
        )
        await asyncio.wait_for(reader_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(reader_acquire_ev.is_set())

        writer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await writer

        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        reader_release_ev.set()
        await reader

    async def test_cancelled_reader_holder(self):
        """Tests that a cancelled write task releases the lock"""
        writer_acquire_ev = asyncio.Event()
        writer_release_ev = asyncio.Event()
        writer_start_ev = asyncio.Event()
        reader_acquire_ev = asyncio.Event()
        reader_release_ev = asyncio.Event()

        reader = asyncio.create_task(self._reader_task(reader_acquire_ev, reader_release_ev))
        await asyncio.wait_for(reader_acquire_ev.wait(), timeout=TIMEOUT)

        writer = asyncio.create_task(
            self._writer_task(writer_acquire_ev, writer_release_ev, writer_start_ev)
        )
        await asyncio.wait_for(writer_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(writer_acquire_ev.is_set())

        reader.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await reader

        await asyncio.wait_for(writer_acquire_ev.wait(), timeout=TIMEOUT)

        writer_release_ev.set()
        await writer

    async def test_cancelled_write_holder_waiting_writers(self):
        """Tests a cancelled write holder with waiting writers behind it"""

        reader1_acquire_ev = asyncio.Event()
        reader1_release_ev = asyncio.Event()
        reader2_acquire_ev = asyncio.Event()
        reader2_release_ev = asyncio.Event()
        reader2_start_ev = asyncio.Event()
        waiter1_acquire_ev = asyncio.Event()
        waiter1_release_ev = asyncio.Event()
        waiter1_start_ev = asyncio.Event()
        waiter2_acquire_ev = asyncio.Event()
        waiter2_release_ev = asyncio.Event()
        waiter2_start_ev = asyncio.Event()

        # Start a reader and let it get the lock
        reader1 = asyncio.create_task(self._reader_task(reader1_acquire_ev, reader1_release_ev))
        await asyncio.wait_for(reader1_acquire_ev.wait(), timeout=TIMEOUT)

        # Start a writer and let it queue behind the reader
        waiter1 = asyncio.create_task(
            self._writer_task(waiter1_acquire_ev, waiter1_release_ev, waiter1_start_ev)
        )
        await asyncio.wait_for(waiter1_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(waiter1_acquire_ev.is_set())

        # Start another writer and let it wait behind the writer
        waiter2 = asyncio.create_task(
            self._writer_task(waiter2_acquire_ev, waiter2_release_ev, waiter2_start_ev)
        )
        await asyncio.wait_for(waiter2_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(waiter2_acquire_ev.is_set())

        # Start another reader
        reader2 = asyncio.create_task(
            self._reader_task(reader2_acquire_ev, reader2_release_ev, reader2_start_ev)
        )
        await asyncio.wait_for(reader2_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(reader2_acquire_ev.is_set())

        # Cancel the first writer
        waiter1.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter1

        self.assertEqual(self.lock._waiting_writers, 1)

        # Now signal the reader
        reader1_release_ev.set()

        # Verify the other writer gets the lock
        await asyncio.wait_for(waiter2_acquire_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(reader2_acquire_ev.is_set())

        # Signal the other writer
        waiter2_release_ev.set()

        # Verify the other reader gets the lock
        await asyncio.wait_for(reader2_acquire_ev.wait(), timeout=TIMEOUT)

        # End
        reader2_release_ev.set()
        await asyncio.gather(waiter2, reader1, reader2)

    async def test_cancelled_waiter_waiting_readers(self):
        """Tests that a cancelled writer waiter with no active writer doesn't
        block readers"""

        reader1_acquire_ev = asyncio.Event()
        reader1_release_ev = asyncio.Event()
        reader2_acquire_ev = asyncio.Event()
        reader2_release_ev = asyncio.Event()
        reader2_start_ev = asyncio.Event()
        waiter_acquire_ev = asyncio.Event()
        waiter_release_ev = asyncio.Event()
        waiter_start_ev = asyncio.Event()

        # Start a reader and let it get the lock
        reader1 = asyncio.create_task(self._reader_task(reader1_acquire_ev, reader1_release_ev))
        await asyncio.wait_for(reader1_acquire_ev.wait(), timeout=TIMEOUT)

        # Start a writer and let it queue behind the reader
        waiter = asyncio.create_task(
            self._writer_task(waiter_acquire_ev, waiter_release_ev, waiter_start_ev)
        )
        await asyncio.wait_for(waiter_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(waiter_acquire_ev.is_set())

        # Start another reader
        reader2 = asyncio.create_task(
            self._reader_task(reader2_acquire_ev, reader2_release_ev, reader2_start_ev)
        )
        await asyncio.wait_for(reader2_start_ev.wait(), timeout=TIMEOUT)
        self.assertFalse(reader2_acquire_ev.is_set())

        # Cancel the first writer
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter

        self.assertEqual(self.lock._waiting_writers, 0)

        # Now signal the reader
        reader1_release_ev.set()

        # Verify the other reader gets the lock
        await asyncio.wait_for(reader2_acquire_ev.wait(), timeout=TIMEOUT)

        # End
        reader2_release_ev.set()
        await asyncio.gather(reader1, reader2)
