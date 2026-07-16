
import unittest
from threading import Event, Thread
from test import UnitTest
from pymongo.synchronous.rwlock import RWLock
from pymongo.lock import _create_lock


TIMEOUT = 1.0

class TestRWLock(UnitTest):
    """Test class for RWLock"""

    class _TestError(Exception):
        pass

    def setUp(self):
        self.lock = RWLock(_create_lock())

    def _optional_start_event(self, start_ev):
        """Set a start event if provided"""
        if start_ev is not None:
            start_ev.set()

    def _reader_task(self, acquire_ev, release_ev, start_ev=None):
        """Task for test readers"""

        self._optional_start_event(start_ev)

        with self.lock.read_lock():
            acquire_ev.set()
            release_ev.wait()

    def _writer_task(self, acquire_ev, release_ev, start_ev=None):
        """Task for test writers"""

        self._optional_start_event(start_ev)

        with self.lock.write_lock():
            acquire_ev.set()
            release_ev.wait()

    def test_multiple_concurrent_readers(self):
        """Tests that multiple concurrent readers can get the lock"""
        reader1_acquire_ev = Event()
        reader1_release_ev = Event()
        reader2_acquire_ev = Event()
        reader2_release_ev = Event()

        # Start one reader and verify it gets the lock
        reader1 = Thread(target=self._reader_task, 
                                   args=(reader1_acquire_ev, reader1_release_ev,))
        reader1.start()
        self.assertTrue(reader1_acquire_ev.wait(timeout=TIMEOUT))

        # Start another reader and verify that it also gets the lock
        reader2 = Thread(target=self._reader_task, 
                                   args=(reader2_acquire_ev, reader2_release_ev,))
        reader2.start()
        self.assertTrue(reader2_acquire_ev.wait(timeout=TIMEOUT))

        # End
        reader1_release_ev.set()
        reader2_release_ev.set()
        reader1.join()
        reader2.join()

    def test_writer_excludes_readers(self):
        """Tests that an active writer excludes readers"""
        writer_acquire_ev = Event() 
        writer_release_ev = Event()
        reader_acquire_ev = Event()
        reader_release_ev = Event() 
        reader_start_ev = Event()

        # Start a writer and verify that it gets the lock 
        writer = Thread(target=self._writer_task, args=(writer_acquire_ev, writer_release_ev,))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Start a reader 
        reader = Thread(target=self._reader_task, 
                                  args=(reader_acquire_ev, reader_release_ev, reader_start_ev,))
        reader.start()
        # Verify the reader starts, but doesn't get the lock
        self.assertTrue(reader_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(reader_acquire_ev.is_set())

        # Release the writer
        writer_release_ev.set() 

        # Verify the reader now gets the lock
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))
        
        # End
        reader_release_ev.set()
        writer.join()
        reader.join()

    def test_writer_excludes_writers(self):
        """Tests that an active writer excludes other writers"""
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        waiter_start_ev = Event()
        waiter_acquire_ev = Event()
        waiter_release_ev = Event()

        # Start a writer and wait till they get the lock
        writer = Thread(target=self._writer_task, 
                                  args=(writer_acquire_ev, writer_release_ev,))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Start another writer
        waiter = Thread(target=self._writer_task, 
                                  args=(waiter_acquire_ev, waiter_release_ev, waiter_start_ev,))
        waiter.start()
        # Verify this waiter starts but waits
        self.assertTrue(waiter_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(waiter_acquire_ev.is_set())
        
        # Release the first writer
        writer_release_ev.set()
        # Verify the waiter now gets the lock
        self.assertTrue(waiter_acquire_ev.wait(timeout=TIMEOUT))

        # End
        waiter_release_ev.set()
        writer.join()
        waiter.join()

    def test_write_after_last_reader_release_succeeds(self):
        """Tests that a write succeeds after the last reader completes"""
        reader_acquire_ev = Event()
        reader_release_ev = Event()
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        writer_start_ev = Event()

        # Start a reader and verify they get the lock
        reader = Thread(target=self._reader_task, 
                                  args=(reader_acquire_ev, reader_release_ev,))
        reader.start()
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))

        # Start a writer
        writer = Thread(target=self._writer_task, 
                                  args=(writer_acquire_ev, writer_release_ev, writer_start_ev,))
        writer.start()
        # Verify the writer starts but waits
        self.assertTrue(writer_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(writer_acquire_ev.is_set())

        # Release the reader
        reader_release_ev.set()
        # Verify the writer gets the lock
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # End
        writer_release_ev.set()
        reader.join()
        writer.join()

    def test_writer_preference(self):
        """Tests that writers are given preference when readers and writers are waiting"""
        writer_acquire_ev = Event() 
        writer_release_ev = Event()
        read_waiter_start_ev = Event()
        read_waiter_acquire_ev = Event()
        read_waiter_release_ev = Event()
        write_waiter_start_ev = Event()
        write_waiter_acquire_ev = Event()
        write_waiter_release_ev = Event()

        # Start a writer and wait till they get the lock
        writer = Thread(target=self._writer_task, 
                                  args=(writer_acquire_ev, writer_release_ev,))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Start a reader and verify that it starts and waits
        read_waiter = Thread(target=self._reader_task, args=(
            read_waiter_acquire_ev, read_waiter_release_ev, read_waiter_start_ev))
        read_waiter.start()
        self.assertTrue(read_waiter_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(read_waiter_acquire_ev.is_set())

        # Start another writer and verify that it starts and waits
        writer_waiter = Thread(
            target=self._writer_task, 
            args=(write_waiter_acquire_ev, write_waiter_release_ev, write_waiter_start_ev))
        writer_waiter.start()
        self.assertTrue(write_waiter_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(write_waiter_acquire_ev.is_set())

        # Signal the writer to release the lock
        writer_release_ev.set() 

        # Verify that the waiting writer gets the lock, not the reader
        self.assertTrue(write_waiter_acquire_ev.wait(timeout=TIMEOUT))
        self.assertFalse(read_waiter_acquire_ev.is_set())

        #  Verify the reader gets the lock if the second writer exits
        write_waiter_release_ev.set()
        self.assertTrue(read_waiter_acquire_ev.wait(timeout=TIMEOUT))

        # End
        read_waiter_release_ev.set()
        writer.join()
        writer_waiter.join()
        read_waiter.join()

    def test_reentrant_read(self):
        """Tests that an error is raised if a re-entrant read call is made"""
        with self.lock.read_lock():
            with self.assertRaises(RuntimeError):
                self.lock.acquire_read()
                
    def test_reentrant_write(self):
        """Tests that an error is raised if a re-entrant write call is made"""
        with self.lock.write_lock():
            with self.assertRaises(RuntimeError):
                self.lock.acquire_write()

    def test_read_write_upgrade(self):
        """Tests that an error is raised if an attempt is made to upgrade from read to write"""
        with self.lock.read_lock():
            with self.assertRaises(RuntimeError):
                self.lock.acquire_write()

    def test_write_read_downgrade(self):
        """Tests that an error is raised if an attempt is made to downgrade from write to read"""
        with self.lock.write_lock():
            with self.assertRaises(RuntimeError):
                self.lock.acquire_read()

    def test_unheld_read_release(self):
        """Tests that an error is raised if an unheld read lock is released"""
        with self.assertRaises(RuntimeError):
            self.lock.release_read()

    def test_wrong_owner_write_release(self):
        """Tests that an error is raised if somebody other than the current owner tries to release"""
        writer_acquire_ev = Event()
        writer_release_ev = Event()

        # Start a writer and verify they hold the lock
        writer = Thread(target=self._writer_task,
                                   args=(writer_acquire_ev, writer_release_ev))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Try to release the lock 
        with self.assertRaises(RuntimeError):
            self.lock.release_write()

        # End
        writer_release_ev.set()
        writer.join()

    def test_unheld_write_release(self):
        """Tests that an error is raised if an unheld write lock is released"""
        with self.assertRaises(RuntimeError):
            self.lock.release_write()

    def test_wrong_owner_read_release(self):
        """Tests that an error is raised if a different reader tries to release"""
        reader_acquire_ev = Event()
        reader_release_ev = Event()

        # Start a reader and verify they hold the lock
        reader = Thread(target=self._reader_task,
                                  args=(reader_acquire_ev, reader_release_ev))
        reader.start()
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))

        # Try to release the lock
        with self.assertRaises(RuntimeError):
            self.lock.release_read()
        
        # End
        reader_release_ev.set()
        reader.join()

    def test_read_context_manager_release(self):
        """Tests that the lock is released  when using the read context manager"""
        # acquire the read lock with context manager
        reader_acquire_ev = Event()
        reader_release_ev = Event()
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        writer_start_ev = Event()

        # Start a reader and verify that it holds the lock
        reader = Thread(target=self._reader_task,
                                  args=(reader_acquire_ev, reader_release_ev))
        reader.start()
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))

        # Start a writer and verify it starts but doesn't acquire
        writer = Thread(target=self._writer_task, 
                                  args=(writer_acquire_ev, writer_release_ev, writer_start_ev))
        writer.start()
        self.assertTrue(writer_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(writer_acquire_ev.is_set())
        
        # Release the reader and verify writer acquires
        reader_release_ev.set()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # End
        writer_release_ev.set()
        writer.join()
        reader.join()

    def test_write_context_manager_release(self):
        """Tests that the lock is released when using the write context manager"""
        reader_acquire_ev = Event()
        reader_release_ev = Event()
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        reader_start_ev = Event()

        # Start a reader and verify that it holds the lock
        writer = Thread(target=self._writer_task,
                                  args=(writer_acquire_ev, writer_release_ev))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Start a writer and verify it starts but doesn't acquire
        reader = Thread(target=self._reader_task, 
                                  args=(reader_acquire_ev, reader_release_ev, reader_start_ev))
        reader.start()
        self.assertTrue(reader_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(reader_acquire_ev.is_set())
        
        # Release the reader and verify writer acquires
        writer_release_ev.set()
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))

        # End
        reader_release_ev.set()
        writer.join()
        reader.join()

    def test_read_context_manager_release_on_exception(self):
        """Tests that the lock is released  when using the read context manager"""
        reader_acquire_ev = Event()
        reader_release_ev = Event()
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        writer_start_ev = Event()
        exception = None

        def reader_task(acquire_ev, release_ev):
            nonlocal exception
            try:
                with self.lock.read_lock():
                    acquire_ev.set()
                    release_ev.wait()
                    raise self._TestError()
            except self._TestError as e:
                exception = e
            
        # Start a reader and wait for it to acquire
        reader = Thread(target=reader_task, args=(reader_acquire_ev, reader_release_ev))
        reader.start()
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))

        # Start a writer and verify it starts but doesn't acquire
        writer = Thread(target=self._writer_task, 
                                  args=(writer_acquire_ev, writer_release_ev, writer_start_ev))
        writer.start()
        self.assertTrue(writer_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(writer_acquire_ev.is_set())

        # Release the reader and confirm exception is raised before exit
        reader_release_ev.set()
        reader.join()
        self.assertIsInstance(exception, self._TestError)
            
        # Verify the writer can now get the lock
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))
        
        # End
        writer_release_ev.set()
        writer.join()

    def test_write_context_manager_release_on_exception(self):
        """Tests that the lock is released when using the write context manager"""
        reader_acquire_ev = Event()
        reader_release_ev = Event()
        writer_acquire_ev = Event()
        writer_release_ev = Event()
        reader_start_ev = Event()
        exception = None

        def writer_task(acquire_ev, release_ev):
            nonlocal exception
            try:
                with self.lock.write_lock():
                    acquire_ev.set()
                    release_ev.wait()
                    raise self._TestError()
            except self._TestError as e:
                exception = e
            
        # Start a writer and wait for it to acquire
        writer = Thread(target=writer_task, args=(writer_acquire_ev, writer_release_ev))
        writer.start()
        self.assertTrue(writer_acquire_ev.wait(timeout=TIMEOUT))

        # Start a writer and verify it starts but doesn't acquire
        reader = Thread(target=self._reader_task, 
                                  args=(reader_acquire_ev, reader_release_ev, reader_start_ev))
        reader.start()
        self.assertTrue(reader_start_ev.wait(timeout=TIMEOUT))
        self.assertFalse(reader_acquire_ev.is_set())

        # Release the reader and confirm exception is raised before exit
        writer_release_ev.set()
        writer.join()
        self.assertIsInstance(exception, self._TestError)
            
        # Verify the reader can now get the lock
        self.assertTrue(reader_acquire_ev.wait(timeout=TIMEOUT))
        
        # End
        reader_release_ev.set()
        reader.join()

