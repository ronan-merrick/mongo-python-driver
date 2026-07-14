import threading
import unittest
from pymongo.synchronous.rwlock import RWLock
from pymongo.lock import _create_lock
from test import UnitTest

TIMEOUT = 1.0



class TestRWLock(UnitTest):
    """Test class for AsyncRWLock"""

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
        pass

    def _writer_task(self, acquire_ev, release_ev, start_ev=None):
        """Task for test writers"""
        pass 

    def test_multiple_concurrent_readers(self):
        """Tests that multiple concurrent readers can get the lock"""
        pass

    def test_writer_excludes_readers(self):
        """Tests that an active writer excludes readers"""
        pass

    def test_writer_excludes_writers(self):
        """Tests that an active writer excludes other writers"""
        pass

    def test_write_after_last_reader_release_succeeds(self):
        """Tests that a write succeeds after the last reader completes"""
        pass

    def test_writer_preference(self):
        """Tests that writers are given preference when readers and writers are waiting"""
        pass 

    def test_reentrant_read(self):
        """Tests that an error is raised if a re-entrant read call is made"""
        pass

    def test_reentrant_write(self):
        """Tests that an error is raised if a re-entrant write call is made"""
        pass

    def test_read_write_upgrade(self):
        """Tests that an error is raised if an attempt is made to upgrade from read to write"""
        pass

    def test_write_read_downgrade(self):
        """Tests that an error is raised if an attempt is made to downgrade from write to read"""
        pass

    def test_unheld_read_release(self):
        """Tests that an error is raised if an unheld read lock is released"""
        pass

    def test_wrong_owner_write_release(self):
        """Tests that an error is raised if somebody other than the current owner tries to release"""
        pass

    def test_unheld_write_release(self):
        """Tests that an error is raised if an unheld write lock is released"""
        pass

    def test_wrong_owner_read_release(self):
        """Tests that an error is raised if a different reader tries to release"""
        pass

    def test_read_context_manager_release(self):
        """Tests that the lock is released  when using the read context manager"""
        # acquire the read lock with context manager
        pass

    def test_write_context_manager_release(self):
        """Tests that the lock is released when using the write context manager"""
        pass

    def test_read_context_manager_release_on_exception(self):
        """Tests that the lock is released  when using the read context manager"""
        pass

    def test_write_context_manager_release_on_exception(self):
        """Tests that the lock is released when using the write context manager"""
        pass

    def test_cancelled_write_waiter_no_phantom_waiters(self):
        """Tests that a cancelled write waiter decrements _waiting_writers"""
        pass

    def test_cancelled_write_holder(self):
        """Tests that a cancelled write task releases the lock"""
        pass

    def test_cancelled_reader_holder(self):
        """Tests that a cancelled write task releases the lock"""
        pass