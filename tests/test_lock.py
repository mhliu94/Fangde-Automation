import tempfile
import unittest
import threading
import time
from pathlib import Path

from fangde.errors import SafetyError
from fangde.lock import EmulatorLock, fair_thread_lock, _thread_gates


class LockTests(unittest.TestCase):
    def test_waiting_broker_request_precedes_poller_reacquisition(self):
        key = object()
        events = []
        def waiter():
            with fair_thread_lock(key):
                events.append('holdings')
        with fair_thread_lock(key):
            thread = threading.Thread(target=waiter)
            thread.start()
            # Wait until the competing request has entered the admission queue.
            deadline = time.monotonic() + 2
            while len(_thread_gates[key][1]) < 2 and time.monotonic() < deadline:
                time.sleep(0.001)
            self.assertEqual(len(_thread_gates[key][1]), 2)
        with fair_thread_lock(key):
            events.append('next_poll')
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(events, ['holdings', 'next_poll'])

    def test_timed_out_waiter_does_not_block_later_requests(self):
        key = object()
        with fair_thread_lock(key):
            with self.assertRaises(SafetyError):
                with fair_thread_lock(key, timeout=0):
                    pass
        with fair_thread_lock(key, timeout=0):
            pass

    def test_second_controller_times_out(self):
        path = Path(tempfile.mkdtemp()) / "emulator.lock"
        with EmulatorLock(path, 0.1):
            with self.assertRaises(SafetyError):
                with EmulatorLock(path, 0.05):
                    pass


if __name__ == "__main__":
    unittest.main()
