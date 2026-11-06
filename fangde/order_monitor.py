"""Independent broker polling and durable Kafka delivery for order changes."""
from __future__ import annotations

import logging
import threading
import time

from .api import AuthenticationRequired
from .broker import ApiTrading

LOG = logging.getLogger("fangde")


class OrderMonitor:
    def __init__(self, runner, producer):
        self.runner = runner
        self.producer = producer
        self.api = ApiTrading(runner.settings)
        self.interval = float(runner.settings.get("kafka.orderPollIntervalSeconds", 0.5))
        self.stop = threading.Event()
        self.changed = threading.Event()
        self.poll_now = threading.Event()
        self.api.on_order_observed = self.changed.set
        self.threads = []

    def start(self):
        for target, name in ((self._poll, "fangde-order-poll"), (self._deliver, "fangde-order-delivery")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            self.threads.append(thread)
            thread.start()
        LOG.info("Independent order monitoring started interval_s=%g publish_on_change=true.", self.interval)

    def wake(self):
        self.changed.set()
        self.poll_now.set()

    def _poll(self):
        next_report = 0.0
        previous_start = None
        while (not self.stop.is_set() and self.runner.running
               and not self.runner.stop_path.exists()):
            self.poll_now.clear()
            started = time.monotonic()
            try:
                result = self.api.poll_order_status()
                LOG.log(logging.INFO if started >= next_report else logging.DEBUG,
                        "Order poll completed observed=%d errors=%d elapsed_s=%.3f cadence_s=%s target_s=%g.",
                        result["observed"], result["errors"], time.monotonic() - started,
                        round(started - previous_start, 3) if previous_start is not None else None,
                        self.interval)
                if started >= next_report:
                    next_report = started + 60
            except AuthenticationRequired as error:
                self.runner._authentication_required(error)
                return
            except Exception as error:
                LOG.error("Order polling failed error_type=%s; retrying.", type(error).__name__)
            previous_start = started
            self.poll_now.wait(max(0, self.interval - (time.monotonic() - started)))

    def _deliver(self):
        while not self.stop.is_set():
            self.changed.clear()
            try:
                self.runner._publish_orders(self.producer, [])
            except Exception as error:
                LOG.error("Order outbox delivery failed error_type=%s; retrying.", type(error).__name__)
                # Back off even when observations continue arriving during an outage.
                self.stop.wait(self.interval)
            self.changed.wait(self.interval)

    def close(self):
        self.stop.set()
        self.wake()
        # Broker requests have bounded timeouts; do not release the runner lock
        # while its order workers can still access the account state.
        for thread in self.threads:
            thread.join()
        self.producer.flush(5)
