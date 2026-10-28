"""Stateless inventory event handler (unit testable without HTTP)."""

from __future__ import annotations

import time

from libs.mesh import broker as broker_client
from libs.mesh import logging as mesh_log, metrics
from libs.mesh.storage import DocumentNotFound, ShardedStore, StorageUnavailable

log = mesh_log.get_logger("inventory.handler")


class InventoryHandler:
    def __init__(
        self,
        store: ShardedStore,
        broker: broker_client.BrokerClient,
        events_topic: str,
        work_ms: int = 1,
        metrics_obj: metrics.Metrics | None = None,
    ) -> None:
        self.store = store
        self.broker = broker
        self.events_topic = events_topic
        self.work_ms = work_ms
        self.metrics = metrics_obj or metrics.get()

    def handle(self, message: broker_client.Message) -> None:
        payload = message.value or {}
        order_id = str(payload.get("order_id", ""))
        sku = str(payload.get("sku", ""))
        quantity = int(payload.get("quantity", 0))
        customer_id = str(payload.get("customer_id", "unknown"))
        if not order_id or not sku or quantity <= 0:
            # Poison message: raising nacks and eventually dead-letters it.
            raise ValueError(f"invalid order event: {payload!r}")

        if self.work_ms:
            time.sleep(self.work_ms / 1000.0)  # models inventory/ERP lookup cost

        key = f"inventory:{sku}"
        try:
            result = self.store.mutate(
                key,
                op="decrement",
                amount=quantity,
                idempotency_key=f"order:{order_id}",
            )
        except DocumentNotFound:
            self._publish(order_id, customer_id, "REJECTED", reason="unknown_sku", sku=sku, quantity=quantity)
            self.metrics.inc("inventory_rejected_total", reason="unknown_sku")
            return
        except StorageUnavailable:
            # Let the broker redeliver: the mutation is idempotent so a retry is safe.
            self.metrics.inc("inventory_storage_errors_total")
            raise

        value = result.get("value") or {}
        available = int(value.get("available", 0))
        if result.get("outcome") == "insufficient":
            self._publish(
                order_id,
                customer_id,
                "REJECTED",
                reason="insufficient_stock",
                sku=sku,
                quantity=quantity,
                available=available,
            )
            self.metrics.inc("inventory_rejected_total", reason="insufficient_stock")
        else:
            self._publish(
                order_id,
                customer_id,
                "CONFIRMED",
                reason=None,
                sku=sku,
                quantity=quantity,
                available=available,
            )
            self.metrics.inc("inventory_reserved_total", sku=sku)
        if result.get("cached"):
            self.metrics.inc("inventory_duplicate_events_total")
            log.info("duplicate event ignored via idempotency key", extra=mesh_log.fields(order_id=order_id))

    def _publish(
        self,
        order_id: str,
        customer_id: str,
        status: str,
        reason: str | None,
        sku: str,
        quantity: int,
        available: int | None = None,
    ) -> None:
        self.broker.publish(
            self.events_topic,
            key=customer_id,
            value={
                "order_id": order_id,
                "customer_id": customer_id,
                "status": status,
                "reason": reason,
                "sku": sku,
                "quantity": quantity,
                "available": available,
                "handled_at": time.time(),
            },
            headers={"event-type": "inventory.reserved", "schema-version": "1"},
        )
