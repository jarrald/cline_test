"""Order repository: sharded persistence + idempotent creation.

Shard key: ``customer_id``. All orders for a customer live on one shard and
one replica set, which makes per-customer queries cheap and keeps ordering
guarantees simple.

Idempotency: clients send an ``Idempotency-Key`` header. The repository stores
a ``idem:order:<key> -> order_id`` pointer using optimistic concurrency
(``expected_version=0``), so a retried POST - by the gateway, a load balancer,
or the user - always resolves to the same order instead of creating duplicates.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable

from libs.mesh import logging as mesh_log
from libs.mesh.httpkit import Problem
from libs.mesh.storage import DocumentNotFound, ShardedStore, StorageConflict, StorageUnavailable

log = mesh_log.get_logger("orders.repository")

FINAL_STATES = {"CONFIRMED", "REJECTED"}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class OrderRepository:
    def __init__(self, store: ShardedStore, publisher: Callable[[dict], None]) -> None:
        self.store = store
        self.publisher = publisher

    # ------------------------------------------------------------------ create
    def create(self, payload: dict, idempotency_key: str | None) -> dict:
        customer_id = str(payload.get("customer_id", "")).strip()
        sku = str(payload.get("sku", "")).strip()
        quantity = payload.get("quantity", 1)
        if not customer_id:
            raise Problem(400, "invalid_customer", "customer_id is required")
        if not sku:
            raise Problem(400, "invalid_sku", "sku is required")
        if not isinstance(quantity, int) or quantity < 1 or quantity > 100:
            raise Problem(400, "invalid_quantity", "quantity must be an integer between 1 and 100")

        key = idempotency_key or str(payload.get("idempotency_key") or "")
        if key:
            existing = self._find_by_idempotency(key)
            if existing is not None:
                self._ensure_published(existing)
                return existing

        order = {
            "order_id": f"ord_{uuid.uuid4().hex[:16]}",
            "customer_id": customer_id,
            "sku": sku,
            "quantity": quantity,
            "status": "PENDING",
            "reason": None,
            "created_at": utcnow(),
            "updated_at": utcnow(),
            "event_published": False,
        }
        try:
            self.store.put(self._order_key(order["order_id"]), order, expected_version=0)
        except StorageConflict:  # pragma: no cover - uuid collision
            raise Problem(409, "order_conflict", "order already exists")
        if key:
            try:
                self.store.put(self._idem_key(key), {"order_id": order["order_id"]}, expected_version=0)
            except StorageConflict:
                winner = self._find_by_idempotency(key)
                if winner is not None:
                    return winner
        self._ensure_published(order)
        return order

    def _ensure_published(self, order: dict) -> None:
        """Publish ``orders.created`` at least once; safe to repeat.

        Duplicate events are possible (at-least-once), which is why the
        inventory worker mutates stock with an idempotency key derived from
        ``order_id``: repeated events cannot decrement twice.
        """
        if order.get("event_published"):
            return
        try:
            self.publisher(order)
        except Exception as exc:
            log.warning("publish failed", extra=mesh_log.fields(order_id=order.get("order_id"), error=str(exc)))
            self._set_status(order, "PUBLISH_FAILED", reason="event publish failed")
            raise Problem(503, "event_publish_failed", "order accepted but event publishing failed", retry_after=0.25)
        order["event_published"] = True
        updated = {**order, "updated_at": utcnow()}
        try:
            stored = self.store.get(self._order_key(order["order_id"]), consistent=True)
            version = stored["version"] if stored else 0
            self.store.put(self._order_key(order["order_id"]), updated, expected_version=version)
        except (StorageConflict, StorageUnavailable, DocumentNotFound) as exc:
            # Publishing succeeded; a later retry simply republishes (idempotent downstream).
            log.warning("could not persist publish marker", extra=mesh_log.fields(error=str(exc)))

    # -------------------------------------------------------------------- read
    def get(self, order_id: str, consistent: bool = False) -> dict | None:
        result = self.store.get(self._order_key(order_id), consistent=consistent)
        return result["value"] if result else None

    def update_status(self, order_id: str, status: str, reason: str | None, detail: dict) -> dict | None:
        for attempt in range(3):
            stored = self.store.get(self._order_key(order_id), consistent=True)
            if stored is None:
                log.warning("event for unknown order", extra=mesh_log.fields(order_id=order_id))
                return None
            order = stored["value"]
            if order.get("status") in FINAL_STATES:
                return order  # idempotent: first terminal state wins
            order.update(
                {
                    "status": status,
                    "reason": reason,
                    "updated_at": utcnow(),
                    "confirmation": detail,
                }
            )
            try:
                self.store.put(self._order_key(order_id), order, expected_version=stored["version"])
                return order
            except StorageConflict:
                continue  # concurrent update, re-read and retry
        raise StorageUnavailable("could not update order status after 3 attempts")

    # ----------------------------------------------------------------- helpers
    def _find_by_idempotency(self, key: str) -> dict | None:
        pointer = self.store.get(self._idem_key(key), consistent=True)
        if pointer is None:
            return None
        order = self.get(pointer["value"]["order_id"], consistent=True)
        if order is None:
            return None
        return order

    def _set_status(self, order: dict, status: str, reason: str | None) -> None:
        try:
            stored = self.store.get(self._order_key(order["order_id"]), consistent=True)
            version = stored["version"] if stored else 0
            order.update({"status": status, "reason": reason, "updated_at": utcnow()})
            self.store.put(self._order_key(order["order_id"]), order, expected_version=version)
        except Exception as exc:  # best effort; caller still returns an error
            log.warning("could not record failure status", extra=mesh_log.fields(error=str(exc)))

    @staticmethod
    def _order_key(order_id: str) -> str:
        return f"order:{order_id}"

    @staticmethod
    def _idem_key(key: str) -> str:
        return f"idem:order:{key}"
