"""Inventory worker: consumer-group member that reserves stock.

Scaling model: this is a queue worker, not an HTTP replica. The number of
replicas is bounded by the topic partition count and each pod owns a stable
subset (``partition % POD_COUNT == POD_ORDINAL``) so per-SKU events stay ordered
(Kafka's static partition assignment). Autoscaling worker tiers therefore needs
partition-aware autoscalers (KEDA scales to at most ``partitions`` replicas).

Exactly-once stock decrement: the storage node applies the mutation and records
the idempotency key in one SQLite transaction, so a redelivered ``order.created``
event never decrements twice.
"""

from __future__ import annotations

import time

from libs.mesh import broker as broker_client
from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.storage import DocumentNotFound, ShardedStore, StorageUnavailable
from services.inventory_worker.handler import InventoryHandler

SERVICE = "inventory-worker"


def build_app(state: dict, max_concurrency: int) -> httpkit.App:
    app = httpkit.App(SERVICE, max_concurrency=max_concurrency, readiness=lambda: (True, "consuming"))

    @app.route("GET", "/stats", name="worker.stats")
    def stats(_: httpkit.Request):
        return {
            **state,
            "processed": state.get("consumer").processed if state.get("consumer") else 0,
            "failed": state.get("consumer").failed if state.get("consumer") else 0,
        }

    return app


def main() -> None:
    pod, ordinal, count = config.pod_identity()
    mesh_log.configure(SERVICE, pod)
    metrics.configure(SERVICE, pod)
    broker = broker_client.BrokerClient(config.env("BROKER_URL", "http://127.0.0.1:16000"))
    topic = config.env("ORDERS_TOPIC", "orders.created")
    events_topic = config.env("INVENTORY_EVENTS_TOPIC", "inventory.events")
    group = config.env("CONSUMER_GROUP", "inventory")
    store = ShardedStore.from_env(metrics.get())
    handler = InventoryHandler(
        store=store,
        broker=broker,
        events_topic=events_topic,
        work_ms=config.env_int("WORK_MS", 1),
    )
    partitions = broker.partitions(topic)
    assigned = [p for p in range(partitions) if p % count == ordinal]
    consumer = broker_client.PartitionConsumer(
        broker=broker,
        topic=topic,
        group=group,
        partitions=assigned,
        handler=handler.handle,
        metrics=metrics.get(),
        max_messages=config.env_int("MAX_MESSAGES", 10),
        poll_interval=config.env_float("POLL_INTERVAL_S", 0.03),
    )
    state = {
        "pod": pod,
        "ordinal": ordinal,
        "replica_count": count,
        "assigned_partitions": assigned,
        "topic": topic,
        "group": group,
        "consumer": consumer,
        "handler": handler,
    }
    app = build_app(state, config.env_int("MAX_CONCURRENCY", 8))
    consumer.start()
    mesh_log.get_logger(SERVICE).info(
        "inventory worker consuming",
        extra=mesh_log.fields(partitions=assigned, topic=topic, group=group),
    )
    try:
        app.serve(config.env_int("PORT", 15000))
    finally:
        consumer.stop()
        store.close()


if __name__ == "__main__":
    main()
