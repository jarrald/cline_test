"""Status projector: CQRS read-model updater.

Consumes ``inventory.events`` from a consumer group and projects the outcome
onto the order aggregate (PENDING -> CONFIRMED/REJECTED). It is a separate
deployment from the order API so that:

* the HTTP tier stays stateless and freely autoscalable
* consumer-group membership is stable (one pod per partition)
* a crash in projection cannot take down order intake

Projection is idempotent: the first terminal state wins and duplicate events are
ignored, so at-least-once delivery is safe.
"""

from __future__ import annotations

from libs.mesh import broker as broker_client
from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.storage import ShardedStore, StorageUnavailable
from services.order_service.repository import OrderRepository

SERVICE = "status-projector"


def build_app(state: dict) -> httpkit.App:
    app = httpkit.App(SERVICE, max_concurrency=4, readiness=lambda: (True, "consuming"))

    @app.route("GET", "/stats", name="projector.stats")
    def stats(_: httpkit.Request):
        consumer = state.get("consumer")
        return {
            "pod": state["pod"],
            "assigned_partitions": state["assigned_partitions"],
            "topic": state["topic"],
            "group": state["group"],
            "processed": consumer.processed if consumer else 0,
            "failed": consumer.failed if consumer else 0,
        }

    return app


def main() -> None:
    pod, ordinal, count = config.pod_identity()
    mesh_log.configure(SERVICE, pod)
    metrics.configure(SERVICE, pod)
    broker = broker_client.BrokerClient(config.env("BROKER_URL", "http://127.0.0.1:16000"))
    topic = config.env("INVENTORY_EVENTS_TOPIC", "inventory.events")
    group = config.env("CONSUMER_GROUP", "order-status")
    store = ShardedStore.from_env(metrics.get())
    # The projector never creates orders, so the publisher callback is unused.
    repo = OrderRepository(store, publisher=lambda order: None)

    def project(message: broker_client.Message) -> None:
        payload = message.value or {}
        status = str(payload.get("status", "")).upper()
        if status not in {"CONFIRMED", "REJECTED"}:
            raise ValueError(f"unexpected status {status!r}")
        detail = {
            "sku": payload.get("sku"),
            "quantity": payload.get("quantity"),
            "available": payload.get("available"),
            "handled_at": payload.get("handled_at"),
        }
        try:
            order = repo.update_status(str(payload["order_id"]), status, payload.get("reason"), detail)
        except StorageUnavailable:
            raise
        if order is None:
            metrics.get().inc("projector_unknown_order_total")
        else:
            metrics.get().inc("projector_updates_total", status=status)

    partitions = broker.partitions(topic)
    assigned = [p for p in range(partitions) if p % count == ordinal]
    consumer = broker_client.PartitionConsumer(
        broker=broker,
        topic=topic,
        group=group,
        partitions=assigned,
        handler=project,
        metrics=metrics.get(),
        max_messages=config.env_int("MAX_MESSAGES", 20),
        poll_interval=config.env_float("POLL_INTERVAL_S", 0.03),
    )
    app = build_app({"pod": pod, "assigned_partitions": assigned, "topic": topic, "group": group, "consumer": consumer})
    consumer.start()
    mesh_log.get_logger(SERVICE).info(
        "status projector consuming", extra=mesh_log.fields(topic=topic, partitions=assigned, group=group)
    )
    try:
        app.serve(config.env_int("PORT", 15500))
    finally:
        consumer.stop()
        store.close()


if __name__ == "__main__":
    main()
