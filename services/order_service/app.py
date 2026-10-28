"""Order service (stateless HTTP tier, horizontally scalable).

Responsibilities
----------------
* validate and persist orders in the sharded store (source of truth)
* publish ``orders.created`` events to the broker (keyed by customer_id so a
  customer's events stay ordered on one partition)
* serve order reads, preferring a shard replica and falling back to the primary

The service owns no local state, so Kubernetes can run any number of replicas
behind the Service load balancer. The only cross-replica coordination needed is
the idempotency record in the database - no sticky sessions.
"""

from __future__ import annotations

import time

from libs.mesh import broker as broker_client
from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.httpkit import Problem
from libs.mesh.storage import ShardedStore, StorageUnavailable
from services.order_service.repository import OrderRepository

SERVICE = "order-service"


def build_app(repo: OrderRepository, io_delay_ms: int, max_concurrency: int) -> httpkit.App:
    app = httpkit.App(SERVICE, max_concurrency=max_concurrency)

    @app.route("POST", "/orders", name="orders.create")
    def create_order(req: httpkit.Request):
        if io_delay_ms:
            time.sleep(io_delay_ms / 1000.0)  # models downstream database/I/O latency
        order: dict | None = None
        try:
            order = repo.create(req.json(), req.header("Idempotency-Key") or None)
            return httpkit.Response.json(
                order,
                status=202,
                Location=f"/orders/{order['order_id']}",
                **{"X-Order-Status": order["status"]},
            )
        except StorageUnavailable as exc:
            app.metrics.inc("orders_storage_unavailable_total")
            raise Problem(503, "storage_unavailable", str(exc), retry_after=0.2) from exc

    @app.route("GET", "/orders/{order_id}", name="orders.get")
    def get_order(req: httpkit.Request):
        if io_delay_ms:
            time.sleep(io_delay_ms / 2000.0)
        try:
            order = repo.get(req.params["order_id"], consistent=req.query_bool("consistent"))
        except StorageUnavailable as exc:
            raise Problem(503, "storage_unavailable", str(exc), retry_after=0.2) from exc
        if order is None:
            raise Problem(404, "order_not_found", "order not found")
        return {"order": order}

    @app.route("GET", "/debug/state", name="debug.state")
    def debug_state(_: httpkit.Request):
        return {
            "pod": config.env("POD_NAME", "order-service-0"),
            "shards": {
                name: {"primary": shard.primary, "replicas": shard.replicas}
                for name, shard in repo.store.shards.items()
            },
            "events": repo.store.events()[-20:],
        }

    @app.route("GET", "/orders", name="orders.list")
    def list_hint(_: httpkit.Request):
        return {
            "message": "orders are sharded by customer_id; fetch a single order by id",
            "example": "/orders/ord_123",
        }

    return app


def main() -> None:
    pod = config.env("POD_NAME", "order-service-0")
    mesh_log.configure(SERVICE, pod)
    metrics.configure(SERVICE, pod)
    broker_url = config.env("BROKER_URL", "http://127.0.0.1:16000")
    topic = config.env("ORDERS_TOPIC", "orders.created")
    broker = broker_client.BrokerClient(broker_url)

    def publisher(order: dict) -> None:
        broker.publish(
            topic,
            key=order["customer_id"],
            value={
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "sku": order["sku"],
                "quantity": order["quantity"],
                "created_at": order["created_at"],
            },
            headers={"event-type": "order.created", "schema-version": "1"},
        )
        metrics.get().inc("orders_published_total")

    store = ShardedStore.from_env(metrics.get())
    repo = OrderRepository(store, publisher)
    app = build_app(
        repo,
        io_delay_ms=config.env_int("ORDER_IO_MS", 0),
        max_concurrency=config.env_int("MAX_CONCURRENCY", 16),
    )
    mesh_log.get_logger(SERVICE).info(
        "order service ready",
        extra=mesh_log.fields(broker=broker_url, topic=topic, shards=list(store.shards)),
    )
    app.serve(config.env_int("PORT", 12000))


if __name__ == "__main__":
    main()
