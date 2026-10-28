"""Declarative cluster specification for the local control plane.

This is the local equivalent of a Kubernetes manifest set: deployments, replica
counts, environment (ConfigMap), ports and autoscaling rules. The same topology
is expressed as real YAML in ``deploy/k8s``; ``kpilot`` executes it here so the
system can be demonstrated and tested without a container runtime.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class Autoscale:
    """Horizontal Pod Autoscaler equivalent.

    ``inflight`` (concurrent requests per pod) is used as the saturation signal:
    it is Little's law (rps x latency), which reflects queueing far earlier than
    CPU does for I/O bound services. Kubernetes manifests use CPU + inflight
    through the custom metrics API (see deploy/k8s).
    """

    metric: str = "inflight"
    target: float = 2.0
    min_replicas: int = 2
    max_replicas: int = 6
    scale_up_cooldown_s: float = 3.0
    scale_down_cooldown_s: float = 12.0
    stabilization_s: float = 4.0
    max_step: int = 4


@dataclass
class DeploymentSpec:
    name: str
    module: str
    replicas: int
    port_base: int
    env: dict[str, str] = field(default_factory=dict)
    probe_path: str = "/readyz"
    probe_port_offset: int = 0
    startup_timeout_s: float = 20.0
    autoscale: Autoscale | None = None
    description: str = ""


@dataclass
class ClusterSpec:
    name: str
    base_port: int
    runtime_dir: str
    deployments: list[DeploymentSpec]
    admin_port: int = 0
    hpa_interval_s: float = 2.0

    def to_dict(self) -> dict:
        payload = asdict(self)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def deployment(self, name: str) -> DeploymentSpec:
        for deployment in self.deployments:
            if deployment.name == name:
                return deployment
        raise KeyError(name)


def shard_topology(base_port: int) -> list[dict]:
    """3 logical shards, each a primary + read replica (6 storage processes)."""
    shards = []
    for shard in range(3):
        primary_port = base_port + shard * 2
        replica_port = primary_port + 1
        shards.append(
            {
                "name": f"shard-{shard}",
                "primary": f"http://127.0.0.1:{primary_port}",
                "replicas": [f"http://127.0.0.1:{replica_port}"],
                "ports": {"primary": primary_port, "replica": replica_port},
            }
        )
    return shards


def build_cluster(
    base_port: int,
    runtime_dir: str,
    profile: str = "demo",
    order_io_ms: int | None = None,
    order_concurrency: int = 8,
    order_replicas: int = 2,
    max_backlog: int | None = None,
    hpa_interval_s: float | None = None,
) -> ClusterSpec:
    """Build the full topology. ``profile`` tunes latency/cooldowns for demos vs tests."""
    shards = shard_topology(base_port)
    shards_env = json.dumps([{k: v for k, v in s.items() if k != "ports"} for s in shards])
    broker_url = f"http://127.0.0.1:{base_port + 10}"
    discovery_file = f"{runtime_dir}/registry.json"
    topics = {
        "orders.created": {"partitions": 3},
        "orders.created.dlq": {"partitions": 1},
        "inventory.events": {"partitions": 3},
        "inventory.events.dlq": {"partitions": 1},
    }
    io_ms = order_io_ms if order_io_ms is not None else (25 if profile == "demo" else 12)
    backlog = max_backlog if max_backlog is not None else (4000 if profile == "demo" else 400)
    hpa_interval = hpa_interval_s if hpa_interval_s is not None else (2.0 if profile == "demo" else 0.5)
    up_cooldown = 3.0 if profile == "demo" else 1.5
    down_cooldown = 12.0 if profile == "demo" else 8.0
    stabilization = 4.0 if profile == "demo" else 3.0

    deployments: list[DeploymentSpec] = []
    for shard in shards:
        for role in ("primary", "replica"):
            port = shard["ports"][role]
            name = f"storage-{shard['name']}-{role}"
            deployments.append(
                DeploymentSpec(
                    name=name,
                    module="services.storage_node.app",
                    replicas=1,
                    port_base=port,
                    env={
                        "SHARD_NAME": shard["name"],
                        "SHARD_ROLE": role,
                        "NODE_ID": name,
                        "DATA_DIR": f"{runtime_dir}/data/{name}",
                        "MAX_CONCURRENCY": "64",
                    },
                    description=f"SQLite {role} for {shard['name']}",
                )
            )

    deployments.append(
        DeploymentSpec(
            name="broker",
            module="services.message_broker.app",
            replicas=1,
            port_base=base_port + 10,
            env={
                "BROKER_DATA_DIR": f"{runtime_dir}/data/broker",
                "DEFAULT_PARTITIONS": "3",
                "TOPICS": json.dumps(topics),
                "MAX_BACKLOG": str(backlog),
                "MAX_ATTEMPTS": "3",
                "RETRY_BACKOFF_S": "0.1",
                "MAX_CONCURRENCY": "128",
            },
            description="Kafka-like log with consumer groups, retries and DLQ",
        )
    )

    deployments.append(
        DeploymentSpec(
            name="order-service",
            module="services.order_service.app",
            replicas=order_replicas,
            port_base=base_port + 20,
            env={
                "SHARDS": shards_env,
                "BROKER_URL": broker_url,
                "ORDERS_TOPIC": "orders.created",
                "ORDER_IO_MS": str(io_ms),
                "MAX_CONCURRENCY": str(order_concurrency),
            },
            autoscale=Autoscale(
                metric="inflight",
                target=max(1.0, order_concurrency * 0.35),
                min_replicas=order_replicas,
                max_replicas=max(order_replicas, 6),
                scale_up_cooldown_s=up_cooldown,
                scale_down_cooldown_s=down_cooldown,
                stabilization_s=stabilization,
            ),
            description="Stateless order API; the HPA scales this tier",
        )
    )

    for index in range(3):
        deployments.append(
            DeploymentSpec(
                name="inventory-worker",
                module="services.inventory_worker.app",
                replicas=3,
                port_base=base_port + 70 + index,
                env={
                    "SHARDS": shards_env,
                    "BROKER_URL": broker_url,
                    "ORDERS_TOPIC": "orders.created",
                    "INVENTORY_EVENTS_TOPIC": "inventory.events",
                    "CONSUMER_GROUP": "inventory",
                    "WORK_MS": "2",
                    "POD_COUNT": "3",
                    "POD_ORDINAL": str(index),
                },
                description=f"Consumer-group member owning partition {index}",
            )
        )

    for index in range(3):
        deployments.append(
            DeploymentSpec(
                name="status-projector",
                module="services.status_projector.app",
                replicas=3,
                port_base=base_port + 80 + index,
                env={
                    "SHARDS": shards_env,
                    "BROKER_URL": broker_url,
                    "INVENTORY_EVENTS_TOPIC": "inventory.events",
                    "CONSUMER_GROUP": "order-status",
                    "POD_COUNT": "3",
                    "POD_ORDINAL": str(index),
                },
                description=f"CQRS projector owning partition {index}",
            )
        )

    deployments.append(
        DeploymentSpec(
            name="api-gateway",
            module="services.api_gateway.app",
            replicas=2,
            port_base=base_port + 40,
            env={
                "UPSTREAM_SERVICE": "order-service",
                "DISCOVERY_FILE": discovery_file,
                "API_KEYS": "demo-key,load-test-key",
                "RATE_LIMIT_RPS": "400",
                "RATE_LIMIT_BURST": "800",
                "UPSTREAM_TIMEOUT_S": "2.0",
                "UPSTREAM_RETRIES": "1",
                "BREAKER_THRESHOLD": "5",
                "BREAKER_RESET_S": "4",
                "MAX_CONCURRENCY": "64",
            },
            autoscale=Autoscale(
                metric="inflight",
                target=20.0,
                min_replicas=2,
                max_replicas=4,
                scale_up_cooldown_s=up_cooldown,
                scale_down_cooldown_s=down_cooldown,
                stabilization_s=stabilization,
            ),
            description="Auth, rate limiting, routing, retries, circuit breaking",
        )
    )

    deployments.append(
        DeploymentSpec(
            name="edge-lb",
            module="services.edge_lb.app",
            replicas=1,
            port_base=base_port + 60,
            env={
                "LISTEN_PORT": str(base_port + 60),
                "ADMIN_PORT": str(base_port + 61),
                "UPSTREAM_SERVICE": "api-gateway",
                "DISCOVERY_FILE": discovery_file,
                "MAX_CONNECTIONS": "256",
            },
            probe_port_offset=1,
            description="L4 load balancer (Ingress equivalent) in front of gateways",
        )
    )

    return ClusterSpec(
        name=f"shop-{profile}",
        base_port=base_port,
        runtime_dir=runtime_dir,
        deployments=deployments,
        admin_port=base_port + 90,
        hpa_interval_s=hpa_interval,
    )
