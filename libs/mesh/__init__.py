"""Mesh: the small shared platform library used by every service in this repo.

The library is intentionally dependency free (Python standard library only) so
that the whole system can be started on any machine, including CI runners and
the sandbox this repo was developed in.

Modules
-------
config      environment/12-factor configuration helpers
logging     structured JSON logs with request correlation ids
metrics     Prometheus text + JSON snapshots (used by the autoscaler)
httpkit     tiny HTTP server framework + keep-alive HTTP client
resilience  token bucket, circuit breaker, retry helpers
hashing     consistent-hash ring used for database sharding
discovery   service registry (file based, mirrors Kubernetes Endpoints)
storage     sharded + replicated document store client
broker      message broker client, producer and partition consumer
"""

__all__ = [
    "config",
    "logging",
    "metrics",
    "httpkit",
    "resilience",
    "hashing",
    "discovery",
    "storage",
    "broker",
]
