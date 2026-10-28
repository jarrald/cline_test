"""Prometheus text exposition for :class:`mesh.metrics.Metrics`.

Split into its own module so that ``metrics.py`` stays focused and readable.
"""

from __future__ import annotations

from .metrics import LATENCY_BUCKETS, Metrics, _Histogram


def render_prometheus(metrics: Metrics) -> str:
    lines: list[str] = []
    with metrics._lock:  # noqa: SLF001 - same package, intentional
        counters = dict(metrics._counters)
        gauges = dict(metrics._gauges)
        histograms = {
            key: _Histogram(list(hist.buckets), hist.count, hist.total)
            for key, hist in metrics._histograms.items()
        }
        inflight = metrics._inflight
        inflight_max = metrics._inflight_max
        cpu = metrics._cpu_percent
    for key, value in sorted(counters.items()):
        lines.append(f"{key} {value:g}")
    for key, value in sorted(gauges.items()):
        lines.append(f"{key} {value:g}")
    for key, hist in sorted(histograms.items()):
        name, _, raw_labels = key.partition("{")
        labels = raw_labels.rstrip("}")
        for bound, count in zip(LATENCY_BUCKETS, hist.buckets):
            comma = f",le=\"{bound}\"" if labels else f'le="{bound}"'
            lines.append(f"{name}_bucket{{{labels}{comma}}} {count:g}")
        comma = ",le=\"+Inf\"" if labels else 'le="+Inf"'
        lines.append(f"{name}_bucket{{{labels}{comma}}} {hist.count:g}")
        lines.append(f"{key} {hist.total:g}")
        lines.append(f"{key}_count {hist.count:g}")
    base = f'service="{metrics.service}",pod="{metrics.pod}"'
    lines.append(f"mesh_inflight{{{base}}} {inflight:g}")
    lines.append(f"mesh_inflight_max{{{base}}} {inflight_max:g}")
    lines.append(f"mesh_cpu_percent{{{base}}} {cpu:g}")
    lines.append(f"mesh_rps{{{base}}} {metrics._rps():g}")  # noqa: SLF001
    return "\n".join(lines) + "\n"
