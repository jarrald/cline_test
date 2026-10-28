"""Local control plane core: pods, deployments, probes and the HPA loop.

This mirrors the Kubernetes reconciliation model in miniature:

    observe (process alive? /readyz?) -> diff (ready pods vs desired) -> act

Every action is recorded as an event so demos and tests can assert on the
control-plane decisions (for example "HPA scaled order-service 2 -> 6").
"""

from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

from libs.mesh import logging as mesh_log
from libs.mesh.httpkit import HttpClient, HttpUnavailable
from services.kpilot.spec import Autoscale, ClusterSpec, DeploymentSpec

log = mesh_log.get_logger("kpilot")


@dataclass
class Pod:
    deployment: str
    ordinal: int
    port: int
    probe_port: int
    process: subprocess.Popen | None = None
    ready: bool = False
    ready_failures: int = 0
    restarts: int = 0
    started_at: float = 0.0
    last_exit: float = 0.0
    log_path: str = ""

    @property
    def name(self) -> str:
        return f"{self.deployment}-{self.ordinal}"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def to_dict(self) -> dict:
        return {
            "pod": self.name,
            "ordinal": self.ordinal,
            "port": self.port,
            "url": self.url,
            "ready": self.ready,
            "pid": self.process.pid if self.process else None,
            "restarts": self.restarts,
            "uptime_s": round(time.time() - self.started_at, 1) if self.started_at else 0,
        }


class DeploymentController:
    def __init__(self, spec: DeploymentSpec, runtime_dir: str, repo_root: str, events) -> None:
        self.spec = spec
        self.runtime_dir = runtime_dir
        self.repo_root = repo_root
        self._record_event = events
        self.desired = spec.replicas
        self.pods: dict[int, Pod] = {}
        self.autoscale_enabled = spec.autoscale is not None
        self.last_scale_up = 0.0
        self.last_scale_down = 0.0
        self.last_restart: dict[int, float] = {}
        self.recommendations: list[tuple[float, int]] = []
        self.last_metrics: dict[int, dict] = {}
        self.http = HttpClient(timeout=0.6, name=f"kpilot-{spec.name}")
        os.makedirs(f"{runtime_dir}/logs", exist_ok=True)

    # --------------------------------------------------------------- lifecycle
    def _env_for(self, pod: Pod) -> dict[str, str]:
        env = dict(os.environ)
        env.update(self.spec.env)
        env.update(
            {
                "PORT": str(pod.port),
                "POD_NAME": pod.name,
                "POD_ORDINAL": str(pod.ordinal),
                "POD_COUNT": str(max(1, self.desired)),
                "PYTHONPATH": self.repo_root,
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": env.get("LOG_LEVEL", "info"),
            }
        )
        return env

    def start_pod(self, ordinal: int) -> Pod:
        port = self.spec.port_base + ordinal
        pod = Pod(
            deployment=self.spec.name,
            ordinal=ordinal,
            port=port,
            probe_port=port + self.spec.probe_port_offset,
            log_path=f"{self.runtime_dir}/logs/{self.spec.name}-{ordinal}.log",
        )
        handle = open(pod.log_path, "ab", buffering=0)
        env = self._env_for(pod)
        pod.process = subprocess.Popen(
            [sys.executable, "-m", self.spec.module],
            cwd=self.repo_root,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        pod.started_at = time.time()
        self.pods[ordinal] = pod
        self._event("pod_started", pod=pod.name, port=port, pid=pod.process.pid)
        return pod

    def stop_pod(self, ordinal: int, reason: str, force: bool = False) -> None:
        pod = self.pods.pop(ordinal, None)
        if pod is None:
            return
        process = pod.process
        if process is not None and process.poll() is None:
            try:
                process.send_signal(signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=1.5 if not force else 0.2)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
        self._event("pod_stopped", pod=pod.name, reason=reason)

    # ------------------------------------------------------------------ probing
    def probe_once(self, pod: Pod) -> bool:
        try:
            result = self.http.request("GET", f"http://127.0.0.1:{pod.probe_port}{self.spec.probe_path}", retry=False)
            return result.ok
        except HttpUnavailable:
            return False

    def metrics_once(self, pod: Pod) -> dict | None:
        try:
            result = self.http.request("GET", f"http://127.0.0.1:{pod.probe_port}/metrics.json", retry=False)
            return result.json() if result.ok else None
        except (HttpUnavailable, ValueError):
            return None

    def _event(self, kind: str, **fields) -> None:
        entry = {"ts": round(time.time(), 3), "type": kind, "deployment": self.spec.name, **fields}
        self._record_event(entry)
        if kind != "probe":
            log.info(kind, extra=mesh_log.fields(**fields))

    # -------------------------------------------------------------- reconcile
    def reconcile(self) -> None:
        now = time.time()
        # scale down / remove surplus pods (highest ordinals first)
        for ordinal in sorted(self.pods, reverse=True):
            if ordinal >= self.desired:
                self.stop_pod(ordinal, reason="scale_down")
        # create missing pods
        for ordinal in range(self.desired):
            if ordinal in self.pods:
                continue
            last = self.last_restart.get(ordinal, 0.0)
            if now - last < 1.0:
                continue  # simple CrashLoopBackOff
            self.last_restart[ordinal] = now
            self.start_pod(ordinal)
        # liveness + readiness
        for ordinal, pod in list(self.pods.items()):
            if pod.process is not None and pod.process.poll() is not None:
                exit_code = pod.process.returncode
                self.pods.pop(ordinal, None)
                self._event("pod_crashed", pod=pod.name, exit_code=exit_code)
                continue
            was_ready = pod.ready
            ready = self.probe_once(pod)
            pod.ready = ready
            if ready:
                pod.ready_failures = 0
                if not was_ready:
                    self._event("pod_ready", pod=pod.name, startup_s=round(now - pod.started_at, 2))
            else:
                pod.ready_failures += 1
                if was_ready:
                    self._event("pod_not_ready", pod=pod.name)
                if now - pod.started_at > self.spec.startup_timeout_s or pod.ready_failures >= 10:
                    self._event("pod_unhealthy_restart", pod=pod.name, failures=pod.ready_failures)
                    self.stop_pod(ordinal, reason="readiness_failed", force=True)

    def ready_pods(self) -> list[Pod]:
        return [p for p in self.pods.values() if p.ready]

    # ------------------------------------------------------------------- HPA
    def evaluate_autoscale(self, now: float) -> None:
        rule: Autoscale | None = self.spec.autoscale
        if rule is None or not self.autoscale_enabled:
            return
        pods = self.ready_pods()
        if not pods:
            return
        self.last_metrics = {}
        total = 0.0
        for pod in pods:
            snapshot = self.metrics_once(pod)
            if snapshot is None:
                continue
            self.last_metrics[pod.ordinal] = snapshot
            total += float(snapshot.get(rule.metric, 0.0) or 0.0)
        if not self.last_metrics:
            return
        average = total / len(self.last_metrics)
        desired = int(math.ceil(total / max(rule.target, 0.01)))
        desired = max(rule.min_replicas, min(rule.max_replicas, desired))
        self.recommendations.append((now, desired))
        self.recommendations = [(ts, value) for ts, value in self.recommendations if now - ts <= max(rule.stabilization_s, 60)]

        if desired > self.desired:
            if now - self.last_scale_up < rule.scale_up_cooldown_s:
                return
            target = min(desired, self.desired + rule.max_step, rule.max_replicas)
            if target == self.desired:
                return
            self._event(
                "hpa_scale_up",
                metric=rule.metric,
                average=round(average, 2),
                target=rule.target,
                from_replicas=self.desired,
                to_replicas=target,
                reason=f"{rule.metric} {round(average, 2)} > target {rule.target}",
            )
            self.desired = target
            self.last_scale_up = now
            return

        if desired < self.desired:
            if now - self.last_scale_down < rule.scale_down_cooldown_s:
                return
            # stabilization window: only scale down if every recent recommendation is lower
            window = [(ts, value) for ts, value in self.recommendations if now - ts <= rule.stabilization_s]
            if not window or max(value for _, value in window) >= self.desired:
                return
            target = max(desired, self.desired - rule.max_step, rule.min_replicas)
            self._event(
                "hpa_scale_down",
                metric=rule.metric,
                average=round(average, 2),
                target=rule.target,
                from_replicas=self.desired,
                to_replicas=target,
                reason=f"{rule.metric} {round(average, 2)} below target {rule.target}",
            )
            self.desired = target
            self.last_scale_down = now

    def manual_scale(self, replicas: int, disable_autoscale: bool = True) -> None:
        replicas = max(0, min(replicas, 12))
        previous = self.desired
        self.desired = replicas
        if disable_autoscale:
            self.autoscale_enabled = False
        self._event("manual_scale", from_replicas=previous, to_replicas=replicas, autoscale=not disable_autoscale)

    def status(self) -> dict:
        return {
            "deployment": self.spec.name,
            "module": self.spec.module,
            "desired_replicas": self.desired,
            "ready_replicas": len(self.ready_pods()),
            "autoscale": None
            if self.spec.autoscale is None
            else {
                **self.spec.autoscale.__dict__,
                "enabled": self.autoscale_enabled,
                "last_metrics": self.last_metrics,
            },
            "pods": [pod.to_dict() for pod in sorted(self.pods.values(), key=lambda p: p.ordinal)],
        }


class Cluster:
    """Reconciliation loop + registry publishing + control-plane API surface."""

    def __init__(self, spec: ClusterSpec, repo_root: str) -> None:
        self.spec = spec
        self.repo_root = repo_root
        self.events: list[dict] = []
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.started_at = time.time()
        self._last_registry = ""
        self._last_hpa = 0.0
        self.hpa_interval = spec.hpa_interval_s
        os.makedirs(f"{spec.runtime_dir}/state", exist_ok=True)
        self.events_path = f"{spec.runtime_dir}/state/events.jsonl"
        self.deployments = {
            deployment.name: DeploymentController(deployment, spec.runtime_dir, repo_root, self._record)
            for deployment in spec.deployments
        }

    # ------------------------------------------------------------------ events
    def _record(self, entry: dict) -> None:
        with self.lock:
            self.events.append(entry)
            if len(self.events) > 5000:
                del self.events[:1000]
            with open(self.events_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, separators=(",", ":")) + "\n")

    # ---------------------------------------------------------------- registry
    def write_registry(self) -> None:
        services: dict[str, list[dict]] = {}
        for name, controller in self.deployments.items():
            services[name] = [
                {"pod": pod.name, "url": pod.url, "ready": True}
                for pod in sorted(controller.ready_pods(), key=lambda p: p.ordinal)
            ]
        payload = {"updated_at": round(time.time(), 3), "services": services}
        serialized = json.dumps(payload, sort_keys=True)
        if serialized == self._last_registry:
            return
        path = f"{self.spec.runtime_dir}/registry.json"
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.replace(tmp, path)
        self._last_registry = serialized

    # -------------------------------------------------------------------- loop
    def tick(self) -> None:
        for controller in list(self.deployments.values()):
            controller.reconcile()
        self.write_registry()
        now = time.monotonic()
        if now - self._last_hpa >= self.hpa_interval:
            self._last_hpa = now
            for controller in list(self.deployments.values()):
                try:
                    controller.evaluate_autoscale(time.time())
                except Exception as exc:  # control loop must never die
                    self._record({"ts": time.time(), "type": "autoscale_error", "deployment": controller.spec.name, "error": str(exc)})

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.tick()
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("reconcile failed", extra=mesh_log.fields(error=str(exc)))
            self.stop_event.wait(0.2)

    # ------------------------------------------------------------------ status
    def ready(self) -> bool:
        for controller in self.deployments.values():
            if len(controller.pods) != controller.desired:
                return False
            if len(controller.ready_pods()) != controller.desired:
                return False
        return True

    def totals(self) -> dict:
        pods = sum(len(c.pods) for c in self.deployments.values())
        ready = sum(len(c.ready_pods()) for c in self.deployments.values())
        return {"pods": pods, "ready_pods": ready, "deployments": len(self.deployments)}

    def status(self) -> dict:
        with self.lock:
            events = list(self.events[-80:])
        return {
            "cluster": self.spec.name,
            "base_port": self.spec.base_port,
            "uptime_s": round(time.time() - self.started_at, 1),
            "ready": self.ready(),
            "totals": self.totals(),
            "deployments": {name: controller.status() for name, controller in self.deployments.items()},
            "events": events,
        }

    def deployment(self, name: str) -> DeploymentController:
        if name not in self.deployments:
            raise KeyError(name)
        return self.deployments[name]

    # ----------------------------------------------------------------- control
    def scale(self, name: str, replicas: int, disable_autoscale: bool = True) -> dict:
        controller = self.deployment(name)
        controller.manual_scale(replicas, disable_autoscale=disable_autoscale)
        self.tick()
        return controller.status()

    def set_autoscale(self, name: str, enabled: bool) -> dict:
        controller = self.deployment(name)
        controller.autoscale_enabled = enabled
        controller._event("autoscale_toggled", enabled=enabled)
        return controller.status()

    def kill(self, name: str, ordinal: int | None = None) -> dict:
        controller = self.deployment(name)
        killed = []
        for pod_ordinal in ([ordinal] if ordinal is not None else sorted(controller.pods)):
            if pod_ordinal in controller.pods:
                controller.stop_pod(pod_ordinal, reason="chaos_kill", force=True)
                killed.append(f"{name}-{pod_ordinal}")
        controller._event("chaos_kill", pods=killed)
        return {"killed": killed}

    def shutdown(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        log.info("shutting down cluster")
        for controller in self.deployments.values():
            for ordinal in sorted(controller.pods):
                controller.stop_pod(ordinal, reason="shutdown")
