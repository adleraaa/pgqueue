"""Render QueueMetrics in the Prometheus text exposition format (version 0.0.4).

Written by hand instead of using prometheus_client because every value comes
from a SQL aggregate at scrape time; there is no in-process state to register.
Counters are derived from rows in the jobs table, so they only go down if old
jobs are deleted, which Prometheus treats like a counter reset.
"""

from __future__ import annotations

from .models import JobState
from .queue import WAIT_BUCKETS, QueueMetrics

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _labels(**labels: str) -> str:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    return "{" + ",".join(f'{k}="{escape(v)}"' for k, v in labels.items()) + "}"


def _fmt(value: float) -> str:
    return repr(float(value)) if isinstance(value, float) else str(value)


def render(m: QueueMetrics) -> str:
    lines: list[str] = []

    lines += [
        "# HELP pgqueue_jobs Number of jobs currently in each state.",
        "# TYPE pgqueue_jobs gauge",
    ]
    for queue in m.queues:
        # Emit every state, including zeros, so dashboards do not see gaps.
        for state in JobState:
            n = m.depth.get((queue, state.value), 0)
            lines.append(f"pgqueue_jobs{_labels(queue=queue, state=state.value)} {n}")

    lines += [
        "# HELP pgqueue_jobs_completed_total Jobs that finished successfully.",
        "# TYPE pgqueue_jobs_completed_total counter",
    ]
    for queue in m.queues:
        n = m.depth.get((queue, JobState.SUCCEEDED.value), 0)
        lines.append(f"pgqueue_jobs_completed_total{_labels(queue=queue)} {n}")

    lines += [
        "# HELP pgqueue_job_failures_total Failed attempts (handler errors and expired leases).",
        "# TYPE pgqueue_job_failures_total counter",
    ]
    for queue in m.queues:
        lines.append(f"pgqueue_job_failures_total{_labels(queue=queue)} {m.failures.get(queue, 0)}")

    lines += [
        "# HELP pgqueue_enqueue_to_start_seconds Time from a job becoming runnable to its"
        " first claim.",
        "# TYPE pgqueue_enqueue_to_start_seconds histogram",
    ]
    for queue in m.queues:
        buckets = m.wait_buckets.get(queue, [0] * len(WAIT_BUCKETS))
        for le, n in zip(WAIT_BUCKETS, buckets, strict=True):
            lines.append(
                f"pgqueue_enqueue_to_start_seconds_bucket{_labels(queue=queue, le=_fmt(le))} {n}"
            )
        count = m.wait_count.get(queue, 0)
        lines.append(
            f"pgqueue_enqueue_to_start_seconds_bucket{_labels(queue=queue, le='+Inf')} {count}"
        )
        lines.append(
            f"pgqueue_enqueue_to_start_seconds_sum{_labels(queue=queue)}"
            f" {_fmt(m.wait_sum.get(queue, 0.0))}"
        )
        lines.append(f"pgqueue_enqueue_to_start_seconds_count{_labels(queue=queue)} {count}")

    return "\n".join(lines) + "\n"
