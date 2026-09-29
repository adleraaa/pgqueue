"""Pure tests of the Prometheus text rendering (no database)."""

from __future__ import annotations

import re

from pgqueue.metrics import render
from pgqueue.queue import WAIT_BUCKETS, QueueMetrics

LINE = re.compile(r'^([a-z_]+)(\{[a-z_]+="[^"]*"(,[a-z_]+="[^"]*")*\})? (-?[0-9.e+]+)$')


def sample() -> QueueMetrics:
    buckets = [0] * len(WAIT_BUCKETS)
    buckets[3:] = [2] * (len(WAIT_BUCKETS) - 3)
    return QueueMetrics(
        depth={("default", "queued"): 4, ("default", "succeeded"): 2},
        failures={"default": 3},
        wait_buckets={"default": buckets},
        wait_count={"default": 2},
        wait_sum={"default": 0.07},
    )


def test_every_line_is_valid_exposition_format() -> None:
    for line in render(sample()).splitlines():
        assert line.startswith("# HELP ") or line.startswith("# TYPE ") or LINE.match(line), line


def test_all_states_emitted_with_zeros() -> None:
    text = render(sample())
    for state in ("queued", "running", "succeeded", "dead", "cancelled"):
        assert f'pgqueue_jobs{{queue="default",state="{state}"}}' in text
    assert 'pgqueue_jobs{queue="default",state="dead"} 0' in text


def test_histogram_is_cumulative_and_ends_with_inf_equal_to_count() -> None:
    text = render(sample())
    values = [
        int(line.rsplit(" ", 1)[1])
        for line in text.splitlines()
        if line.startswith("pgqueue_enqueue_to_start_seconds_bucket")
    ]
    assert values == sorted(values)
    assert len(values) == len(WAIT_BUCKETS) + 1
    assert 'pgqueue_enqueue_to_start_seconds_bucket{queue="default",le="+Inf"} 2' in text
    assert 'pgqueue_enqueue_to_start_seconds_count{queue="default"} 2' in text
    assert 'pgqueue_job_failures_total{queue="default"} 3' in text


def test_label_values_are_escaped() -> None:
    m = QueueMetrics(depth={('we"ird\\q', "queued"): 1})
    assert 'queue="we\\"ird\\\\q"' in render(m)


def test_empty_database_renders_headers_only() -> None:
    lines = render(QueueMetrics()).splitlines()
    assert lines and all(line.startswith("#") for line in lines)
