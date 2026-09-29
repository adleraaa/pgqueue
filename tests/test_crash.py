"""Small version of bench/crash.py: kill worker processes and lose nothing."""

from __future__ import annotations

from bench.crash import run_crash_test


def test_killed_workers_lose_no_jobs(db_url: str) -> None:
    result = run_crash_test(db_url, jobs=150, workers=3, kills=5, kill_interval=0.3, seed=1)

    assert result["jobs_in_table"] == 150
    assert result["non_terminal_jobs"] == 0
    assert result["final_states"] == {"succeeded": 150}
    assert result["never_delivered"] == 0
    # At-least-once: duplicates are allowed, but each one must correspond to a
    # lease that expired (a killed worker's job being handed out again).
    assert result["duplicate_deliveries"] <= result["expired_leases_reaped"]
