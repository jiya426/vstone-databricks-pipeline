"""Unit tests for the shared per-table runner."""
import pytest

from vstone_framework.runner import raise_if_failed, results_rows, run_per_table


def test_runner_logs_success_and_failure_and_continues():
    logged = []
    logger = lambda table, rows, files, status, started, error: logged.append((table, rows, status, error))

    def action(item):
        if item == "bad":
            raise ValueError("boom")
        return 10, 2

    results = run_per_table(["a", "bad", "c"], lambda x: f"t_{x}", action, logger)
    assert [r.status for r in results] == ["SUCCESS", "FAILED", "SUCCESS"]       # loop did not stop
    assert logged[1] == ("t_bad", None, "FAILED", "boom")
    assert results_rows(results)[0] == ("t_a", 2, 10, "SUCCESS", None)
    with pytest.raises(RuntimeError, match=r"\['t_bad'\]"):
        raise_if_failed(results, "Test load", "ops.pipeline_run_log")


def test_raise_if_failed_is_silent_when_all_succeed():
    results = run_per_table(["a"], str, lambda x: (0, None), lambda *a: None)
    raise_if_failed(results, "Test", "log")
