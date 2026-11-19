"""Orchestration rules from the PDF, checked on the bundle files: no retries, failure e-mail on every job,
master job runs the stages in the right order."""
import glob
import os

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JOBS = {}
for f in glob.glob(os.path.join(ROOT, "resources", "*.yml")):
    JOBS.update(((yaml.safe_load(open(f)) or {}).get("resources", {}) or {}).get("jobs", {}))


def test_no_task_retries_anywhere():
    for job, spec in JOBS.items():
        for t in spec["tasks"]:
            assert t.get("max_retries", 0) == 0, (job, t["task_key"])


def test_every_job_emails_on_failure():
    for job, spec in JOBS.items():
        assert spec.get("email_notifications", {}).get("on_failure") == ["${var.notification_email}"], job


def test_master_pipeline_runs_stages_in_order():
    tasks = {t["task_key"]: t for t in JOBS["master_pipeline"]["tasks"]}
    order = ["preflight", "data_chunking", "bronze_ingestion", "silver_transform", "gold", "security"]
    assert list(tasks) == order
    for prev, cur in zip(order, order[1:]):
        assert tasks[cur]["depends_on"] == [{"task_key": prev}]
    for key in order[1:]:
        ref = tasks[key]["run_job_task"]["job_id"]
        assert ref == f"${{resources.jobs.{key}.id}}" and key in JOBS


def test_master_pipeline_has_the_failure_switch():
    params = {p["name"]: p["default"] for p in JOBS["master_pipeline"]["parameters"]}
    assert params["simulate_failure"] == "${var.simulate_failure}"
