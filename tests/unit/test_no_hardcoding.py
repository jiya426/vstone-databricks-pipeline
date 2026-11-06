"""Guards for the trainer's rules: 100% config-driven code and full code parameterization.

1. No hard-coded values in code: catalog/schema names, /Volumes paths, raw file names, ops table names,
   e-mail addresses or dates may only live in conf/vstone_config.json or databricks.yml.
2. Every widget a job task's notebook declares is passed by the job (job parameter or task parameter),
   and every DLT setting the pipeline reads is in its configuration block - nothing silently uses a default.
"""
import ast
import glob
import os
import re

import pytest
import yaml

from vstone_framework.config import load_config

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CODE_FILES = sorted(glob.glob(os.path.join(ROOT, "src", "**", "*.py"), recursive=True))
CFG = load_config()

# The only two places allowed to build a /Volumes path from parameters
VOLUME_PATH_BUILDERS = {os.path.join("src", "vstone_framework", "runtime.py"),
                        os.path.join("src", "pipelines", "bronze_csv_dlt.py")}


def string_constants(path):
    """Every string literal in the code, excluding docstrings and comments (MAGIC markdown cells are comments)."""
    tree = ast.parse(open(path, encoding="utf-8").read())
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docstrings.add(id(first.value))
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]


def forbidden_literals():
    raw_files = list(CFG["time_series_files"]) + list(CFG["reference_files"]) + list(CFG["metadata_files"])
    ops_tables = [v for v in CFG["ops_tables"].values()]
    chunk_files = [c["file_name"] for c in CFG["chunks"].values()]
    return raw_files + ops_tables + chunk_files


@pytest.mark.parametrize("path", CODE_FILES, ids=lambda p: os.path.relpath(p, ROOT))
def test_no_hardcoded_values_in_code(path):
    rel = os.path.relpath(path, ROOT)
    problems = []
    for s in string_constants(path):
        if "/Volumes/" in s and rel not in VOLUME_PATH_BUILDERS:
            problems.append(f"/Volumes path: {s!r}")
        if re.search(r"\b{}\.({})\b".format(re.escape(CFG["defaults"]["catalog"]), "|".join(CFG["schemas"])), s):
            problems.append(f"catalog.schema name: {s!r}")
        if re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", s):
            problems.append(f"e-mail address: {s!r}")
        if re.search(r"\b(19|20)\d{2}-\d{2}-\d{2}\b", s):
            problems.append(f"date: {s!r}")
        if s in CFG["ops_tables"]:          # a config KEY, e.g. ctx.ops_table("chunk_manifest")
            continue
        for literal in forbidden_literals():
            if literal in s:
                problems.append(f"name from config written in code: {literal!r} in {s!r}")
    assert problems == [], f"{rel}: move these values to conf/vstone_config.json or a parameter: {problems}"


# ------------------------------------------------------------------ parameterization
def load_bundle_resources():
    jobs, pipelines = {}, {}
    for f in glob.glob(os.path.join(ROOT, "resources", "*.yml")):
        res = (yaml.safe_load(open(f)) or {}).get("resources", {})
        jobs.update(res.get("jobs", {}))
        pipelines.update(res.get("pipelines", {}))
    return jobs, pipelines


def notebook_widgets(path):
    text = open(path, encoding="utf-8").read()
    return set(re.findall(r'dbutils\.widgets\.(?:text|dropdown)\(\s*"([a-z0-9_]+)"', text))


JOBS, PIPELINES = load_bundle_resources()
NOTEBOOK_TASKS = [(job, task) for job, spec in JOBS.items() for task in spec["tasks"] if "notebook_task" in task]


@pytest.mark.parametrize("job, task", NOTEBOOK_TASKS, ids=lambda x: x if isinstance(x, str) else x["task_key"])
def test_job_passes_every_widget_the_notebook_declares(job, task):
    nb = os.path.normpath(os.path.join(ROOT, "resources", task["notebook_task"]["notebook_path"]))
    widgets = notebook_widgets(nb)
    assert widgets, f"{nb} declares no widgets - parameters must arrive through widgets"
    passed = {p["name"] for p in JOBS[job].get("parameters", [])} | set(task["notebook_task"].get("base_parameters", {}))
    assert widgets - passed == set(), f"{job}.{task['task_key']}: not passed by the job: {sorted(widgets - passed)}"


def test_job_and_task_parameters_come_from_bundle_variables():
    for job, spec in JOBS.items():
        for p in spec.get("parameters", []):
            assert str(p["default"]).startswith(("${var.", "${bundle.", "{{job.")), (job, p)
        for task in spec["tasks"]:
            for name, value in task.get("notebook_task", {}).get("base_parameters", {}).items():
                assert str(value).startswith("${var."), (job, task["task_key"], name, value)


def test_dlt_pipeline_reads_only_configured_settings():
    code = open(os.path.join(ROOT, "src", "pipelines", "bronze_csv_dlt.py"), encoding="utf-8").read()
    read = set(re.findall(r'spark\.conf\.get\(\s*"(vstone\.[a-z_]+)"', code))
    configured = set(PIPELINES["bronze_csv_dlt"]["configuration"])
    assert read and read - configured == set(), sorted(read - configured)


def test_every_notebook_ends_with_what_it_achieved():
    for nb in glob.glob(os.path.join(ROOT, "src", "notebooks", "*.py")):
        tail = open(nb, encoding="utf-8").read().rstrip().split("# COMMAND ----------")[-1]
        assert "What this notebook achieved" in tail, os.path.basename(nb)
