"""Unit tests for code parameterization: widgets (job + task parameters) -> runtime context."""
import pytest

from vstone_framework.runtime import STANDARD_PARAMS, declare_widgets, get_context, widget_defaults


class FakeWidgets:
    """Behaves like dbutils.widgets: a job/task parameter with the same name wins over the declared default."""

    def __init__(self, job_params=None):
        self.values, self.job_params = {}, dict(job_params or {})

    def text(self, name, default, label=None):
        self.values.setdefault(name, self.job_params.get(name, default))

    def get(self, name):
        if name in self.values:
            return self.values[name]
        if name in self.job_params:            # a job parameter is readable even without a declared widget
            return self.job_params[name]
        raise ValueError(f"No input widget named {name} is defined")


class FakeDbutils:
    def __init__(self, job_params=None):
        self.widgets = FakeWidgets(job_params)


def test_manual_run_uses_config_defaults():
    dbu = FakeDbutils()
    declare_widgets(dbu, STANDARD_PARAMS)
    ctx = get_context(dbu)
    assert ctx.catalog == "vstone" and ctx.run_id == "manual" and ctx.env == "dev"
    assert ctx.raw_path == "/Volumes/vstone/landing/raw"
    assert ctx.chunks_root == "/Volumes/vstone/landing/chunks"
    assert ctx.checkpoints_root == "/Volumes/vstone/landing/checkpoints"
    assert (ctx.bronze, ctx.silver, ctx.gold, ctx.ops) == ("vstone.bronze", "vstone.silver", "vstone.gold", "vstone.ops")


def test_job_parameters_override_widget_defaults():
    dbu = FakeDbutils({"catalog": "vstone_prod", "run_id": "987", "env": "prod"})
    declare_widgets(dbu, STANDARD_PARAMS)
    ctx = get_context(dbu)
    assert ctx.bronze == "vstone_prod.bronze" and ctx.run_id == "987" and ctx.env == "prod"
    assert ctx.raw_path == "/Volumes/vstone_prod/landing/raw"


def test_task_parameters_override_and_are_typed():
    dbu = FakeDbutils({"chunk_no": "3", "include_reference": "false", "levels": "zip,state"})
    declare_widgets(dbu, ["chunk_no", "include_reference", "levels"])
    ctx = get_context(dbu, ["chunk_no", "include_reference", "levels"])
    assert ctx.int_param("chunk_no") == 3
    assert ctx.bool_param("include_reference") is False
    assert ctx.list_param("levels", ["state", "zip", "city"]) == ["zip", "state"]


def test_list_param_all_and_unknown_values():
    dbu = FakeDbutils({"levels": "all", "bad": "zip,moon"})
    ctx = get_context(dbu, ["levels"])
    assert ctx.list_param("levels", ["state", "zip"]) == ["state", "zip"]
    ctx2 = get_context(FakeDbutils({"levels": "zip,moon"}), ["levels"])
    with pytest.raises(ValueError, match="unknown values"):
        ctx2.list_param("levels", ["state", "zip"])


def test_missing_or_empty_widget_falls_back_to_config_default():
    ctx = get_context(FakeDbutils({"catalog": ""}), ["force_rechunk"])
    assert ctx.catalog == widget_defaults()["catalog"]
    assert ctx.param("force_rechunk") == "false"


def test_names_from_config_not_code():
    ctx = get_context(FakeDbutils())
    assert ctx.ops_table("chunk_manifest") == "vstone.ops.chunk_manifest"
    assert ctx.metadata_path("fields_per_level") == "/Volumes/vstone/landing/raw/fields_per_level.json"
