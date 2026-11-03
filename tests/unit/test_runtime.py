"""Unit tests for the notebook runtime context (replaces per-notebook widget boilerplate)."""
from vstone_framework.runtime import STANDARD_PARAMS, get_context


class FakeWidgets:
    def __init__(self, preset=None):
        self.values, self.preset = {}, dict(preset or {})

    def text(self, name, default):
        self.values.setdefault(name, self.preset.get(name, default))   # job parameter wins over default

    def get(self, name):
        return self.values[name]


class FakeDbutils:
    def __init__(self, preset=None):
        self.widgets = FakeWidgets(preset)


def test_defaults_build_paths_and_schemas():
    ctx = get_context(FakeDbutils())
    assert ctx.catalog == "vstone" and ctx.run_id == "manual"
    assert ctx.raw_path == "/Volumes/vstone/landing/raw"
    assert ctx.chunks_root == "/Volumes/vstone/landing/chunks"
    assert ctx.checkpoints_root == "/Volumes/vstone/landing/checkpoints"
    assert (ctx.bronze, ctx.silver, ctx.gold, ctx.ops) == ("vstone.bronze", "vstone.silver", "vstone.gold", "vstone.ops")


def test_job_parameters_override_defaults():
    ctx = get_context(FakeDbutils({"catalog": "vstone_prod", "run_id": "987"}))
    assert ctx.bronze == "vstone_prod.bronze" and ctx.run_id == "987"
    assert ctx.raw_path == "/Volumes/vstone_prod/landing/raw"


def test_extra_parameters_use_config_defaults_when_present():
    dbu = FakeDbutils()
    ctx = get_context(dbu, {"chunk1_end_date": "", "force_rechunk": "false"})
    assert ctx.param("chunk1_end_date") == "2005-12-31"       # from conf defaults
    assert ctx.param("force_rechunk") == "false"
    assert set(STANDARD_PARAMS) <= set(dbu.widgets.values)
