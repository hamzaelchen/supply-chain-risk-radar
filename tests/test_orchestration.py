"""Le job Dagster : ordre des étapes, planning désactivé par défaut, arrêt à la première erreur."""

import pytest
from dagster import DefaultScheduleStatus

from orchestration import definitions as orch

STEPS = ["produce", "bronze", "silver", "proximity", "gold", "export"]


def commands_of(calls):
    return [" ".join(a[:3]) if len(a) >= 3 else " ".join(a) for a, _ in calls]


@pytest.fixture
def recorder(monkeypatch):
    calls = []

    def fake(context, argv, cwd=orch.ROOT):
        calls.append((argv, cwd))

    monkeypatch.setattr(orch, "run_command", fake)
    return calls


def test_job_chains_the_six_steps_in_order(recorder):
    result = orch.pipeline.execute_in_process()
    assert result.success
    run_steps = [e.step_key for e in result.get_step_success_events()]
    assert run_steps == STEPS


def test_every_step_calls_the_same_module_as_the_command_line(recorder):
    orch.pipeline.execute_in_process()
    joined = [" ".join(argv) for argv, _ in recorder]
    assert any("ingestion.gdelt_producer" in c for c in joined)
    assert any("streaming.bronze_gdelt_events" in c for c in joined)
    assert any("silver.gdelt_events" in c and "--until-caught-up" in c for c in joined)
    assert any("silver.proximity" in c for c in joined)
    assert [c for c in joined if c.endswith("--profiles-dir .")] and len(
        [c for c in joined if "dbt" in c]
    ) == 3
    assert any("transform/export.py" in c for c in joined)


def test_dbt_runs_in_the_transform_folder(recorder):
    orch.pipeline.execute_in_process()
    dbt_calls = [(argv, cwd) for argv, cwd in recorder if argv[0].endswith("dbt")]
    assert [argv[1] for argv, _ in dbt_calls] == ["seed", "run", "test"]
    assert {cwd for _, cwd in dbt_calls} == {orch.ROOT / "transform"}


def test_a_failing_step_stops_the_chain(monkeypatch):
    seen = []

    def fake(context, argv, cwd=orch.ROOT):
        seen.append(argv)
        if "silver.gdelt_events" in argv:
            raise orch.Failure(description="silver en panne")

    monkeypatch.setattr(orch, "run_command", fake)
    result = orch.pipeline.execute_in_process(raise_on_error=False)
    assert not result.success
    assert not any("silver.proximity" in " ".join(a) for a in seen)
    assert [e.step_key for e in result.get_step_success_events()] == ["produce", "bronze"]


def test_schedule_is_every_15_minutes_and_not_started_by_default():
    schedule = orch.pipeline_every_15_minutes
    assert schedule.cron_schedule == "*/15 * * * *"
    assert schedule.execution_timezone == "UTC"
    assert schedule.default_status == DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "pipeline"


def test_definitions_expose_the_job_and_the_schedule():
    assert [j.name for j in orch.defs.jobs] == ["pipeline"]
    assert [s.name for s in orch.defs.schedules] == ["pipeline_every_15_minutes"]


def test_run_command_raises_with_the_output_tail_on_failure():
    from dagster import build_op_context

    context = build_op_context()
    with pytest.raises(orch.Failure) as error:
        orch.run_command(context, [orch.PYTHON, "-c", "import sys; print('boom'); sys.exit(3)"])
    assert "code 3" in str(error.value.description)
    assert "boom" in error.value.metadata["sortie"].text
