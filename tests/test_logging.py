from pathlib import Path


def test_resolve_log_file_path_keeps_default_name(tmp_path):
    from powerbuddy.main import resolve_log_file_path

    candidate = tmp_path / "var" / "log"
    candidate.mkdir(parents=True, exist_ok=True)

    resolved = resolve_log_file_path(candidate)

    assert resolved.name == "powerbuddy.log"
    assert str(resolved).endswith("powerbuddy.log")
