from pathlib import Path

import main


def test_env_file_exists_and_has_assignments() -> None:
    env_path = Path('.env')
    assert env_path.exists(), '.env file is missing'

    content = env_path.read_text(encoding='utf-8').splitlines()
    assignments = [line for line in content if line.strip() and not line.strip().startswith('#') and '=' in line]
    assert assignments, '.env does not contain any KEY=VALUE assignment'


def test_data_files_are_readable() -> None:
    data_file = Path('data/raw_jobs.csv')
    assert data_file.exists(), 'Expected data/raw_jobs.csv to exist'

    first_chunk = data_file.read_text(encoding='utf-8-sig')[:200]
    assert len(first_chunk) > 0, 'raw_jobs.csv appears to be empty'


def test_cli_help_mode_does_not_run_pipeline(monkeypatch) -> None:
    called = {'pipeline': False}

    def _fake_pipeline() -> None:
        called['pipeline'] = True

    monkeypatch.setattr(main, 'run_pipeline', _fake_pipeline)

    exit_code = main.main([])

    assert exit_code == 0
    assert called['pipeline'] is False


def test_cli_run_flag_triggers_pipeline(monkeypatch) -> None:
    called = {'pipeline': False}

    def _fake_pipeline() -> None:
        called['pipeline'] = True

    monkeypatch.setattr(main, 'run_pipeline', _fake_pipeline)

    exit_code = main.main(['--run'])

    assert exit_code == 0
    assert called['pipeline'] is True
