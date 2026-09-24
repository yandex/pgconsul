from configparser import RawConfigParser
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def make_runner(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    commands = {
        'python3': '''#!/bin/sh
case "$1 $2" in
  '-m pip') exit 0 ;;
  '-m behave')
    for feature do :; done
    feature=$(basename "$feature")
    printf 'run:%s\\n' "$feature" >> "$EVENTS"
    test -f "$READY" || exit 1
    rm "$READY"
    test "$FAIL_FEATURE" != "$feature"
    ;;
esac
''',
        'docker': '''#!/bin/sh
case "$*" in
  *'/root/main.py'*)
    printf 'random\\n' >> "$EVENTS"
    printf '%s\\n' "$*" >> "$RANDOM_ARGS"
    test -f "$READY" || exit 1
    rm "$READY"
    test "$RANDOM_FAIL" != yes
    ;;
esac
''',
    }
    for name, script in commands.items():
        path = bin_dir / name
        path.write_text(script)
        path.chmod(0o755)

    features = tmp_path / 'tests/faultstorm/features'
    features.mkdir(parents=True)
    for name in ['first', 'second']:
        (features / f'{name}.feature').touch()
    saver = tmp_path / 'docker/faultstorm/save_logs.sh'
    saver.parent.mkdir(parents=True)
    saver.write_text('#!/bin/sh\nprintf "save\\n" >> "$EVENTS"\n')
    saver.chmod(0o755)
    (tmp_path / 'Makefile').write_text(f'''include {ROOT / 'Makefile'}
faultstorm_restart:
\t@printf 'prepare\\n' >> "$(EVENTS)"
\t@printf '%s\\n' "$(FAULTSTORM_PGCONSUL_CONFIG)" >> "$(PROFILE_EVENTS)"
\t@test "$(PREPARE_FAIL)" != yes
\t@mkdir -p logs
\t@touch "$(READY)"
''')
    ready = tmp_path / 'ready'
    ready.touch()
    events = tmp_path / 'events'
    profile_events = tmp_path / 'profile_events'
    random_args = tmp_path / 'random_args'
    env = dict(
        os.environ,
        PATH=f'{bin_dir}{os.pathsep}{os.environ["PATH"]}',
        READY=str(ready),
        EVENTS=str(events),
        PROFILE_EVENTS=str(profile_events),
        RANDOM_ARGS=str(random_args),
    )

    def run(target='faultstorm_behave', **options):
        result = subprocess.run(
            [shutil.which('make'), '--no-print-directory', target, 'FAULTSTORM_COMMIT=unit-test',
             *(f'{key}={value}' for key, value in options.items())],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
        )
        return result, events.read_text().splitlines() if events.exists() else []

    run.profile_events = profile_events
    run.random_args = random_args
    return run


def test_features_get_fresh_cluster_after_previous_logs_are_saved(make_runner):
    result, events = make_runner()

    assert result.returncode == 0, result.stdout + result.stderr
    assert events == ['prepare', 'run:first.feature', 'save', 'prepare', 'run:second.feature', 'save']


def test_feature_failure_is_preserved_after_next_feature_passes(make_runner):
    result, events = make_runner(FAIL_FEATURE='first.feature')

    assert result.returncode != 0
    assert events == ['prepare', 'run:first.feature', 'save', 'prepare', 'run:second.feature', 'save']


def test_maintenance_resetup_uses_lossy_profile(make_runner):
    result, _ = make_runner(FAULTSTORM_FEATURE='tests/faultstorm/features/maintenance_resetup.feature')

    assert result.returncode == 0, result.stdout + result.stderr
    assert make_runner.profile_events.read_text().splitlines() == [
        './docker/faultstorm/pgconsul_faultstorm_lossy.conf'
    ]


def test_lossy_profile_does_not_use_wal_durability_reads():
    config = RawConfigParser()
    config.read(ROOT / 'docker/faultstorm/pgconsul_faultstorm_lossy.conf')

    assert config.getboolean('global', 'quorum_commit') is False
    assert config.getboolean('global', 'use_lwaldump') is False


def test_faultstorm_build_keeps_images_for_incremental_builds():
    makefile = (ROOT / 'Makefile').read_text()
    build = makefile.split('faultstorm_build:', 1)[1].split('\nfaultstorm_rebuild:', 1)[0]

    assert '--rmi all' not in build
    assert 'faultstorm_rebuild:' in makefile


def test_faultstorm_build_keeps_its_docker_inputs_stable():
    makefile = (ROOT / 'Makefile').read_text()
    dockerignore_path = ROOT / '.dockerignore'
    build = makefile.split('faultstorm_build:', 1)[1].split('\nfaultstorm_rebuild:', 1)[0]

    assert '-f test_ssh_key' in build
    assert 'yes | ssh-keygen' not in build
    assert dockerignore_path.is_file()
    dockerignore = dockerignore_path.read_text().splitlines()
    assert 'logs/' in dockerignore
    assert 'venv/' in dockerignore


@pytest.mark.parametrize('target', ['faultstorm_behave', 'faultstorm_test'])
def test_failed_cluster_setup_stops_tests(make_runner, target):
    result, events = make_runner(target, PREPARE_FAIL='yes')

    assert result.returncode != 0
    assert events == ['prepare']


@pytest.mark.parametrize('fails', ['yes', 'no'])
def test_random_run_gets_fresh_cluster_and_preserves_result(make_runner, fails):
    result, events = make_runner('faultstorm_test', RANDOM_FAIL=fails)

    assert (result.returncode != 0) == (fails == 'yes')
    assert events == ['prepare', 'random', 'save']


def test_random_run_keeps_read_validation_open_through_primary_promotion(make_runner):
    result, _ = make_runner('faultstorm_test')

    assert result.returncode == 0, result.stdout + result.stderr
    assert '--read-duration 20' in make_runner.random_args.read_text()
