import sys
from unittest.mock import Mock, patch

import pytest

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli


DEPRECATION = (
    "Deprecated: Replace '-m/--mode {value}' with positional '{replacement}' "
    "(recommended syntax: 'mnt {replacement}'); keep all other options unchanged.\n"
)


@pytest.mark.parametrize('command', ['maint', 'maintenance', 'mnt'])
@pytest.mark.parametrize('action, mode', [
    ('on', 'enable'), ('off', 'disable'), ('get', 'get'), ('show', 'get'), ('enable', 'enable'), ('disable', 'disable'),
])
@pytest.mark.parametrize('options', [['-w', '-t', '60'], ['--timeout=60', '--wait_all']])
def test_maintenance_subcommands(monkeypatch, capsys, command, action, mode, options):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, action, *options])
    opts = cli.parse_args()
    assert opts.action is cli.maintenance
    assert (opts.mode, opts.wait_all, opts.timeout) == (mode, True, 60)
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('arguments, wait, timeout', [
    ([], False, 300), (['-w'], True, 300), (['-t', '60', '-w'], True, 60),
    (['--timeout=0'], False, 0), (['-w', '--timeout', '300'], True, 300),
])
def test_default_maintenance_enables_without_warning(monkeypatch, capsys, command, arguments, wait, timeout):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, *arguments])
    opts = cli.parse_args()
    assert (opts.mode, opts.wait_all, opts.timeout) == ('enable', wait, timeout)
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('action', ['on', 'off'])
def test_legacy_mode_rejects_positional_only_actions(monkeypatch, action):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'maintenance', '-m', action])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 2


@pytest.mark.parametrize('arguments, warning', [
    (['start'], ''),
    (['-m', 'enable'], DEPRECATION.format(value='enable', replacement='start')),
    ([], ''),
])
def test_maintenance_command_controls_choices_default_and_recommendation(monkeypatch, capsys, arguments, warning):
    monkeypatch.setitem(cli.MAINTENANCE_ACTIONS, 'enable', 'start')
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'maintenance', *arguments])
    assert cli.parse_args().mode == 'enable'
    assert capsys.readouterr().err == warning


@pytest.mark.parametrize('arguments, mode, replacement', [
    (['maintenance', '-m', 'enable'], 'enable', 'on'),
    (['maint', '--mode', 'disable'], 'disable', 'off'),
    (['maintenance', '--mode=show'], 'get', 'show'),
    (['maint', '-m', 'disable'], 'disable', 'off'),
    (['maint', '-m', 'show'], 'get', 'show'),
    (['maint', '--mode', 'enable'], 'enable', 'on'),
    (['maintenance', '-m', 'get'], 'get', 'show'),
    (['maint', '--mode', 'get'], 'get', 'show'),
    (['mnt', '--mode=get'], 'get', 'show'),
])
def test_legacy_maintenance_keeps_defaults_and_warns(monkeypatch, capsys, arguments, mode, replacement):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments])
    opts = cli.parse_args()
    assert (opts.mode, opts.wait_all, opts.timeout) == (mode, False, 300)
    error = capsys.readouterr().err
    assert error == DEPRECATION.format(value=arguments[-1].split('=')[-1], replacement=replacement)


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('flag', ['-m', '--mode'])
@pytest.mark.parametrize('options_first', [False, True])
@pytest.mark.parametrize('value, mode, replacement', [
    ('enable', 'enable', 'on'), ('disable', 'disable', 'off'),
    ('show', 'get', 'show'), ('get', 'get', 'show'),
])
def test_legacy_mode_warns_for_every_command_alias(monkeypatch, capsys, command, flag, options_first, value, mode, replacement):
    arguments = ['-w', '-t', '60', flag, value] if options_first else [flag, value, '-w', '-t', '60']
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, *arguments])
    opts = cli.parse_args()
    assert (opts.mode, opts.wait_all, opts.timeout) == (mode, True, 60)
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err == DEPRECATION.format(value=value, replacement=replacement)


def test_legacy_disable_executes_without_usage(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'maintenance', '-m', 'disable'])
    monkeypatch.setattr(cli, 'read_config', Mock())
    monkeypatch.setattr(cli, 'init_logging', Mock())
    zk = Mock()
    with patch.object(cli, 'create_zk') as create_zk:
        create_zk.return_value.__enter__.return_value = zk
        cli.entry()
    zk.write_maintenance_status.assert_called_once_with('disable')
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err == DEPRECATION.format(value='disable', replacement='off')


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('flag', [None, '-m', '--mode', '--mode='])
@pytest.mark.parametrize('value, suggestion', [('enabled', 'enable'), ('disabled', 'disable')])
def test_removed_maintenance_aliases_are_rejected(monkeypatch, capsys, command, flag, value, suggestion):
    if flag == '--mode=':
        arguments = [flag + value]
    else:
        arguments = [flag, value] if flag else [value]
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, *arguments])
    read_config = Mock(side_effect=AssertionError('must not read configuration'))
    monkeypatch.setattr(cli, 'read_config', read_config)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 2
    output = capsys.readouterr()
    option = ' -m' if flag else ''
    assert output.out == ''
    assert output.err == f"The most similar command is 'pgconsul-util maintenance{option} {suggestion}'\n"
    read_config.assert_not_called()


@pytest.mark.parametrize('arguments', [
    ['show'], ['get'], ['-m', 'show'], ['-m', 'get'], ['--mode', 'show'], ['--mode', 'get'], ['--mode=show'], ['--mode=get'],
])
@pytest.mark.parametrize('status, expected', [('enable', 'enabled'), ('disable', 'disabled'), (None, 'disabled')])
def test_status_synonyms_keep_stdout_separate_from_deprecation(monkeypatch, capsys, arguments, status, expected):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'maintenance', *arguments])
    monkeypatch.setattr(cli, 'read_config', Mock())
    monkeypatch.setattr(cli, 'init_logging', Mock())
    zk = Mock()
    zk.get_maintenance_status.return_value = status
    with patch.object(cli, 'create_zk') as create_zk:
        create_zk.return_value.__enter__.return_value = zk
        cli.entry()
    zk.get_maintenance_status.assert_called_once_with()
    zk.write_maintenance_status.assert_not_called()
    output = capsys.readouterr()
    assert output.out == f'{expected}\n'
    warning = DEPRECATION.format(value=arguments[-1].split('=')[-1], replacement='show') if arguments[0].startswith('-') else ''
    assert output.err == warning


@pytest.mark.parametrize('arguments, suggestion', [
    (['maint', 'om'], 'pgconsul-util maintenance on'),
    (['maintenance', 'g'], 'pgconsul-util maintenance get'),
    (['maintenance', 'of'], 'pgconsul-util maintenance off'),
    (['maint', '-m', 'enabel'], 'pgconsul-util maintenance -m enable'),
    (['maint', '--mode', 'shwo'], 'pgconsul-util maintenance -m show'),
    (['switcover'], 'pgconsul-util switchover'),
])
def test_typo_suggests_command_without_executing_it(monkeypatch, capsys, arguments, suggestion):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments])
    read_config = Mock(side_effect=AssertionError('must not read configuration'))
    monkeypatch.setattr(cli, 'read_config', read_config)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 2
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err == f"The most similar command is '{suggestion}'\n"
    read_config.assert_not_called()


@pytest.mark.parametrize('arguments, command', [
    (['zzzzzzzz'], 'pgconsul-util'),
    (['maintenance', 'zzzzzzzz'], 'pgconsul-util maintenance'),
    (['maint', 'zzzzzzzz'], 'pgconsul-util maintenance'),
    (['mnt', '-m', 'zzzzzzzz'], 'pgconsul-util maintenance'),
])
def test_unknown_choice_shows_only_help_hint(monkeypatch, capsys, arguments, command):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments])
    read_config = Mock(side_effect=AssertionError('must not read configuration'))
    monkeypatch.setattr(cli, 'read_config', read_config)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 2
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err == f"See '{command} --help' for help.\n"
    read_config.assert_not_called()


@pytest.mark.parametrize('arguments', [
    ['maint', 'on', '-m', 'disable'],
    ['maint', '-m', 'enable', 'off'],
    ['maint', 'on', 'off'],
    ['maint', '-m'],
    ['maint', 'on', '--unknown'],
    ['maint', 'on', '-t', 'invalid'],
    ['-m', 'enable', 'maintenance'],
    ['mnt', 'on', '-c', '/review/custom.conf'],
    ['mnt', 'on', '--zk', 'review-zk:2181'],
    ['mnt', 'on', '--zk-prefix', '/review/cluster'],
])
def test_invalid_or_conflicting_arguments_are_rejected(monkeypatch, arguments):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 2


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('action', ['on', 'off', 'get', 'show', 'enable', 'disable'])
@pytest.mark.parametrize('options', [['-w'], ['-t', '60'], ['-t', '300'], ['--timeout=0', '--wait_all']])
def test_action_after_options_is_rejected_before_config_or_zk(monkeypatch, capsys, command, action, options):
    monkeypatch.setattr(sys, 'argv', [
        'pgconsul-util', '-c', '/review/custom.conf', '--zk', 'review-zk:2181', '--zk-prefix', '/review/cluster',
        command, *options, action,
    ])
    read_config = Mock(side_effect=AssertionError('must not read configuration'))
    create_zk = Mock(side_effect=AssertionError('must not connect to ZooKeeper'))
    monkeypatch.setattr(cli, 'read_config', read_config)
    monkeypatch.setattr(cli, 'create_zk', create_zk)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 2
    output = capsys.readouterr()
    assert output.out == ''
    assert 'action must precede its options' in output.err
    read_config.assert_not_called()
    create_zk.assert_not_called()


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('arguments', [
    ['-m', 'enable', '-m', 'enable'],
    ['-m', 'enable', '--mode', 'disable'],
    ['--mode=show', '-w', '--mode=get'],
    ['--mode', 'disable', '-t', '60', '-mdisable'],
    ['on', '-m', 'enable'],
    ['off', '--mode=disable'],
    ['get', '--mode', 'show'],
])
def test_duplicate_maintenance_mode_is_rejected_before_config_or_zk(monkeypatch, capsys, command, arguments):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, *arguments])
    read_config = Mock(side_effect=AssertionError('must not read configuration'))
    create_zk = Mock(side_effect=AssertionError('must not connect to ZooKeeper'))
    monkeypatch.setattr(cli, 'read_config', read_config)
    monkeypatch.setattr(cli, 'create_zk', create_zk)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 2
    output = capsys.readouterr()
    assert output.out == ''
    assert 'maintenance action is specified more than once' in output.err
    assert 'Deprecated:' not in output.err
    read_config.assert_not_called()
    create_zk.assert_not_called()


@pytest.mark.parametrize('arguments', [['-w', '--', 'off'], ['-m', 'enable', '--', 'off']])
def test_end_of_options_does_not_allow_a_late_action(monkeypatch, arguments):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'mnt', *arguments])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 2


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
def test_global_option_values_do_not_affect_action_order(monkeypatch, capsys, command):
    monkeypatch.setattr(sys, 'argv', [
        'pgconsul-util', '-c', 'off', '--zk', 'mnt', '--zk-prefix', 'maintenance', command, 'off', '-w', '-t', '60',
    ])
    opts = cli.parse_args()
    assert (opts.config_file, opts.zk_hosts, opts.zk_lockpath_prefix) == ('off', 'mnt', 'maintenance')
    assert (opts.mode, opts.wait_all, opts.timeout) == ('disable', True, 60)
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
def test_action_after_end_of_options_marker(monkeypatch, capsys, command):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, '--', 'off'])
    assert cli.parse_args().mode == 'disable'
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('arguments, flags', [
    ([], ['--config', '--zk', '--zk-prefix']),
    (['maintenance'], ['--mode', '--wait_all', '--timeout']),
    (['maint'], ['--mode', '--wait_all', '--timeout']),
    (['mnt'], ['--mode', '--wait_all', '--timeout']),
    (['switchover'], ['--destination', '--block', '--timeout', '--yes', '--reset']),
    (['info'], ['--short', '--json']),
])
def test_help_keeps_flags(monkeypatch, capsys, arguments, flags):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments, '--help'])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 0
    help_text = capsys.readouterr().out
    assert help_text.startswith('usage:')
    assert all(flag in help_text for flag in flags)
    if arguments and arguments[0] in ('maintenance', 'maint', 'mnt'):
        assert 'maintenance [ACTION] [OPTIONS]' in help_text.splitlines()[0]


@pytest.mark.parametrize('arguments', [
    ['maint', 'on', '-w', '-t', '10'],
    ['maint', 'on', '-t', '10', '-w'],
    ['maintenance', 'enable', '-w', '-t', '10'],
    ['maintenance', '-m', 'enable', '-w', '-t', '10'],
])
def test_both_formats_use_existing_maintenance_handler(monkeypatch, arguments):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', *arguments])
    zk = Mock()
    with patch.object(cli, 'create_zk') as create_zk, patch.object(cli, 'enable_maintenance') as enable:
        create_zk.return_value.__enter__.return_value = zk
        opts = cli.parse_args()
        opts.action(opts, Mock())
    enable.assert_called_once_with(zk, 10, True)


@pytest.mark.parametrize('command', ['maintenance', 'maint', 'mnt'])
@pytest.mark.parametrize('arguments, mode', [
    ([], 'enable'), (['on'], 'enable'), (['enable'], 'enable'),
    (['off'], 'disable'), (['disable'], 'disable'), (['show'], 'get'), (['get'], 'get'),
])
def test_positional_dispatch_and_implicit_enable(monkeypatch, capsys, command, arguments, mode):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', command, *arguments])
    monkeypatch.setattr(cli, 'read_config', Mock())
    monkeypatch.setattr(cli, 'init_logging', Mock())
    zk = Mock()
    zk.get_maintenance_status.return_value = 'disable'
    with patch.object(cli, 'create_zk') as create_zk, patch.object(cli, 'enable_maintenance') as enable:
        create_zk.return_value.__enter__.return_value = zk
        cli.entry()
    if mode == 'enable':
        enable.assert_called_once_with(zk, 300, False)
    else:
        enable.assert_not_called()
    if mode == 'disable':
        zk.write_maintenance_status.assert_called_once_with('disable')
    else:
        zk.write_maintenance_status.assert_not_called()
    if mode == 'get':
        zk.get_maintenance_status.assert_called_once_with()
    else:
        zk.get_maintenance_status.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ('disabled\n' if mode == 'get' else '')
    assert output.err == ''


@pytest.mark.parametrize('flag', ['-m', '--mode', '--mode='])
@pytest.mark.parametrize('options_first', [False, True])
@pytest.mark.parametrize('value, mode, replacement', [
    ('enable', 'enable', 'on'), ('disable', 'disable', 'off'), ('show', 'get', 'show'), ('get', 'get', 'show'),
])
def test_legacy_dispatch_preserves_cluster_and_wait_options(monkeypatch, capsys, flag, options_first, value, mode, replacement):
    arguments = [flag + value] if flag.endswith('=') else [flag, value]
    arguments = ['-w', '-t', '60', *arguments] if options_first else [*arguments, '-w', '-t', '60']
    monkeypatch.setattr(sys, 'argv', [
        'pgconsul-util', '-c', '/review/custom.conf', '--zk', 'review-zk:2181', '--zk-prefix', '/review/cluster',
        'maintenance', *arguments,
    ])
    config = Mock()
    read_config = Mock(return_value=config)
    monkeypatch.setattr(cli, 'read_config', read_config)
    monkeypatch.setattr(cli, 'init_logging', Mock())
    zk = Mock()
    zk.get_maintenance_status.return_value = 'enable'
    with patch.object(cli, 'create_zk') as create_zk, patch.object(cli, 'enable_maintenance') as enable, patch.object(cli, '_wait_maintenance_disabled') as wait:
        create_zk.return_value.__enter__.return_value = zk
        cli.entry()
    opts = read_config.call_args.kwargs['options']
    read_config.assert_called_once_with(filename='/review/custom.conf', options=opts)
    assert (opts.mode, opts.zk_hosts, opts.zk_lockpath_prefix, opts.wait_all, opts.timeout) == (mode, 'review-zk:2181', '/review/cluster', True, 60)
    create_zk.assert_called_once_with(config=config)
    if mode == 'enable':
        enable.assert_called_once_with(zk, 60, True)
    else:
        enable.assert_not_called()
    if mode == 'disable':
        zk.write_maintenance_status.assert_called_once_with('disable')
        wait.assert_called_once_with(zk, 60)
    else:
        zk.write_maintenance_status.assert_not_called()
        wait.assert_not_called()
    if mode == 'get':
        zk.get_maintenance_status.assert_called_once_with()
    else:
        zk.get_maintenance_status.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ('enabled\n' if mode == 'get' else '')
    assert output.err == DEPRECATION.format(value=value, replacement=replacement)
