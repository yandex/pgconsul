import logging

from . import helpers


_substitutions = {
    'pgdata': '%p',
    'primary_host': '%m',
    'timeout': '%t',
    'argument': '%a',
}


@helpers.decorate_all_class_methods(helpers.func_name_logger)
class CommandManager:
    def __init__(self, commands: dict[str, str]):
        self._commands = commands

    def _prepare_command(self, command_name: str, **kwargs):
        command: str = self._commands.get(command_name, '')
        for arg_name, arg_value in kwargs.items():
            command = command.replace(_substitutions[arg_name], str(arg_value))
        return command

    def _exec_command(self, command_name: str, **kwargs):
        command = self._prepare_command(command_name, **kwargs)
        return helpers.subprocess_call(command)

    def promote(self, pgdata):
        return self._exec_command('promote', pgdata=pgdata)

    def rewind(self, pgdata, primary_host):
        return self._exec_command('rewind', pgdata=pgdata, primary_host=primary_host)

    def get_control_parameter(self, pgdata, parameter, preproc=None, log=True):
        command = self._prepare_command('get_control_parameter', pgdata=pgdata, argument=parameter)
        res = helpers.subprocess_popen(command, log_cmd=log)
        if not res:
            return None
        (stdout, stderr) = res.communicate()
        if res.returncode != 0:
            logging.error('error occured with command %s', command)
            logging.error('stderr: %s', stderr.decode('utf-8'))
            logging.error('stdout: %s', stdout.decode('utf-8'))
            return None
            
        value = stdout.decode('utf-8').split(':')[-1].strip()
        if preproc:
            return preproc(value)
        else:
            return value

    def list_clusters(self, log=True):
        command = self._prepare_command('list_clusters')
        res = helpers.subprocess_popen(command, log_cmd=log)
        if not res:
            return None
        output, _ = res.communicate()
        return output.decode('utf-8').rstrip('\n').split('\n')

    def start_postgresql(self, timeout, pgdata):
        return self._exec_command('pg_start', timeout=timeout, pgdata=pgdata)

    def stop_postgresql(self, timeout, pgdata):
        return self._exec_command('pg_stop', timeout=timeout, pgdata=pgdata)

    def get_postgresql_status(self, pgdata):
        return self._exec_command('pg_status', pgdata=pgdata)

    def reload_postgresql(self, pgdata):
        return self._exec_command('pg_reload', pgdata=pgdata)

    def start_pooler(self):
        return self._exec_command('pooler_start')

    def stop_pooler(self):
        return self._exec_command('pooler_stop')

    def get_pooler_status(self):
        return self._exec_command('pooler_status')

    def generate_recovery_conf(self, filepath, primary_host):
        return self._exec_command('generate_recovery_conf', pgdata=filepath, primary_host=primary_host)
