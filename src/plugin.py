"""
pgconsul plugin support module
"""
# encoding: utf-8

import inspect
import os
import sys

from .types import PluginsConfig


class PostgresPlugin(object):
    """
    Abstract class for postgresql plugin
    """

    def before_close_from_load(self):
        """
        This method executed before stopping pooler
        """
        pass

    def after_close_from_load(self):
        """
        This method executed right after stopping pooler
        """
        pass

    def before_promote(self, conn, config):
        """
        This method executed before calling pg_ctl promote
        """
        pass

    def after_promote(self, conn, config: PluginsConfig):
        """
        This method executed right after calling pg_ctl promote
        """
        pass

    def before_open_for_load(self):
        """
        This method executed before starting pooler
        """
        pass

    def after_open_for_load(self):
        """
        This method executed right after starting pooler
        """
        pass

    def before_populate_recovery_conf(self, primary_host):
        """
        This method executed before generating recovery.conf
        """
        pass

    def after_populate_recovery_conf(self, primary_host):
        """
        This method executed right after generating recovery.conf
        """
        pass


class ZookeeperPlugin(object):
    """
    Abstract class for zookeeper plugin
    """

    def on_lost(self):
        """
        This method executed on zk conn lost
        """
        pass

    def on_suspend(self):
        """
        This method executed on zk disconnection start
        """
        pass

    def on_connect(self):
        """
        This method executed on after zk connection is established
        """
        pass


def load_plugins(path):
    """
    Load plugins and return dict with Plugin lists
    """
    if path not in sys.path:
        sys.path.insert(0, path)

    ret: dict[str, list[PostgresPlugin] | list[ZookeeperPlugin]] = {'Postgres': [], 'Zookeeper': []}
    for i in os.listdir(path):
        if not i.endswith('.py'):
            continue

        module = __import__(i.split('.')[0])

        for j in [j for j in dir(module) if not j.startswith('__')]:
            try:
                j_class = getattr(module, j)
                for mro in inspect.getmro(j_class):
                    if mro == PostgresPlugin:
                        ret['Postgres'].append(j_class())
                    elif mro == ZookeeperPlugin:
                        ret['Zookeeper'].append(j_class())
            except Exception:
                pass

    return ret


class PluginRunner(object):
    """
    Plugin support helper
    """

    def __init__(self, plugins):
        self._plugins = plugins

    def list(self):
        """
        Return list of plugins
        """
        return self._plugins[:]

    def run(self, method, *args):
        """
        Execute method for each plugin
        """
        for i in self._plugins:
            getattr(i, method)(*args)
