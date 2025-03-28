#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configparser import RawConfigParser

from tests.steps import helpers


def fromfile(filename, fileobj):
    if filename in ['pgconsul.conf', 'pgbouncer.ini']:
        return ConfigINI(fileobj)
    elif filename in ['postgresql.conf', 'postgresql.auto.conf', 'recovery.conf']:
        return ConfigPG(fileobj)
    elif filename == 'standby.signal':
        return EmptyConfig(fileobj)
    else:
        raise NotImplementedError('Unknown config file {filename}'.format(filename=filename))


def getint(context, container_name, conf_name, section, key):
    container = context.containers[container_name]
    config = RawConfigParser()
    config.read_file(helpers.container_get_conffile(container, conf_name))
    return config.getint(section, key)


class Config(object):
    def __init__(self):
        raise NotImplementedError()

    def merge(self, config):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()

    def check_values_equal(self, config):
        raise NotImplementedError()


class EmptyConfig(Config):
    def __init__(self, _):
        pass

    def merge(self, _):
        pass

    def write(self, _):
        pass

    def check_values_equal(self, _):
        pass


class ConfigINI(Config):
    def __init__(self, fileobj):
        self.config = RawConfigParser()
        self.config.read_file(fileobj)

    def merge(self, config):
        assert isinstance(config, dict)
        for section, values in config.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            for key, value in values.items():
                self.config.set(section, key, str(value))

    def write(self, fileobj):
        return self.config.write(fileobj)

    def check_values_equal(self, config):
        assert isinstance(config, dict)
        for section, values in config.items():
            if not self.config.has_section(section):
                return False, 'missing section "{sec}"'.format(sec=section)
            for key, expected_value in values.items():
                if not self.config.has_option(section, key):
                    return False, 'missing option "{opt}" in section "{sec}"'.format(opt=key, sec=section)
                value = self.config.get(section, key)
                if str(value) != str(expected_value):
                    return False, 'option "{opt}" in section "{sec}" has value "{val}" expected "{exp}"'.format(
                        opt=key, sec=section, val=value, exp=expected_value
                    )
        return True, None


class ConfigPG(Config):
    def __init__(self, fileobj):
        self.config = {}
        for line in fileobj:
            if line.strip().startswith('#'):
                continue
            tup = line.strip('\n').split('=', maxsplit=1)
            assert len(tup) == 2, 'unexpected tuple {tup}'.format(tup=tup)
            self.config[tup[0].strip()] = tup[1].strip()

    def merge(self, config):
        assert isinstance(config, dict)
        for key, value in config.items():
            self.config[key.strip()] = "'{value}'".format(value=str(value).strip().replace("'", r"\'"))

    def write(self, fileobj):
        for key, value in self.config.items():
            fileobj.write('{key} = {value}\n'.format(key=key, value=value))

    def check_values_equal(self, config):
        assert isinstance(config, dict)
        for key, val in config.items():
            expected_value = str(val).strip()
            stripped_key = key.strip()
            if stripped_key not in self.config:
                return False, 'missing option "{opt}"'.format(opt=key)

            # NOTE: We need to be more carefully here. It is wrong to
            # simply replace "'" to "" if value has escaped quote "\'".
            # But seems that we have not this cases.
            value = self.config[stripped_key].replace("'", '')
            if str(value) != str(expected_value):
                return False, 'option "{opt}" has value "{val}", expected "{exp}"'.format(
                    opt=stripped_key, val=value, exp=expected_value
                )
        return True, None
