import configparser
import os
import shutil
import platform
import time
import file


def get_config():
    config = configparser.ConfigParser()
    try:
        config.read('config.ini')
        idi = config['config']['working directory']
    except:
        write_default_config()
        config.read('config.ini')
    return config


def write_default_config():
    config = configparser.ConfigParser()
    config['config'] = {}
    config['config']['working directory'] = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'share')

    with open('config.ini', 'w') as configfile:
        config.write(configfile)


def get_working_directory():
    config = get_config()
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'share')


def set_working_directory(path):
    config = get_config()
    config['config']['working directory'] = path

    with open('config.ini', 'w') as configfile:
        config.write(configfile)


def get_file_names():
    files = os.listdir(get_working_directory())

    if len(files) == 0:
        files.append(' ')

    return files


def get_filepath(name):
    return os.path.join(get_working_directory(), name)


def get_mod_time(filepath):
    fileStatsObj = os.stat(filepath)
    return fileStatsObj.st_mtime


def newest_file(filepath1, filepath2):
    time1 = get_mod_time(filepath1)
    time2 = get_mod_time(filepath2)
    if time1 >= time2:
        return filepath1
    else:
        return filepath2


def scan():
    list = []
    names = get_file_names()
    for name in names:
        path = get_filepath(name)
        mod = get_mod_time(path)
        list.append(file.File(name, mod, path))
    return list
