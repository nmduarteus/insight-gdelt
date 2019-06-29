#!/usr/bin/python

"""
Module to read database configs
"""
import logging
import os
from configparser import ConfigParser


def config(section, filename=os.path.dirname(__file__) + '/config.ini'):
    """
    Function to read config file and parse it
    :param section: section of the config file to be read
    :param filename: filename of the config file
    :return: Details from the config
    """
    logging.info("Parsing configuration file....")

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(os.path.expanduser(filename))

    cfg = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            cfg[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return cfg