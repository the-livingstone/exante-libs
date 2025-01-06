#!/usr/bin/env python3

'''
Library to standardise logging initialisation and calls
'''

import logging
import argparse


class TsLogging:

    loglevels = ['debug', 'info', 'warning', 'error', 'critical']
    default_logformat = '%(asctime)s: pid %(process)s: %(levelname)s: %(name)s.%(funcName)s: %(message)s'
    default_loglevel = 'warning'

    def __init__(self, args=None, loglevel=None, logformat=None, logfile=None):
        """
        main init method
        :param args: result of argparse.ArgumentParser().parse_args() method
        :param loglevel: one of loglevels
        :param logformat: desired logformat
        :param logfile: path to logfile
        """
        if args is None:
            if loglevel is None:
                loglevel = self.default_loglevel
            if logformat is None:
                logformat = self.default_logformat
            logging.basicConfig(filename=logfile, format=logformat, level=loglevel.upper(), force=True) #3.8+
        else:
            logging.basicConfig(filename=args.log, format=args.log_format, level=args.log_level.upper(), force=True) #3.8+

        for level in self.loglevels:
            setattr(self, level, getattr(logging, level))

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser, def_level=None):
        """
        classmethod to add standard logging arguments to argparse.parser
        :param parser: parser to add arguments to
        :param def_level: default loglevel, default_loglevel will be used if None
        """
        if def_level is None:
            def_level = cls.default_loglevel
        parser.add_argument(
            '--log',
            help='log file. Default is None',
            default=None)
        parser.add_argument(
            '--log-format',
            help='log format',
            default=cls.default_logformat)
        parser.add_argument(
            '--log-level',
            help=f'log level. Default is {def_level}',
            default=def_level,
            choices=cls.loglevels)
