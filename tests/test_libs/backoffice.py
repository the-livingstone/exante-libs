import json
import logging
import os


class BackOffice:
    def __init__(
            self,
            env='prod',
            sdb_lists_path = f'{os.getcwd()}/libs/tests/test_libs/sdb_lists'
        ) -> None:
        self.env = env
        self.sdb_lists_path = sdb_lists_path
        self.used_symbols_prod = []
        with open(f"{'/'.join([self.sdb_lists_path, 'used_symbols.jsonl'])}", 'r') as f:
            for line in f:
                self.used_symbols_prod.append(json.loads(line))
        self.used_symbols_demo = []
        with open(f"{'/'.join([self.sdb_lists_path, 'used_symbols_demo.jsonl'])}", 'r') as f:
            for line in f:
                self.used_symbols_demo.append(json.loads(line))
        self.feed_permissions = []
        with open(f"{'/'.join([self.sdb_lists_path, 'feed_permissions.jsonl'])}", 'r') as f:
            for line in f:
                self.feed_permissions.append(json.loads(line))

    def __repr__(self):
        return f'BackOffice_test'

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def used_symbols(self):
        """
        retrieve list of symbols ever mentioned in account summary
        :return: list of used symbols
        """
        if self.env == 'prod':
            return self.used_symbols_prod
        elif self.env == 'demo':
            return self.used_symbols_demo

    def feed_permissions_get(self):
        return self.feed_permissions