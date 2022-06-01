#!/usr/bin/env python3
# Author: Da Fy
# License: BSD
import requests
import re
import logging


class CryptoStats:
    """
    Class for gathering information from cryptostats.
    Uses cryptostats HTTP API
    """

    def __init__(self):
        """
        class init method
        """
        self.url = f'http://crypto-stats1.cprod.zorg.sh:8080/api'
        self.counterparties = self.__get_counterparties()

    def __get(self, url, params):
        """
        universal getter with response classification
        :param url: url to the needed page
        :param params: params for the request
        :return: response json
        """
        params = self.param_str(params)
        response = requests.get(url, params=params)
        result = response.json()['response']
        if 'message' in result:
            msg = result['message']
            # if re.match(r'Parameter `\w+` missed', msg) or not msg or 'message' in msg:
            if not msg:
                return list()
            raise RuntimeError(f"ERROR {response.status_code}: {msg}")
        return result

    def __get_counterparties(self) -> list:
        """
        hidden method for collecting the list of existing brokers from cryptostats
        :return: the list of brokers' names
        """
        return sorted(list(self.__get(self.url + '/counterparts', None)))

    @staticmethod
    def param_str(params: dict):
        """
        method to make params string for URL
        :param params: params dict
        :return: params string
        """
        result = list()
        if params:
            for key, value in params.items():
                if isinstance(value, list):
                    for val in value:
                        result.append([key, val])
                elif value is None:
                    continue
                else:
                    result.append([key, value])
        return '&'.join(['='.join(i) for i in result])

    def __find_cryptostats_brokers(self, broker) -> list:
        """
        hidden method returning correct broker name from cryptostats brokers names
        :param broker: broker name to find
        :return: correct broker name or None
        """
        brokers = list()
        if broker:
            for cp in self.counterparties:
                if cp.lower().startswith(broker.lower()):
                    brokers.append(cp)
        if brokers:
            return brokers
        else:
            raise RuntimeError(f'No such broker: {broker}')

    def __list_generator(self, action, broker: str = None, **kwargs):
        """
        hidden generator method yielding the info from specified brokers' pages
        :param brokers: list of broker names
        :param kwargs: arguments to convert into request params
        :return: dicts made from the tables' rows
        """
        result = list()
        if broker:
            brokers = self.__find_cryptostats_brokers(broker)
        else:
            brokers = self.counterparties
        url = f'{self.url}/{action}'
        params = kwargs
        for broker in brokers:
            params['name'] = broker
            for item in self.__get(url, params=params):
                if item not in result:
                    result.append(item)
        return sorted(list(result), key=lambda x: x['id'])

    def __orders_generator(self, broker, **kwargs):
        """
        submethod of __list_generator for collecting orders
        :param brokers: list of broker names
        :param kwargs: arguments to convert into request params
        :return: order dicts made from the tables' rows
        """
        return self.__list_generator('orders', broker, **kwargs)

    def get_order(self, order_id, broker: str = None) -> dict:
        """
        method to get specified order info from cryptostats
        :param order_id: counterparty id of the order
        :param broker: broker name to make the search quicker
        :return: dict with order's info
        """
        for order in self.__orders_generator(broker, order_id=order_id):
            if order['id'] == order_id:
                return order

    def get_orders(self, broker: str = None, symbol: str = None, from_date: str = None, to_date: str = None) -> list:
        """
        method to collect all orders on the specified broker
        :param broker: broker name
        :param symbol: symbol from the order
        :param from_date: USO datetime string
        :param to_date: USO datetime string
        :return: orders list
        """
        return self.__orders_generator(broker, symbol=symbol, from_date=from_date, to_date=to_date)

    def get_balance(self, broker: str = None):
        return self.__list_generator('balance', broker)

    def __trades_generator(self, broker, **kwargs):
        """
        submethod of __list_generator for collecting trades
        :param brokers: broker name
        :param kwargs: arguments to convert into request params
        :return: order dicts made from the tables' rows
        """
        return self.__list_generator('trades', broker, **kwargs)

    def get_trades(
            self, broker: str = None, symbol: str = None, from_date: str = None, to_date: str = None,
            order_id: str = None
    ) -> list:
        """
        method to collect all trades on the specified broker
        :param broker: broker name
        :param symbol: symbol from the trades
        :param from_date: USO datetime string
        :param to_date: USO datetime string
        :param order_id: order id to filter the response
        :return: orders list
        """
        all_trades = self.__trades_generator(
            broker, symbol=symbol, from_date=from_date, to_date=to_date
        )
        if order_id:
            return list(filter(lambda x: x['order_id'] == order_id, all_trades))
        else:
            return all_trades

    def __transactions_generator(self, broker, **kwargs):
        """
        submethod of __list_generator for collecting transactions
        :param brokers: broker name
        :param kwargs: arguments to convert into request params
        :return: order dicts made from the tables' rows
        """
        return self.__list_generator('transactions', broker, **kwargs)

    def get_transactions(self, broker: str = None, symbol: str = None, from_date: str = None, to_date: str = None) -> list:
        """
        method to collect all transactions on the specified broker
        :param broker: broker name
        :param symbol: symbol from the transactions
        :param from_date: USO datetime string
        :param to_date: USO datetime string
        :return: orders list
        """
        return self.__transactions_generator(broker, symbol=symbol, from_date=from_date, to_date=to_date)
