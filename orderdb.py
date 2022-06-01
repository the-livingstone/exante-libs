#!/usr/bin/env python3

import datetime
import logging
from enum import Enum

import requests
from retrying import retry


class OrderDuration(Enum):
    """
    Order duration enum
    """
    none = -1
    fill_or_kill = 0
    immediate_or_cancel = 1
    good_till_cancel = 2
    day = 3
    at_the_opening = 4
    at_the_close = 5

    @staticmethod
    def string(_duration):
        """
        get string duration
        :param _duration: order duration
        :return: string duration
        """
        if _duration == OrderDuration.fill_or_kill:
            return 'fill_or_kill'
        elif _duration == OrderDuration.immediate_or_cancel:
            return 'immediate_or_cancel'
        elif _duration == OrderDuration.good_till_cancel:
            return 'good_till_cancel'
        elif _duration == OrderDuration.day:
            return 'day'
        elif _duration == OrderDuration.at_the_opening:
            return 'at_the_opening'
        elif _duration == OrderDuration.at_the_close:
            return 'at_the_close'
        else:
            return str()


class OrderSide(Enum):
    """
    Order side enum
    """
    none = -1
    buy = 0
    sell = 1

    @staticmethod
    def string(_side):
        """
        get string side
        :param _side: order side
        :return: string side
        """
        if _side == OrderSide.buy:
            return 'buy'
        elif _side == OrderSide.sell:
            return 'sell'
        else:
            return str()


class OrderSource(Enum):
    """
    Order sources enum
    """
    none = -1
    other = 0
    atp = 1
    fix = 2
    robots = 3
    counter = 4

    @staticmethod
    def string(_source):
        """
        get string source
        :param _source: order source
        :return: string source
        """
        if _source == OrderSource.other:
            return 'other'
        elif _source == OrderSource.atp:
            return 'atp'
        elif _source == OrderSource.fix:
            return 'fix'
        elif _source == OrderSource.robots:
            return 'robots'
        elif _source == OrderSource.counter:
            return 'counter'
        else:
            return str()


class OrderStatus(Enum):
    """
    Order statuses enum
    """
    none = -1
    placing = 0
    pending = 2
    working = 3
    filled = 4
    cancelled = 5
    rejected = 6

    @staticmethod
    def string(_status):
        """
        get string status
        :param _status: order status
        :return: string status
        """
        if _status == OrderStatus.placing:
            return 'placing'
        elif _status == OrderStatus.pending:
            return 'pending'
        elif _status == OrderStatus.working:
            return 'working'
        elif _status == OrderStatus.filled:
            return 'filled'
        elif _status == OrderStatus.cancelled:
            return 'cancelled'
        elif _status == OrderStatus.rejected:
            return 'rejected'
        else:
            return str()


class OrderType(Enum):
    """
    Order type enum
    """
    none = -1
    limit = 0
    stop = 1
    stop_limit = 2
    market = 3
    pegged = 4

    @staticmethod
    def string(_type):
        """
        get string type
        :param _type: order type
        :return: string type
        """
        if _type == OrderType.limit:
            return 'limit'
        elif _type == OrderType.stop:
            return 'stop'
        elif _type == OrderType.stop_limit:
            return 'stop_limit'
        elif _type == OrderType.market:
            return 'market'
        elif _type == OrderType.pegged:
            return 'pegged'
        else:
            return str()


class OrderDB:
    """
    Class to work with orderdb
    """

    url = None

    def __init__(self, env: str='prod'):
        """
        class init method
        :param env: environment
        """
        self.env = env
        self.url = 'http://orderdb.{}.zorg.sh'.format(env)
        self.headers = {'Content-Type': 'application/json'}
        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())

    def __repr__(self):
        return 'OrderDB({})'.format(repr(self.env))

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=lambda x: isinstance(x, requests.exceptions.Timeout))
    def __request(self, method, handle, params=None, data=None, head=None):
        """
        wrapper method for requests
        :param method: requests method to be invoked
        :param handle: authdb api handle
        :param params: additional parameters to pass with this request
        :param data: json to pass with request
        :return: requests response object
        """
        if params:
            logging.debug('passed params: {}'.format(params))
        if data:
            logging.debug('passed json: {}'.format(data))
        logging.debug('full url: {}'.format(self.url + handle))
        r = method(self.url + handle, params=params, headers=head, json=data)
        return r

    def _get(self, link, params=None):
        """
        custom requests.get method
        :param link: request link
        :param params: additional params to be passed
        :return: json of the response
        """
        resp = self.__request(method=self.session.get, handle=link, params=params, head=self.headers)
        try:
            return resp.json()
        except ValueError:
            return resp

    @staticmethod
    def _sources():
        return {'atp', 'fix', 'robots', 'counter', 'excel', 'web', 'mobile', 'other'}

    def get_order(self, _id):
        """
        method to get order by its id
        :param _id: order ID
        :return: order dictionary
        """
        return self._get('/orders/{}'.format(_id))

    def get_order_chain(self, _id):
        """
        method to get order chain, created by automation engine, by its id
        :param _id: order ID
        :return: order dictionary
        """
        pardata = {'clientOrder': _id}
        return self._get('/orders', params=pardata)

    def get_orders(self, account: str=None, user: str=None, username: str=None, instrument: str=None, minDate: str=None, maxDate: str=None, 
                   broker: str=None, order_type: str=None, source: str=None, status: str=None, limit: int=500, only_orders: bool=True,
                   automation_orders: bool=False, **kwargs):
        """
        method to get orders by specified filter
        :param account: account name
        :param user: matches accounts visible for the corresponding backoffice user. Can be used multiple times.
        :param username: filter on user name
        :param instrument: specific instrument
        :param minDate: YYYY-MM-DDTHH:MM:SSZ date
        :param maxDate: YYYY-MM-DDTHH:MM:SSZ date
        :param broker: specific broker url
        :param limit: limit order by count
        :param status: status (filled, working, rejected, pending)
        :param only_orders: return only orders, not all response
        :param kwargs: over supported flags, reffer to https://bitbucket.org/exante/orderdb-server
        :return list of orders
        """
        data = dict()
        data['automationOrders'] = automation_orders
        if account:
            data['account'] = account
        elif user:
            data['user'] = user
        else:
            data['allAccounts'] = 'true'
        if username:
            data['username'] = username
        if instrument:
            data['instrument'] = instrument
        if broker:
            data['brokerUrl'] = broker
        if order_type:
            data['type'] = order_type
        if maxDate:
            data['maxDate'] = maxDate
        if minDate:
            data['minDate'] = minDate
        if source:
            data['source'] = source
        if status:
            data['status'] = status
        data['maxSize'] = limit
        for key, value in kwargs.items():
            if type(value) == bool:
                data[key] = str(value).lower()
            elif type(value) == datetime.datetime:
                # +00:00 timezone hook
                data[key] = str(value.replace(tzinfo=datetime.timezone.utc).isoformat().split('+')[0])
            else:
                data[key] = str(value)
        response = self._get('/orders', params=data)
        if only_orders:
            if response.get('foundMore'):
                logging.warning('There are results which are not shown with limit {}'.format(limit))
            return response['orders']
        else:
            return response

    def update_date(self, min_date: datetime.datetime=None,
                    max_date: datetime.datetime=None, data: dict=None):
        """
        method to update date filter
        :param min_date: minimal order date
        :param max_date: maximal order date
        :param data: exist payload
        :return: appended data
        """
        ret_data = dict() if data is None else data
        if min_date:
            ret_data['minDate'] = min_date.strftime('%Y-%m-%dT%H:%M:%S')
        if max_date:
            ret_data['maxDate'] = max_date.strftime('%Y-%m-%dT%H:%M:%S')
        del min_date
        del max_date
        del data
        return ret_data

    def update_duration(self, duration: OrderDuration, data: dict=None):
        """
        method to update duration filter
        :param duration: order duration
        :param data: exist payload
        :return: appended data
        """
        ret_data = dict() if data is None else data
        ret_data['duration'] = OrderDuration.string(duration)
        del data
        return ret_data

    def update_source(self, source: OrderSource, data: dict=None):
        """
        method to update source filter
        :param source: order source
        :param data: exist payload
        :return: appended data
        """
        ret_data = dict() if data is None else data
        ret_data['source'] = OrderSource.string(source)
        del data
        return ret_data

    def update_status(self, status: OrderStatus, data: dict=None):
        """
        method to update status filter
        :param status: order status
        :param data: exist payload
        :return: appended data
        """
        ret_data = dict() if data is None else data
        ret_data['status'] = OrderStatus.string(status)
        del data
        return ret_data

    def update_type(self, ord_type: OrderType, data: dict=None):
        """
        method to update type filter
        :param ord_type: order type
        :param data: exist payload
        :return: appended data
        """
        ret_data = dict() if data is None else data
        ret_data['type'] = OrderType.string(ord_type)
        del data
        return ret_data
