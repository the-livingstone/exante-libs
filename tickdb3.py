#!/usr/bin/env python3.7

import logging
import numpy
import pytz
import re
import requests

from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import datetime, timezone
from dateutil import parser
from decimal import Decimal, InvalidOperation
from itertools import islice
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Dict, List, Optional, Set, Union
from urllib.parse import quote as urlencode
import json

"""
Helpers
"""


class CommitLockedError(Exception):
    pass


class DecodeError(Exception):
    pass


def json_decoder(obj):
    for key, value in obj.items():
        if isinstance(value, str):
            try:
                value = Decimal(value)
            except (InvalidOperation, ValueError):
                try:
                    value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    value = value.replace(tzinfo=timezone.utc)
                except ValueError:
                    try:
                        value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
                        value = value.replace(tzinfo=timezone.utc)
                    except ValueError:
                        pass
        obj[key] = value
    return obj


def key_generator(item, interval='1day'):
    if interval == '1min':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, hour=item.timestamp.hour,
                        minute=item.timestamp.minute, tzinfo=timezone.utc)

    if interval == '5min':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, hour=item.timestamp.hour,
                        minute=(item.timestamp.minute - item.timestamp.minute % 5),
                        tzinfo=timezone.utc)
    if interval == '15min':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, hour=item.timestamp.hour,
                        minute=(item.timestamp.minute - item.timestamp.minute % 15),
                        tzinfo=timezone.utc)
    if interval == '30min':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, hour=item.timestamp.hour,
                        minute=(item.timestamp.minute - item.timestamp.minute % 30),
                        tzinfo=timezone.utc)
    if interval == '1hour':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, hour=item.timestamp.hour, tzinfo=timezone.utc)
    if interval == '1day':
        return datetime(year=item.timestamp.year, month=item.timestamp.month,
                        day=item.timestamp.day, tzinfo=timezone.utc)
    else:
        raise ValueError('Invalid interval')


def json_encoder(value):
    if isinstance(value, datetime):
        return str(int(value.timestamp() * 1000))
    elif isinstance(value, Decimal) or isinstance(value, int) or \
            isinstance(value, float):
        return str(value)
    else:
        return value


"""
QDictator types
"""


class iDictator:
    """
    Interface for classes how can post to dictator
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def dictator_dict(self): raise NotImplementedError


class MarketData:
    TYPES = (
        'trades',
        'quotes',
        'option_data',
        'prices'
    )
    # Length of corresponding interval in seconds
    INTERVALS = {
        '1min': 60,
        '5min': 300,
        # '15min': 900,
        # '30min': 1800,
        '1hour': 3600,
        '1day': 86400
    }
    # Only for option data candles
    POSITIONS = (
        'vega',
        'delta',
        'theta',
        'gamma',
        'implied_volatility',
        'theoretical_price',
        'implied_forward_price',
        'risk_free_rate'
    )

    def __init__(
            self,
            data_type: str,
            interval: str = None,
            position: str = None
        ):
        self.data = data_type
        self.interval = interval
        self.position = position

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):
        if value not in self.TYPES:
            raise ValueError(
                f"Invalid data type, expected one of following: {', '.join(self.TYPES)}"
            )
        self.__data = value

    @property
    def interval(self):
        return self.__interval

    @interval.setter
    def interval(self, value):
        if value not in self.INTERVALS and value is not None:
            raise ValueError(
                f"Invalid interval, expected one of following: {', '.join(self.INTERVALS)}"
            )
        self.__interval = value

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, value):
        if value not in self.POSITIONS and value is not None:
            raise ValueError(
                f"Invalid position, expected one of following: {', '.join(self.POSITIONS)}"
            )
        self.__position = value

    def __repr__(self):
        interval=f', {self.interval=}' if self.interval else ''
        position=f', {self.position=}' if self.position else ''
        return f'MarketData({self.data=}{interval}{position})'
    def __hash__(self):
        return hash(self.__repr__())

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    @classmethod
    def all_available(cls):
        """
        Generator that returns instances of MarketData class with all
        available types and intervals
        """
        for data_type in cls.TYPES[:2]:
            yield cls(data_type)
            for interval in cls.INTERVALS:
                yield cls(data_type, interval)
        for data_type in cls.TYPES[2:3]:
            yield cls(data_type)
            for interval in ['1hour']:
                yield cls(data_type, interval)
                # maybe in the future
                # for position in cls.POSITIONS:
                # yield cls(data_type, interval, position)
        yield cls(cls.TYPES[3])
        return

    @property
    def is_candle(self):
        return bool(self.interval)

    @property
    def quote_api(self):
        if self.is_candle:
            data_type = self.data[:-1] if self.data != 'option_data' else self.data
            position=f'/{self.position}' if self.position else ''
            return f'{data_type}_candles/{self.interval}{position}'
        else:
            return self.data

    @property
    def qdictator(self):
        if self.is_candle:
            data=f'{self.data[0]}candle' if self.interval and self.data != 'option_data' else self.data
            mid='mid_' if self.data == 'quotes' else ''
            return f'{data}_{mid}{self.interval}'
        else:
            return self.data

    @property
    def import_api(self):
        if self.is_candle:
            data=f'{self.data[0]}candles' if self.interval and self.data != 'option_data' else self.data
            return f'{data}/{self.interval}'
        else:
            return self.data if self.data != 'prices' else 'price'


class Trade(MarketData, iDictator):
    def __init__(
            self,
            timestamp: Union[datetime, str],
            price: Decimal,
            volume: Decimal
        ):
        super(Trade, self).__init__('trades')
        self.timestamp = timestamp
        self.price = price
        self.volume = volume

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value if isinstance(value, datetime) else parser.parse(value)

    @property
    def price(self):
        return self.__price

    @price.setter
    def price(self, value):
        self.__price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def volume(self):
        return self.__volume

    @volume.setter
    def volume(self, value):
        self.__volume = value if isinstance(value, Decimal) else Decimal(str(value))

    def __repr__(self):
        return f'Trade({self.timestamp=}, {self.price=}, {self.volume=})'

    @property
    def dictator_dict(self):
        return {
            'time': self.timestamp,
            'price': self.price,
            'size': self.volume
        }

    @staticmethod
    def from_dict(data):
        try:
            timestamp = parser.parse(data['time'])
            price = Decimal(data['price'])
            volume = Decimal(data['size'])
        except KeyError as e:
            raise ValueError(f'Invalid dict for Trade - reason {e}')
        except IndexError as e:
            raise ValueError(f'Invalid dict for Trade  - reason {e}')
        return Trade(timestamp, price, volume)

    @staticmethod
    def enlarge_your_trades(trades_list, interval='1min'):
        """
        Method for generating bigger candles from list of smaller candles DO NOT mix quote and trade candles
        :param trades_list: list of Candle objects
        :param interval: '1min', '5min', '1hour', '1day'
        """
        interval_dict = {}
        trades_list = sorted(trades_list, key=lambda x: x.timestamp)
        for i in trades_list:
            key = key_generator(i, interval)
            interval_dict.setdefault(key, [])
            interval_dict[key].append(i)
        result = []
        for i in interval_dict:
            candle = Candle(data_type='trades', interval=interval, timestamp=i,
                            max_price=max(j.price for j in interval_dict[i]),
                            min_price=min(j.price for j in interval_dict[i]),
                            open_price=interval_dict[i][0].price,
                            close_price=interval_dict[i][-1].price,
                            volume=sum(j.volume for j in interval_dict[i]))
            result.append(candle)
        return result


class Quote(MarketData):
    """
    Class represent quote side, real quote is MarketDepth class.

    """
    SIDES = ('bid', 'ask')

    def __init__(
            self,
            timestamp: Union[datetime, str],
            side: str,
            price: Decimal,
            volume: Decimal = 1
        ):
        super(Quote, self).__init__('quotes')

        self.side = side
        self.timestamp = timestamp
        self.price = price
        self.volume = volume

    @property
    def side(self):
        return self.__side

    @side.setter
    def side(self, value):
        assert value in self.SIDES
        self.__side = value

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value if isinstance(value, datetime) else parser.parse(value)

    @property
    def price(self):
        return self.__price

    @price.setter
    def price(self, value):
        self.__price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def volume(self):
        return self.__volume

    @volume.setter
    def volume(self, value):
        self.__volume = value if isinstance(value, Decimal) else Decimal(str(value))

    def __repr__(self):
        return f'Quote({self.timestamp}, {self.side=}, {self.price=}, {self.volume=})'

    @property
    def dict(self):
        return {'price': self.price, 'size': self.volume}


class MarketDepth(iDictator):
    """
    Class represents Quote/market depth: two list of quotes sorted by price. First
    item in each list is best offer.
    """
    data = 'quotes'
    qdictator = 'quotes'
    import_api = 'quotes'
    quote_api = 'quotes'
    interval = None
    position = None

    def __init__(
            self,
            timestamp: datetime,
            bid: List[Quote],
            ask: List[Quote],
            bid_yield: Decimal = None,
            ask_yield: Decimal = None
        ):
        """
        :param timestamp: timestamp of MarketDepth instance
        :param bid: list of bids
        :param ask: list of quotes
        :param bid_yield: yield (for bonds)
        :param ask_yield: yield (for bonds)
        """
        try:
            self.bid = sorted(
                bid,
                key=lambda x: x.price,
                reverse=True
            )
            self.ask = sorted(
                ask,
                key=lambda x: x.price,
                reverse=False
            )
            if not all(x.side == 'bid' for x in self.bid):
                raise ValueError(
                    f"Param 'bid' should be a list of bid quotes"
                )
            if not all(x.side == 'ask' for x in self.ask):
                raise ValueError(
                    f"Param 'ask' should be a list of ask quotes"
                )
        except AttributeError:
            raise TypeError(
                f"Params 'ask' and 'bid' should be a lists of quotes"
            )
        self.bid_yield = bid_yield
        self.ask_yield = ask_yield
        self.timestamp = timestamp

    def __repr__(self):
        bid_yield=f', {self.bid_yield=}' if self.bid_yield else ''
        ask_yield=f', {self.ask_yield=}' if self.ask_yield else ''

        return f'MarketDepth({self.timestamp}, {self.bid=}, {self.ask=}{bid_yield}{ask_yield})'

    def __eq__(self, other):
        try:
            if not self.timestamp == other.timestamp:
                logging.debug('Timestamps are not equal')
                return False
            if not self.ask_yield == other.ask_yield:
                logging.debug('Ask yields are not equal')
                return False
            if not self.bid_yield == other.bid_yield:
                logging.debug('Bid yields are not equal')
                return False
            if not self.ask == other.ask:
                logging.debug('Asks are not equal')
                return False
            if not self.bid == other.bid:
                logging.debug('Bids are not equal')
                return False
            return True
        except AttributeError:
            raise TypeError(f'MarketDepth expected but {type(other)} fount')

    @classmethod
    def from_quotes(cls, quotes: list, bid_yield: Decimal = None,
                    ask_yield: Decimal = None):
        """
        Method makes correct MarketDepth from unsorted list of quotes.
        Returned MarketDepth instance has timestamp of oldest quote in passed
        list of quotes.
        :param quotes: list of quotes. All timestamps of quotes must be either
         aware ore naive. Mixing will cause TypeError.
        :param bid_yield: bid yield (for bonds)
        :param ask_yield: ask yield (for bonds)
        :return: instance of MarketDepth
        """
        bids, asks = [], []
        if quotes[0].timestamp.tzinfo:
            timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)
        else:
            timestamp = datetime.fromtimestamp(0)
        for quote in quotes:
            if quote.side == 'bid':
                bids.append(quote)
            else:
                asks.append(quote)
            timestamp = max(timestamp, quote.timestamp)
        return cls(timestamp, bids, asks, bid_yield, ask_yield)

    @classmethod
    def from_dict(cls, data):
        """
        there are 2 options for data format:
        data = {
            'time': Union[datetime, str],
            'bid': {
                'pricedata': [
                    {
                        'price': Decimal,
                        'size': Decimal,
                    },
                    <...>
                ],
                'yield': Optional[Decimal]
            },
            'ask': {
                'pricedata': [
                    {
                        'price': Decimal,
                        'size': Decimal,
                    },
                    <...>
                ],
                'yield': Optional[Decimal]
            }
        }
        or
        data = {
            'time': Union[datetime, str],
            'bid': Union[float, Decimal],
            'ask': Union[float, Decimal]
        }
        """
        quotes = []
        yields = {}
        for side in ('bid', 'ask'):
            if data.get(side) is not None:
                if isinstance(data[side], (float, int, Decimal)):
                    price = Decimal(data[side])
                    quotes.append(
                        Quote(
                            timestamp=data['time'] if 'time' in data else data['frame_time'],
                            side=side,
                            price=price
                        )
                    )
                elif isinstance(data[side], dict):
                    yields[side] = data[side].get('yield')
                    try:
                        quotes += [
                            Quote(
                                timestamp=data['time'] if 'time' in data else data['frame_time'],
                                side=side,
                                price=p_data['price'],
                                volume=p_data['size']
                            ) for p_data
                            in data[side]['pricedata']
                        ]
                    except KeyError:
                        raise ValueError('Invalid dict for MarketDepth')
                else:
                    raise ValueError('Invalid data format for MarketDepth')
        if quotes:
            return cls.from_quotes(quotes, yields.get('bid'), yields.get('ask'))

    @property
    def dictator_dict(self):
        result = {
            'time': self.timestamp,
            'bid': {
                'pricedata': [i.dict for i in self.bid]
            },
            'ask': {
                'pricedata': [i.dict for i in self.ask]
            }
        }
        if self.bid_yield:
            result['bid']['yield'] = self.bid_yield
        if self.ask_yield:
            result['ask']['yield'] = self.ask_yield
        return result

    @property
    def mid(self):
        try:
            return (self.best_bid.price + self.best_ask.price) / 2
        except AttributeError:
            logging.warning(
                f'MarketDepth is not complete. Bids: {self.bid}, asks: {self.ask}'
            )
            if self.best_bid:
                return self.best_bid.price
            elif self.best_ask:
                return self.best_ask.price

    @property
    def best_bid(self):
        try:
            return self.bid[0]
        except IndexError:
            return None

    @property
    def best_ask(self):
        try:
            return self.ask[0]
        except IndexError:
            return None

    @property
    def relative_spread(self):
        try:
            return 2 * (self.best_ask.price - self.best_bid.price) \
                   / (self.best_bid.price + self.best_ask.price)
        except AttributeError:
            return None

    @property
    def absolute_spread(self):
        try:
            return self.best_ask.price - self.best_bid.price
        except AttributeError:
            return None

    def patch(self, quote: Quote, index: int = 0):
        """
        Method makes copy of current market depth, replaces one of quotes and
        updates timestamp. Original MarketDepth instance remains untouched.
        :param quote: new quote
        :param index: place for new quote
        :return: patched MarketDepth instance
        """
        result = deepcopy(self)
        try:
            depth = getattr(result, quote.side)
            try:
                depth[index] = quote
            except IndexError:
                depth.append(quote)
            result.timestamp = quote.timestamp
        except AttributeError:
            raise TypeError(f'Quote expected, {type(quote)} found')
        return result

    @classmethod
    def mapBidAsk(cls, bid=None, ask=None):
        return {'bid': bid, 'ask': ask}

    @staticmethod
    def enlarge_your_market_depth(market_depth_list, interval='1min'):
        """
        Method for generating bigger candles from list of smaller candles DO NOT mix quote and trade candles
        :param market_depth_list: list of marketDepth objects
        :param interval: '1min', '5min', '1hour', '1day'
        """
        interval_dict = {}
        market_depth_list = sorted(market_depth_list, key=lambda x: x.timestamp)
        for i in market_depth_list:
            key = key_generator(i, interval)
            interval_dict.setdefault(key, [])
            interval_dict[key].append(i)
        candles = []
        for qinterval, quotes in interval_dict.items():
            result_interval = {}
            r_quotes = []
            for quote in quotes:
                pairs = map(lambda x, y: {'bid': y, 'ask': x}, [ask.price for ask in quote.ask],
                            [bid.price for bid in quote.bid])
                mid_pairs = [pair['bid'] + pair['ask'] / 2 for pair in pairs]
                mid = numpy.mean(mid_pairs)
                r_quotes.append({'quote': quote, 'mid': mid})
            result_interval["time"] = qinterval
            result_interval['interval'] = interval
            result_interval['max_price'] = max(quote['mid'] for quote in r_quotes)
            result_interval['min_price'] = min(quote['mid'] for quote in r_quotes)
            result_interval['open_price'] = r_quotes[0]['mid']
            result_interval['close_price'] = r_quotes[-1]['mid']
            candles.append(result_interval)
        result = []
        for can in candles:
            candle = Candle(
                data_type='quotes',
                interval=can['interval'],
                timestamp=can['time'],
                max_price=can['max_price'],
                min_price=can['min_price'],
                open_price=can['open_price'],
                close_price=can['close_price'],
                volume=None
            )
            result.append(candle)
        return result

    def is_correct(self):
        """
        Method checks if market depth is correct: bid is less than ask and
        quotes in both depths are sorted in right order.
        :return: True or False
        """
        try:
            if self.bid[0].price >= self.ask[0].price:
                logging.warning(
                    f'Bid ({self.bid[0].price}) is larger or equal to ask ({self.ask[0].price})'
                )
                return False
        except IndexError:
            logging.warning('MarketDepth has no bids or asks')
            return False

        # damned piece of magic, please don't try to understand it
        for sequence in (self.bid[::-1], self.ask):
            for left, right in zip(sequence, islice(sequence, 1, None)):
                logging.debug(f'Comparing {left} and {right}')
                if left.price < right.price:
                    continue
                elif left.price == right.price:
                    logging.debug(f'Two equal prices of one side: {left}, {right}')
                else:
                    logging.debug(f'Wrong order of quotes in market depth: '
                        f'{left} is larger than {right}')
                return False

        return True


class Candle(MarketData, iDictator):
    def __init__(
            self,
            data_type: str,
            interval: str,
            timestamp: Union[datetime, str],
            open_price: Decimal,
            close_price: Decimal,
            max_price: Decimal,
            min_price: Decimal,
            volume: Decimal = None,
            validate: bool = True
        ):
        """
        Class that describes candle
        :param data_type: quotes or trades`
        :param interval: duration of the candle
        :param timestamp: timestamp of interval beginning
        :param open_price: open price
        :param close_price: close price
        :param max_price: maximal pricer
        :param min_price: minimal price
        :param volume: volume (for trade candles)
        :param validate: check if new candle contains valid data
        """
        if validate:
            if max_price < min_price:
                raise ValueError('Max price must be larger than min price')
            if not (min_price <= open_price <= max_price and min_price <= close_price <= max_price):
                raise ValueError('Close and open price must be larger than '
                                 'min price and smaller than max one')

        super(Candle, self).__init__(data_type, interval)
        self.timestamp = timestamp
        self.open_price = open_price
        self.close_price = close_price
        self.max_price = max_price
        self.min_price = min_price
        self.volume = volume

    @property
    def open_price(self):
        return self.__open_price

    @open_price.setter
    def open_price(self, value):
        self.__open_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def close_price(self):
        return self.__close_price

    @close_price.setter
    def close_price(self, value):
        self.__close_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def max_price(self):
        return self.__max_price

    @max_price.setter
    def max_price(self, value):
        self.__max_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def min_price(self):
        return self.__min_price

    @min_price.setter
    def min_price(self, value):
        self.__min_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def volume(self):
        return self.__volume

    @volume.setter
    def volume(self, value):
        self.__volume = value if isinstance(value, Decimal) or value is None else Decimal(str(value))

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value if isinstance(value, datetime) else parser.parse(value)

    def __repr__(self):
        return (
            f'Candle({self.data=}, {self.interval=}, '
            f'{self.timestamp=}, {self.open_price=}, '
            f'{self.close_price=}, {self.max_price=}, '
            f'{self.min_price=}, {self.volume=}'
        )

    @property
    def dictator_dict(self):
        result = {'frame_time': self.timestamp,
                  'open_price': self.open_price,
                  'close_price': self.close_price,
                  'min_price': self.min_price,
                  'max_price': self.max_price}
        if self.volume is not None:
            result['volume'] = self.volume
        return result

    @staticmethod
    def from_dict(data_type, interval, data):
        try:
            timestamp = parser.parse(data['frame_time'])
            open_price = Decimal(data['open_price'])
            close_price = Decimal(data['close_price'])
            min_price = Decimal(data['min_price'])
            max_price = Decimal(data['max_price'])
            volume = Decimal(data['volume']) if data.get('volume') else None
        except KeyError as e:
            raise ValueError(f'Invalid dict for Candle - reason {e}')
        except IndexError as e:
            raise ValueError(f'Invalid dict for Candle - reason {e}')
        return Candle(
            data_type=data_type,
            interval=interval,
            timestamp=timestamp,
            open_price=open_price,
            close_price=close_price,
            max_price=max_price,
            min_price=min_price,
            volume=volume,
            validate=True
        )

    def is_correct(self):
        return self.min_price <= self.open_price <= self.max_price and \
               self.min_price <= self.close_price <= self.max_price

    @classmethod
    def from_ticks(cls, ticks: list, interval: str):
        """
        Method gets list of ticks and builds candle
        :param ticks: list of ticks (Trade or MarketDepth instances)
        :param interval: string with description of interval
        :return: list of Candles
        """

        if not ticks:
            raise ValueError('Can\'t build candle form empty list of quotes')

        if isinstance(ticks[0], Trade):
            value_attr = 'price'
        elif isinstance(ticks[0], MarketDepth):
            value_attr = 'mid'
        else:
            raise TypeError(
                f'Expected instance if Trade or MarketDepth, {type(ticks[0])} found'
            )

        interval_dict = {}

        for i in ticks:
            key = key_generator(i, interval)
            interval_dict.setdefault(key, [])
            interval_dict[key].append(i)
        result = []
        for i in interval_dict:
            try:
                values = [
                    getattr(i, value_attr) for i
                    in sorted(
                        interval_dict[i],
                        key=lambda x: x.timestamp
                    )
                ]
            except AttributeError:
                raise TypeError('Ticks should have same type')

            candle = cls(
                data_type='quotes' if value_attr == 'mid' else 'trades',
                interval=interval, timestamp=i,
                max_price=max(values),
                min_price=min(values),
                open_price=values[0],
                close_price=values[-1]
            )
            if value_attr == 'price':
                candle.data = 'trades'
                candle.volume = sum(j.volume for j in interval_dict[i])
            result.append(candle)
        return result

    def swap_type(self, volume: int = 1):
        """
        Method returns full copy of current candle but with swapped type.
         It also tries to solves problem with volume. Problem is
         that candles based on quotes do not contain volume while it's required
         for candles based on trades. So method drops volume while creating
         quotes candles and uses `volume` parameter for trades candles.
        :type volume: default volume for candles based on trades
        :return: None
        """
        result = deepcopy(self)

        if result.data == 'trades':
            result.data = 'quotes'
            result.volume = None
        else:
            result.data = 'trades'
            result.volume = volume

        return result

    def __mul__(self, number):
        number = Decimal(str(number))
        return Candle(
            data_type=self.data,
            interval=self.interval,
            timestamp=self.timestamp,
            open_price=self.open_price * number,
            close_price=self.close_price * number,
            max_price=self.max_price * number,
            min_price=self.min_price * number,
            volume=self.volume // number if self.volume else None
        )

    @classmethod
    def enlarge_your_candle(cls, candle_list, interval='1day'):
        """
        Method for generating bigger candles from list of smaller candles DO NOT mix quote and trade candles
        :param candle_list: list of Candle objects
        :param interval: '1min', '5min', '1hour', '1day'
        """
        interval_dict = {}
        candle_list = sorted(candle_list, key=lambda x: x.timestamp)
        trades = False
        if candle_list[0].volume:
            trades = True
        for i in candle_list:
            key = key_generator(i, interval)
            interval_dict.setdefault(key, [])
            interval_dict[key].append(i)
        result = []
        for i in interval_dict:
            candle = cls(
                data_type='quotes' if not trades else 'trades',
                interval=interval,
                timestamp=i,
                max_price=max(j.max_price for j in interval_dict[i]),
                min_price=min(j.min_price for j in interval_dict[i]),
                open_price=interval_dict[i][0].open_price,
                close_price=interval_dict[i][-1].close_price,
                volume=None if not trades else sum(j.volume for j in interval_dict[i]))
            result.append(candle)
        return result


class OptionData(MarketData, iDictator):
    def __init__(
            self,
            timestamp: datetime,
            implied_volatility: float,
            theoretical_price: Decimal,
            delta: float,
            vega: float,
            theta: float,
            gamma: float
        ):
        super(OptionData, self).__init__('option_data')
        self.timestamp = timestamp
        self.implied_volatility = implied_volatility
        self.theoretical_price = theoretical_price
        self.delta = delta
        self.vega = vega
        self.theta = theta
        self.gamma = gamma

    def __repr__(self):
        return (
            f'OptionData({self.timestamp=}, '
            f'{self.implied_volatility=}, '
            f'{self.theoretical_price=}, '
            f'{self.delta=}, {self.vega=}, '
            f'{self.theta=}, {self.gamma=})'
        )

    @property
    def dictator_dict(self):
        return {
            'time': self.timestamp,
            'data': {
                'implied_volatility': self.implied_volatility,
                'theoretical_price': self.theoretical_price,
                'delta': self.delta,
                'vega': self.vega,
                'theta': self.theta,
                'gamma': self.gamma
            }
        }

    @classmethod
    def from_dict(cls, data):
        try:
            return cls(
                timestamp=data['time'],
                implied_volatility=data['data']['implied_volatility'],
                theoretical_price=data['data']['theoretical_price'],
                delta=data['data']['delta'],
                vega=data['data']['vega'],
                theta=data['data']['theta'],
                gamma=data['data']['gamma']
            )
        except KeyError:
            raise ValueError('Invalid dict for OptionData')


class OptionDataCandle(MarketData, iDictator):
    def __init__(self, data_type: str, interval: str, position: str, timestamp: Union[datetime, str],
                 open_price: Decimal, close_price: Decimal,
                 max_price: Decimal, min_price: Decimal,
                 validate: bool = True):
        """
        Class that describes candle
        :param data_type: quotes or trades
        :param interval: duration of the candle
        :param position: on of vega, delta, theta, gamma, implied_volatility,
                        theoretical_price, implied_forward_price, risk_free_rate
        :param timestamp: timestamp of interval beginning
        :param open_price: open price
        :param close_price: close price
        :param max_price: maximal price
        :param min_price: minimal price
        :param validate: check if new candle contains valid data
        """
        if validate:
            if max_price < min_price:
                raise ValueError('Max price must be larger than min price')
            if not (min_price <= open_price <= max_price and min_price <= close_price <= max_price):
                raise ValueError('Close and open price must be larger than '
                                 'min price and smaller than max one')

        super(OptionDataCandle, self).__init__(data_type, interval, position)
        self.timestamp = timestamp
        self.open_price = open_price
        self.close_price = close_price
        self.max_price = max_price
        self.min_price = min_price

    @property
    def open_price(self):
        return self.__open_price

    @open_price.setter
    def open_price(self, value):
        self.__open_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def close_price(self):
        return self.__close_price

    @close_price.setter
    def close_price(self, value):
        self.__close_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def max_price(self):
        return self.__max_price

    @max_price.setter
    def max_price(self, value):
        self.__max_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def min_price(self):
        return self.__min_price

    @min_price.setter
    def min_price(self, value):
        self.__min_price = value if isinstance(value, Decimal) else Decimal(str(value))

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value if isinstance(value, datetime) else parser.parse(value)

    def __repr__(self):
        return (
            f'OptionDataCandle{self.data=}, {self.interval=},'
            f'{self.position=}, {self.timestamp=}, {self.open_price=}, '
            f'{self.close_price=}, {self.max_price=}, '
            f'{self.min_price=})'
        )

    def __mul__(self, number):
        number = Decimal(str(number))
        return OptionDataCandle(
            data_type=self.data,
            interval=self.interval,
            position=self.position,
            timestamp=self.timestamp,
            open_price=self.open_price * number,
            close_price=self.close_price * number,
            max_price=self.max_price * number,
            min_price=self.min_price * number
        )


class Price(MarketData, iDictator):
    def __init__(self, timestamp: Union[datetime, str], price: Decimal):
        super(Price, self).__init__('prices')
        self.timestamp = timestamp
        self.price = price

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value if isinstance(value, datetime) else parser.parse(value)

    @property
    def price(self):
        return self.__price

    @price.setter
    def price(self, value):
        self.__price = value if isinstance(value, Decimal) else Decimal(str(value))

    def __repr__(self):
        return f'Price({repr(self.timestamp)}, price={repr(self.price)})'

    @property
    def dictator_dict(self):
        return {
            'time': self.timestamp,
            'price': self.price
        }

    @staticmethod
    def from_dict(data):
        try:
            timestamp = parser.parse(data['time'])
            price = Decimal(data['price'])
        except KeyError as e:
            raise ValueError(f'Invalid dict for Price - reason {e}')
        except IndexError as e:
            raise ValueError(f'Invalid dict for Price  - reason {e}')
        return Price(timestamp, price)


"""
Main TickDB3 Api

"""


class TickDB3:

    def __init__(self, node: str = 'tickdb3', env: str = 'prod'):
        self.node = node
        self.env = env
        self.domain = 'zorg.sh'
        self.url = f'http://{self.node}.{self.env}.{self.domain}'

        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[
                500,
                502,
                503,
                504
            ]
        )

        self.import_api_session = requests.session()
        self.quote_api_session = requests.session()
        self.quote_api_session.mount('http://', HTTPAdapter(max_retries=retries))
        self.crossrate_api_session = requests.session()
        self.crossrate_api_session.mount('http://', HTTPAdapter(max_retries=retries))
        self.seq = None

    def __repr__(self):
        return f'TickDB3({repr(self.url)})'

    def __get(
            self,
            symbol: str,
            data: MarketData,
            limit: int = None,
            since: datetime = None,
            till: datetime = None,
            **params
        ):
        """
        internal method that returns raw data from QRing
        :param symbol: ExanteID
        :param data: instance of MarketData class
        :param limit: limit on quantity of entries, default is infinite
        :param since: get data older than this datetime
        :param till: get data younger than this datetime
        :param params: this data will be transferred as GET params
        :return: response of Qring parsed as JSON
        :raises: RuntimeError in case of non 200 response from server
        """
        url = self.api_url('quote', data, payload=f"export/symbols/{urlencode(symbol, safe='')}/")
        if since:
            since = since.replace(tzinfo=pytz.UTC)
            params['from'] = int(since.timestamp() * 1000)
        if till:
            till = till.replace(tzinfo=pytz.UTC)
            params['to'] = int(till.timestamp() * 1000)
        if limit:
            params['limit'] = limit
        logging.debug('Parameters are {}'.format(params))
        response = self.quote_api_session.get(
            url=url,
            params=params,
            headers={
                'accept-encoding': 'gzip',
                'accept': 'application/x-ndjson'
            },
            stream=True
        )

        if response.ok:
            for line in response.iter_lines():
                try:
                    yield json.loads(line.decode(), object_hook=json_decoder)
                except ValueError:
                    pass
        else:
            raise RuntimeError(
                f'{response.status_code} ({response.reason}): {response.text}' + '\n'
                f'for request {url} with parameters since: {since} till: {till}'
            )

    def api_url(
            self,
            api: str,
            data_type: Union[MarketData, MarketDepth],
            node: str = None,
            payload: str = ''
        ):
        if not node:
            node = self.node
        else:
            node = node.replace('http://', '').split('.')[0]
        base_url = f'http://{node}.{self.env}.{self.domain}'

        if api in ['import', 'quote'] and data_type.data not in MarketData.TYPES:
            logging.error(f'wrong data_type, should be one of: {MarketData.TYPES}')
            return ''
        elif api not in ["quote", "import"]:
            logging.error(f'wrong API name, should be "quote" or "import"')
            return ''
        port = {
            'trades': ':8181',
            'quotes': ':8181',
            'option_data': ':8181',
            'prices': ':8182'
        }
        api_query = {
            'import': [
                '/v1/import/',
                payload,
                data_type.import_api
            ],
            'quote': [
                '/quote_api/v1/',
                payload,
                data_type.quote_api
            ]
        }
        if api == 'quote' and node == 'tickdb3':
            url = base_url + ''.join(api_query[api])
        else:
            url = base_url + port[data_type.data] + ''.join(api_query[api])
        return url

    def define_urls(self, data_type: Union[MarketData, MarketDepth], symbol: str):
        """
        returns urls for post to tickdb etc
        """
        logging.debug(f'Searching nodes for agregate type {data_type.data}...')
        all_nodes = self.get_nodes()
        if data_type.data in ['quotes', 'trades'] and data_type.interval is None:
            node_type = 'tickdb_server@'
            role = 'server/ticks'
        elif data_type.data in ['quotes', 'trades']:
            if self.env != 'demo':
                node_type = 'tickdb_candles@'
            else:
                node_type = 'tickdb_server@'
            role = 'server/candles'
        elif data_type.data == 'option_data':
            node_type = 'tickdb_server@'
            role = 'server/option_data'
        elif data_type.data in ['prices']:
            node_type = 'tickdb_prices@'
            role = 'symbol_prices'
        else:
            return []
        role_request = '/web/v1/server_info'
        needed_nodes = [
            url.replace(node_type, '') for url
            in all_nodes
            if node_type in url
            and self.quote_api_session.get(
                url=url.replace(node_type, '')+role_request,
                headers={'accept': 'application/json'}
            ).json().get('role') == role
        ]
        urls = [
            self.api_url('import', data_type, node=node, payload=f"symbols/{urlencode(symbol, safe='')}/")
            for node in needed_nodes
        ]

        logging.debug(f"{len(urls)} urls found:" + '\n' + '\n'.join(urls))
        return urls

    def __post(self, symbol: str, data_type: Union[MarketData, MarketDepth], data: list, **kwargs):
        """
        Internal method, posts data to QDictator without any transformations
        :param symbol: string with SymbolID
        :param data_type: string that identifies type of market data for QRing
        :param data: list of dictionaries representing trades, quotes, etc.
        :param force: if 'true' - bypass CommitLock
        :param kwargs: additional POST parameters
        :return: None
        :raises: RuntimeError in case of wrong server answer
        """

        def chunk_input():
            for item in data:
                encoded_item = json.dumps(item, default=json_encoder)
                yield '{}\n'.format(encoded_item).encode()
            return

        if self.node == 'tickdb3':
            urls = self.define_urls(data_type=data_type, symbol=symbol)
        else:
            urls = [
                self.api_url('import', data_type=data_type, payload=f"symbols/{urlencode(symbol, safe='')}/")
            ]

        logging.debug(urls)

        if data_type.data == 'price':
            kwargs.update({'resetPrice': 'true'})

        try:
            logging.debug(f'Importing {len(data)} items to TickDB3')
        except TypeError:
            logging.debug('Importing unknown number of items to TickDB3')

        logging.debug(f'Parameters are {kwargs}')

        for url in urls:
            logging.debug(f'Posting to {url}')
            response = self.import_api_session.post(
                f"{url}?{'&'.join([f'{key}={value}' for key, value in kwargs.items()])}",
                data=chunk_input(),
                headers={'Content-Type': 'application/x-ndjson'}
            )
            logging.debug(f'Response is {response}')
            self.__check_response(response)

    def __check_response(self, response: requests.Response):
        """
        Method parses response to POST request and raises one of exceptions
        in case of error. If no error returns None and updates seq number.
        :param response: requests.Response object
        :return: None
        :raises: CommitLockedError if commit is locked,
        DecodeError in case of problem with data,
        RuntimeError in all other cases.
        """
        if response.status_code == 204:
            return None
        else:
            try:
                parsed_response = response.json()
                status = parsed_response.get('type')
                if status == 'ok':
                    seq = parsed_response.get('seq')
                    if seq is not None:
                        self.seq = seq
                        return None
                    else:
                        raise RuntimeError('seq number not found, '
                                           'maybe unsupported version of API')
                elif status == 'error':
                    reason = parsed_response.get('message')
                    if reason == '#{commit => locked}' or response.status_code == 409:
                        raise CommitLockedError
                    elif reason.startswith('{decode'):
                        raise DecodeError(reason)
                    else:
                        raise RuntimeError(
                            f"{reason}: {parsed_response.get('description')}"
                        )
                else:
                    raise RuntimeError(
                        "Unexpected status. 'ok' or 'error' expected, "
                        f"'{status}' found. Maybe wrong version of API"
                    )
            except ValueError:
                raise RuntimeError(f'{response.status_code} ({response.reason}): {response.text}')

    def post(self, symbol: str, data: list, **kwargs):
        """
        Easiest way to post data to QDictator. Gets list of any data pell-mel,
        sorts by type, prepares and posts to QDictator.
        :param symbol: string with ExanteId
        :param data: list of data
        :param kwargs: additional POST parameters
        :return: dictionary with response of server
        :raises: RuntimeError in case of wrong server answer
        """
        result = dict()
        count = 0
        for item in data:
            if isinstance(item, MarketData) or isinstance(item, MarketDepth):
                result.setdefault(item.import_api, {})
                result[item.import_api].setdefault(
                    'md', MarketData(item.data, item.interval, item.position)
                )
                result[item.import_api].setdefault(
                    'payload', []
                ).append(item.dictator_dict)
            else:
                count += 1
        if count:
            logging.debug(
                f'Don\'t try to break TickDB3 import with {count} wrong objects, '
                'use objects with iDictator interface'
            )
        for data_group, section in result.items():
            logging.debug(f'Going to post {data_group} for {symbol}')
            self.__post(symbol, section['md'], section['payload'], **kwargs)

    def get(
            self,
            symbol: str,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            cls=list,
            **kwargs
        ):
        """
        Method returns all available aggregates for specified symbol.

        Be careful it can return REALLY HUGE VOLUME of data and eat all memory
        :param symbol: string with ExanteId
        :param since: datetime of earliest aggregate
        :param till: datetime of latest aggregate
        :param limit: quantity of aggregates
        :param cls: class of list-like storage for received data
        :param kwargs: additional GET parameters
        :return: list of aggregates
        """
        # TODO there should be another way
        if isinstance(since, str):
            since = parser.parse(since)
        if isinstance(till, str):
            till = parser.parse(till)
        result = cls()
        result.extend(self.get_quotes(symbol, since, till, limit, **kwargs))
        result.extend(self.get_qcandles(symbol, None, since, till, limit, **kwargs))
        result.extend(self.get_trades(symbol, since, till, limit, **kwargs))
        result.extend(self.get_tcandles(symbol, None, since, till, limit, **kwargs))
        return result
    
    def marketdata_to_json(self, data: list[Union[MarketData, MarketDepth]], file_name: str):
        result = []
        for d in data:
            result.append(d.dictator_dict)
        with open(file_name, 'w') as f:
            json.dump(result, f, indent=2, default=json_encoder)

    def get_data(
            self,
            symbol: str,
            md_type: str,
            interval: str = None,
            candles: bool = False,
            position: str = None,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            validate: bool = False,
            **kwargs
        ):
        md_types = [
            'trades',
            'quotes',
            'option_data'
        ]
        if candles:
            intervals = [interval] if interval else list(MarketData.INTERVALS.keys())
        else:
            intervals = [None]
        if md_type == 'option_data':
            positions = [position] if position else MarketData.POSITIONS
        else:
            positions = [None]
        if isinstance(since, str):
            since = parser.parse(since)
        if isinstance(till, str):
            till = parser.parse(till)
        log_message = f"Start getting {md_type} {'candles' if candles else ''}"
        if intervals[0]:
            log_message += f'with following intervals: {intervals}'
        if positions[0]:
            log_message += '\n' + f'for those positions: {positions}'
        logging.debug(log_message)
        tdb_data = []
        for i in intervals:
            for p in positions:
                market_data = MarketData(md_type, i, p)
                tdb_data.append(
                    [
                        self.__get(
                            symbol,
                            market_data,
                            limit,
                            since,
                            till,
                            **kwargs
                        ),
                        market_data
                    ]
                )
        return tdb_data

    def get_tcandles(
            self,
            symbol: str,
            interval: str = None,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            validate: bool = False,
            **kwargs
        ):
        """
        Method returns candles built by trades.
        :param symbol: string with ExanteId
        :param interval: if not specified, returns all available intervals
        :param since: datetime of earliest candle
        :param till: datetime of latest candle
        :param limit: quantity of candles
        :param validate: verify candles received from QRing
        :param kwargs: additional GET parameters
        :return: generator of Candle objects
        :raises: RuntimeError in case of non 200 response from server
        """
        candles = self.get_data(
            symbol=symbol,
            md_type='trades',
            interval=interval,
            candles=True,
            since=since,
            till=till,
            limit=limit,
            validate=validate,
            **kwargs
        )
        for c, marketdata in candles:
            for candle in c:
                semi_result = Candle(
                    marketdata.data,
                    marketdata.interval,
                    timestamp=candle['time'],
                    open_price=candle['open_price'],
                    close_price=candle['close_price'],
                    max_price=candle['max_price'],
                    min_price=candle['min_price'],
                    validate=validate
                )
                if candle.get('volume') is not None:
                    semi_result.volume = Decimal(candle['volume'])
                yield semi_result

    def get_qcandles(
            self,
            symbol: str,
            interval: str = None,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            validate: bool = False,
            **kwargs
        ):
        """
        Method returns candles built by quotes.
        :param symbol: string with ExanteId
        :param interval: if not specified, returns all available intervals
        :param since: datetime of earliest candle
        :param till: datetime of latest candle
        :param limit: quantity of candles
        :param validate: verify candles received from QRing
        :param kwargs: additional GET parameters
        :return: generator of Candle objects
        :raises: RuntimeError in case of non 200 response from server
        """
        candles = self.get_data(
            symbol=symbol,
            md_type='quotes',
            interval=interval,
            candles=True,
            since=since,
            till=till,
            limit=limit,
            validate=validate,
            **kwargs
        )
        for c, marketdata in candles:
            for candle in c:
                try:
                    semi_result = Candle(
                        marketdata.data,
                        marketdata.interval,
                        timestamp=candle['time'],
                        open_price=candle['open_price'],
                        close_price=candle['close_price'],
                        max_price=candle['max_price'],
                        min_price=candle['min_price'],
                        validate=validate
                    )
                except KeyError:
                    logging.warning(
                        f'error occurred on retrieving candle {candle} while working on symbol {symbol}'
                    )
                    continue
                yield semi_result

    def get_trades(
            self,
            symbol: str,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            **kwargs
        ):
        """
        Method returns trades.
        :param symbol: string with ExanteId
        :param since: datetime of earliest candle
        :param till: datetime of latest candle
        :param limit: quantity of trades
        :param kwargs: additional GET parameters
        :return: generator of Trade objects
        :raises: RuntimeError in case of non 200 response from server
        """
        trades = self.get_data(
            symbol=symbol,
            md_type='trades',
            candles=False,
            since=since,
            till=till,
            limit=limit,
            **kwargs
        )
        for t, marketdata in trades:
            for trade in t:
                yield Trade(
                    trade['time'],
                    price=trade['price'],
                    volume=trade['size']
                )

    def get_quotes(
            self,
            symbol: str,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            **kwargs
        ):
        """
        Method returns quotes.
        :param symbol: string with ExanteIddata
        :param since: datetime of earliest candle
        :param till: datetime of latest candle
        :param limit: quantity of quotes
        :param kwargs: additional GET parameters
        :return: generator of MarketDepth objects
        """
        quotes = self.get_data(
            symbol=symbol,
            md_type='quotes',
            candles=False,
            since=since,
            till=till,
            limit=limit,
            **kwargs
        )
        for q, marketdata in quotes:
            for item in q:
                result = MarketDepth.from_dict(item)
                if result:
                    yield result
                else:
                    continue

    def get_crossrates(self, ts: datetime = None):
        """
        method return all available crossrates from EUR
        :param ts: historical snapshot
        :return:
        """
        url = f"{self.url}/crossrate_api/v1/snapshot"
        params = {}
        if ts:
            params['timestamp'] = int(ts.timestamp()) * 1000
        logging.debug('Parameters are {}'.format(params))
        response = self.crossrate_api_session.get(
            url=url,
            params=params,
            headers={'accept-encoding': 'gzip'}
        )

        if response.ok:
            return response.json()
        else:
            raise RuntimeError(
                f'{response.status_code} ({response.reason}): {response.text}' + '\n'
                f'for request {url}'
            )

    def get_crossrate(self, asset1, asset2, ts=None):
        """
        method return crossrate between two assets
        :param asset1: baseCurrency for crossrate (from)
        :param asset2: currency for crossrate (to)
        :param ts: historical crossrate
        :return:
        """
        url = f"{self.url}/crossrate_api/v1/crossrate"
        params = {'from': asset1, 'to': asset2}
        if ts:
            params['timestamp'] = int(ts.timestamp()) * 1000
        logging.debug('Parameters are {}'.format(params))
        response = self.crossrate_api_session.get(
            url=url,
            params=params,
            headers={'accept-encoding': 'gzip'}
        )

        if response.ok:
            return response.json()
        else:
            raise RuntimeError(
                f'{response.status_code} ({response.reason}): {response.text}' +'\n'
                f'for request {url}'
            )

    def quote_at(
            self,
            symbols: list,
            data_type: str,
            timestamp: Union[str, datetime] = None
        ):
        """
        Method allows to get actual quotes for specific moment
        :param symbols: list of symbols
        :param timestamp: datetime object
        :return: dictionary with symbolIds as keys and
         MarketDepth objects as values
        """
        if isinstance(timestamp, str):
            timestamp = parser.parse(timestamp)
        url = '/'.join(self.api_url(
            'quote',
            marketdata=MarketData(data_type),
            payload='history/quote_at/'
        ).split('/')[:-1])
        payload = [('symbol_id', s) for s in symbols]
        payload.append(('timestamp', int(timestamp.timestamp() * 1000)))
        logging.debug('Payload is {}'.format(payload))

        response = requests.post(
            url=url,
            data=payload,
            headers={'accept-encoding': 'gzip'}
        )

        result = dict()
        if response.ok:
            data = response.json(object_hook=json_decoder)
            for item in data:
                if item.get('error'):
                    logging.debug('Error for {}: {}'.format(item['symbol_id'],
                                                            item['error']))
                    continue
                result[item['symbol_id']] = MarketDepth.from_dict(item)

            return result
        else:
            raise RuntimeError(
                f'{response.status_code} ({response.reason}): {response.text}'
            )

    def get_option_data(
            self,
            symbol: str,
            since: datetime = None,
            till: datetime = None,
            limit: int = None,
            **kwargs
        ):
        """
        Method returns option data for specified instrument
        :param symbol: string with ExanteId
        :param since: datetime of earliest aggregate
        :param till: datetime of latest aggregate
        :param limit: quantity of aggregates
        :param kwargs: additional GET parameters
        :return: generator of OptionData objects
        """
        option_data = self.get_data(
            symbol=symbol,
            md_type='option_data',
            candles=True,
            since=since,
            till=till,
            limit=limit,
            **kwargs
        )
        for od in option_data:
            for item in od:
                try:
                    result = OptionData.from_dict(item)
                except ValueError as e:
                    logging.error('{} ({})'.format(e, item))
                    continue

                yield result

    def get_option_data_candles(
            self,
            symbol: str,
            interval: str = None,
            position: str = None,
            since: Union[str,datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            validate: bool = False,
            **kwargs
        ):
        """
        Method returns candles built by quotes.
        :param symbol: string with ExanteId
        :param interval: if not specified, returns all available intervals
        :param position: if not specified, returns all available positions
        :param since: datetime of earliest candle
        :param till: datetime of latest candle
        :param limit: quantity of candles
        :param validate: verify candles received from QRing
        :param kwargs: additional GET parameters
        :return: generator of OptionDataCandle objects
        :raises: RuntimeError in case of non 200 response from server
        """
        # intervals could be added in future, for now only 1hour
        candles = self.get_data(
            symbol=symbol,
            md_type='option_data',
            interval='1hour',
            candles=True,
            position=position,
            since=since,
            till=till,
            limit=limit,
            validate=validate,
            **kwargs
        )
        for c, marketdata in candles:
            for candle in c:
                try:
                    semi_result = OptionDataCandle(
                        'option_data',
                        marketdata.interval,
                        marketdata.position,
                        timestamp=candle['time'],
                        open_price=candle['open_price'],
                        close_price=candle['close_price'],
                        max_price=candle['max_price'],
                        min_price=candle['min_price'],
                        validate=validate
                    )
                except KeyError:
                    logging.warning(
                        f'error occurred on retrieving candle{candle} while working on symbol{symbol}')
                    continue
                yield semi_result

    def get_prices(
            self,
            symbol: str,
            since: Union[str, datetime] = None,
            till: Union[str, datetime] = None,
            limit: int = None,
            **kwargs
        ):
        """
        returns last session price for symbol in specified period of time
        :param symbol: string with EXANTEId
        :param since: datetime of earlist session
        :param till: datetime of latest session
        :param limit: quantity of prices
        :param kwargs: additional GET parameters
        :return: generator of Price objects
        """
        prices = self.get_data(
            symbol=symbol,
            md_type='prices',
            candles=False,
            since=since,
            till=till,
            limit=limit,
            **kwargs
        )

        for p, marketdata in prices:
            for price in p:
                yield Price(
                    price['time'],
                    price['price']
                )

    def delete(
            self,
            symbol: str,
            data_type: Union[MarketData, MarketDepth],
            urls: list[str] = None,
            timestamp: Union[str, datetime] = None
        ):
        """
        Method deletes specified aggregates for specified period
        :param symbol: string with ExanteId
        :param data_type: instance of MarketData that describes required data
        :param timestamp: datetime of single aggregate
        :param since: datetime of earliest aggregate
        :param till: datetime of latest aggregate
        :return: dictionary with response of server
        :raises: RuntimeError in case of wrong server answer
        """
        if isinstance(timestamp, str):
            timestamp = parser.parse(timestamp)
        
        urls = urls if urls else self.define_urls(data_type=data_type, symbol=symbol)

        logging.debug(urls)

        payload = {
            'timestamp': int(timestamp.timestamp() * 1000) if timestamp else data_type.timestamp
        }
        logging.debug('Payload is {}'.format(payload))
        for url in urls:
            response = requests.delete(
                url=url,
                params=payload,
                headers={'Content-Type': 'application/json'}
            )
            self.__check_response(response)

    def get_nodes(self):
        """
        returns list of registered nodes
        :return:
        """
        url = f'{self.url}/quote_api/v1/nodes'
        response = self.quote_api_session.get(url)
        if response.ok:
            return response.json()
        else:
            logging.warning(response.text)
            return list()

    def get_aggregates(self):
        url = f'{self.url}/quote_api/v1/aggregates'
        response = self.quote_api_session.get(url)
        if response.ok:
            return response.json()
        else:
            logging.warning(response.text)
            return list()

    def get_node_aggregates(self, node) -> dict:
        """
        Return list of aggregates for node
        :param: node url like [http://tickdb_server@tickdb70.prod.zorg.sh:8181]
        :return: null-dict if trouble, else dict like {type: [durations]}
        """
        url = f'{node}/quote_api/v1/aggregates'
        response = self.quote_api_session.get(url)
        logging.warning(response.status_code)

        if response.ok:
            data = dict()
            for item in response.json():
                if item['type'] not in data:
                    data.update({item['type']: []})
                data[item['type']].append(item['duration'])
            return data

        logging.warning(response.text)
        return dict()

    def get_all_aggregates_type(self) -> dict:
        nodes = self.get_nodes()
        aggregates = dict()
        for node in nodes:
            temp = self.get_node_aggregates(node)
            for aggregate in temp:
                if aggregate not in aggregates:
                    aggregates.update({aggregate: set()})
                aggregates[aggregate].update(temp[aggregate])
        return aggregates

    def search_nodes(self, type_='quote_v1') -> list:
        """
        Search nodes where data type locate
        :param type_: data type
        :return: list of dictionary [{node, type, env, domain, port}]
        """
        nodes = self.get_nodes()
        node_re = re.compile(r'(http://)(?P<type>\w+)@(?P<node>\w+)\.(?P<env>\w+)\.(?P<domain>.\w+.\w+):(?P<port>\d+)')
        result = list()
        for node in nodes:
            aggregates = self.get_node_aggregates(node)
            if type_ in aggregates:
                data = node_re.match(node)
                if data:
                    result.append({key: data.group(key) for key in ['node', 'type', 'env', 'domain', 'port']})
        return result


    @staticmethod
    def acqire_node_list(env: str, full_format: bool = False) -> Dict[str, Set[str]]:
        def make_node():
            if full_format:
                return f"http://{node}"
            else:
                return node.split('.')[0]
        nodelist = requests.get(f"http://tickdb3.{env}.zorg.sh/quote_api/v1/nodes").json()
        output = {}
        for item in nodelist:
            node = item.split('@')[-1]
            for k in requests.get(f"http://{node}/quote_api/v1/aggregates").json():
                t = k['type']
                if t in output:
                    output[t].add(make_node())
                else:
                    output[t] = {make_node()}
        return output
