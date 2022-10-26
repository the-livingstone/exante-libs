#!/usr/bin/env python3

import asyncio
import datetime
import json
import logging
import os
import re
import subprocess
import sys
import uuid
from libs.nexus import Nexus
from libs.async_sdb_additional import SDBAdditional
from dateutil import parser as date_parser


class FeedClient:
    """python wrapper for feed-client.jar"""

    cmd = 'java -classpath {} eu.exante.feed.client.cli.Main'
    env = None
    path = None
    nexus_project_name = 'feed-client'
    nexus_project_path = 'ghcg-internal/com/ghcg/gateway'

    def __init__(self, env='prod', path='feed-client.jar'):
        """
        class init method
        :param env: environment
        :param path: path to jar executable
        """
        self.env = env
        self.path = path

    def __repr__(self):
        return 'FeedClient({}, ver={})'.format(repr(self.env), repr(self.version()))

    def __build_data(self, _id, data, ignore_schedule=False, oneshot=False,
                     filter_by_schedule=None):
        """
        build data to send subscribe request
        :param _id: request id
        :param data: request data
        :param ignore_schedule: ignore or not the schedule
        :param oneshot: oneshot subsciption
        :param filter_by_schedule: filter data by schedule. If None ignore_schedule
        will be used
        :return: dict to send to feed-client
        """
        payload = {
            'command': 'subscribe',
            'subscriptionId': _id,
            'url': data['url'],
            'instrument': data['instrument'],
            'quoteLevel': data.get('level', 'best_price'),
            'forceSubscribe': ignore_schedule,
            'filterBySchedule': not ignore_schedule if filter_by_schedule is None else filter_by_schedule,
            'trades': data.get('trades', True),
            'auxData': data.get('auxData', True),
            'optionData': data.get('optionData', True),
            'oneshot': oneshot,
            'bondData': data.get('bondData',True),
            'enableSsl': data.get('enableSsl',True)
        }
        logging.info('feed-client payload is {}'.format(payload))
        return payload

    def __build_data_aux(self, inp):
        """
        build aux data
        :param inp: input dictionary
        :return: aux data dictionary
        """
        return {
            'high': inp.get('limitHigh'),
            'low': inp.get('limitLow'),
            'close': inp.get('lastSessionClose'),
            'open': inp.get('sessionOpen'),
            'dailyVolume': inp.get('dailyVolume'),
            'openInterest': inp.get('openInterest')
        }
    
    def __build_data_bond(self, inp):
        """
        build bond data
        :param inp: input dictionary
        :return: bond data dictionary
        """
        return {
            'accruedInterest': inp.get('accruedInterest'),
            'dirtyPrice':inp.get('dirtyPrice'),
            'yieldToMaturity':inp.get('yields',{}).get('yieldToMaturity'),
            'askYieldToMaturity':inp.get('yields',{}).get('askYieldToMaturity'),
            'bidYieldToMaturity':inp.get('yields',{}).get('bidYieldToMaturity'),
            'providerTimestamp':inp.get('providerTimestamp')
        }


    def __build_data_options(self, inp):
        """
        build options data
        :param inp: input dictionary
        :return: options data dictionary
        """
        return {
            'ts': inp['providerTimestamp'] if inp.get('providerTimestamp') else inp.get('timestamp', ''),
            'volatility': inp['impliedVolatility'],
            'price': inp['theoreticalPrice'],
            'delta': inp['delta'],
            'vega': inp['vega'],
            'gamma': inp['gamma'],
            'theta': inp['theta']
        }

    def __build_proc(self):
        """
        method to build process
        :return: pointer to process
        """
        self.check_app()
        command = self.cmd.format(self.path)
        return subprocess.Popen(command.split(), bufsize=1, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def __build_unsubscribe(self, _id):
        """
        build data to send unsubscribe request
        :param _id: request id
        :return: dict to send to feed-client
        """
        payload = {
            'command': 'unsubscribe',
            'subscriptionId': _id
        }
        logging.info('feed-client payload is {}'.format(payload))
        return payload

    def __calc_data(self, data):
        """
        calculate source data
        :param data: source data
        :return: appended and parsed data
        """
        parsed_data = dict()
        # calculate delay
        try:
            delta = date_parser.parse(data['timestamp']) - date_parser.parse(data['providerTimestamp'])
        except (AttributeError, TypeError):
            delta = datetime.timedelta(0, 0, 0)
        parsed_data['delay'] = delta.total_seconds()
        # build data
        parsed_data['data'] = list()
        if data['event'] == 'trade':
            # do nothing if it is trade
            parsed_data['data'].append({
                'bid': data['price'],
                'bid_size': data['size'],
                'ask': data['price'],
                'ask_size': data['size'],
                'matchesSchedule': data['matchesSchedule'],
                'spread': 0.0,
                'ts': data['providerTimestamp'] if data.get('providerTimestamp') else data.get('timestamp', ''),
                'type': 'trade'
            })
        elif data['event'] == 'quote':
            parsed_data['data'] = list()
            # append bids
            for bid in data['bid']['levels']:
                parsed_data['data'].append({
                    'bid': bid['price'],
                    'bid_size': bid['size'],
                    'ask': None,
                    'ask_size': None,
                    'matchesSchedule': data['matchesSchedule'],
                    'ts': data['providerTimestamp'] if data.get('providerTimestamp') else data.get('timestamp', ''),
                    'type': data['event']
                })
            # append asks
            for i in range(len(data['ask']['levels'])):
                try:
                    parsed_data['data'][i]['ask'] = data['ask']['levels'][i]['price']
                    parsed_data['data'][i]['ask_size'] = data['ask']['levels'][i]['size']
                except IndexError:
                    parsed_data['data'].append({
                        'bid': None,
                        'bid_size': None,
                        'ask': data['ask']['levels'][i]['price'],
                        'ask_size': data['ask']['levels'][i]['size'],
                        'matchesSchedule': data['matchesSchedule'],
                        'ts': data['providerTimestamp'] if data.get('providerTimestamp') else data.get('timestamp', ''),
                        'type': data['event']
                    })
            # calculate spreads
            for i in range(len(parsed_data['data'])):
                try:
                    parsed_data['data'][i]['spread'] = 100 * 2 * \
                        (parsed_data['data'][i]['ask'] - parsed_data['data'][i]['bid']) / \
                        (parsed_data['data'][i]['ask'] + parsed_data['data'][i]['bid'])
                except (ZeroDivisionError, TypeError):
                    # zero bid and ask? WTF?
                    parsed_data['data'][i]['spread'] = 0.0
        return parsed_data

    def __read_json(self, stdout):
        """
        method to read json
        :param stdout: stream from data be written
        :return: parsed json
        """
        while True:
            payload = stdout.readline().decode('utf8')
            try:
                json_output = json.loads(payload)
                stdout.flush()
                break
            except json.decoder.JSONDecodeError:
                print(payload)
        return json_output

    def __subscribe(self, _id, source, ignore_schedule, oneshot, stdin):
        """
        method to send subscribe request
        :param _id: ID to subscribe
        :param source: data to be sent
        :param ignore_schedule: ignore schedule
        :param oneshot: oneshot subscription
        :param stdin: stream to data be written
        """
        data = self.__build_data(_id, source, ignore_schedule, oneshot)
        stdin.write('{}\n'.format(json.dumps(data)).encode('utf8'))
        stdin.flush()

    def __terminate(self, proc):
        """
        method to terminate the process
        :param proc: pointer to the process
        """
        proc.stdin.close()
        proc.stdout.close()
        proc.stderr.close()
        proc.terminate()

    def __unsubscribe(self, _id, stdin):
        """
        method to send unsubscribe request
        :param _id: ID to unsubscribe
        :param stdin: stream to data be written
        """
        request = '{}\n'.format(json.dumps(self.__build_unsubscribe(_id)))
        stdin.write(request.encode('utf8'))
        stdin.flush()

    def check_app(self):
        """
        check if app exists
        :return: is application exist
        """
        if not os.path.isfile(self.path):
            Nexus().get_maven_app(self.nexus_project_path, self.nexus_project_name, self.path)

        return os.path.isfile(self.path)

    def version(self):
        """
        print and return adaptor version
        """
        ver = '0.0.2'
        logging.info('Feed client verson is {}'.format(ver))
        return ver

    def _generate_exante_id(self, option: str, strike: float, side: str):
        ticker, exchange, maturity = option.split('.')[:3]
        if re.search(r'\.0$', str(strike)):
            strike = int(strike)
        underline = str(strike).replace('.', '_')
        return f"{ticker}.{exchange}.{maturity}.{side[0]}{underline}"


    def prepare_data(self, symbol, status='active', symType=None):
        """
        parse symboldb and get feed gateway url
        :param symbol: symbol regexp to search
        :param status: search only for symbols with this status
        :return: dict, keys are symbolID, values are list of feed urls
        """
        data = {}
        sdbadds = SDBAdditional(self.env)
        sdb = sdbadds.sdb
        for s in asyncio.run(sdb.get_v2(symbol, fields=['symbolId', '_id', 'strikePrices'])):
            compiled = asyncio.run(sdbadds.build_inheritance(s['_id'], include_self=True))
            if not s.get('strikePrices'):
                data[str(uuid.uuid1())] = {
                    'data': list(),
                    'instrument': s['symbolId'],
                    'maxSpread': 100 * compiled.get('quoteFilters', dict()).get('maxSpread', 0.0),
                    'url': next((
                        x[2] for x
                        in asyncio.run(sdbadds.get_list_from_sdb('gateways', additional_fields=['feedAddress']))
                        if x[1] == next((
                            y['gatewayId'] for y
                            in compiled['feeds']['gateways']
                            if y['gateway'].get('enabled')
                        ), None)
                    ), None)
                }
            else:
                for side, strikes in s['strikePrices'].items():
                    for strike in strikes:
                            data[str(uuid.uuid1())] = {
                                'data': list(),
                                'instrument': self._generate_exante_id(s['symbolId'], strike['strikePrice'], side),
                                'maxSpread': 100 * compiled.get('quoteFilters', dict()).get('maxSpread', 0.0),
                                'url': next((
                                    x[2] for x
                                    in asyncio.run(sdbadds.get_list_from_sdb('gateways', additional_fields=['feedAddress']))
                                    if x[1] == next((
                                        y['gatewayId'] for y
                                        in compiled['feeds']['gateways']
                                        if y['gateway'].get('enabled')
                                    ), None)
                                ), None)
                            }
        return data

    def quotes(self, symbols, ignore_schedule=False, oneshot=False):
        """
        get quotes from server
        :param symbols: symbols dictionary in format of self.get_feed_urls()
        :param ignore_schedule: ignore schedule. Default is False
        :param trades: subscribe to trades
        :return: quotes generator
        """
        self.version()
        proc = self.__build_proc()
        for s in symbols:
            self.__subscribe(s, symbols[s], ignore_schedule, oneshot, proc.stdin)
        # read data
        while proc.poll() is None:
            feed_output = self.__read_json(proc.stdout)
            logging.info(feed_output)
            # read data
            if feed_output['event'] == 'snapshot':
                # check status first
                if not feed_output['feedStatus']['status'] == 'active':
                    yield {
                        'hasError': True,
                        'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                        'message': feed_output['feedStatus']['message'],
                        'url': symbols[feed_output['subscriptionId']]['url']
                    }
                # update schedule status
                yield {
                    'schedule': feed_output['feedStatus']['scheduleActive'],
                    'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                    'url': symbols[feed_output['subscriptionId']]['url']
                }
                # get last trade
                if feed_output.get('lastTrade') is not None:
                    feed_output['lastTrade']['event'] = 'trade'
                    parsed_data = self.__calc_data(feed_output['lastTrade'])
                    parsed_data['instrument'] = symbols[feed_output['subscriptionId']]['instrument']
                    parsed_data['url'] = symbols[feed_output['subscriptionId']]['url']
                    yield parsed_data
                # get quote
                if feed_output.get('quote') is not None:
                    feed_output['quote']['event'] = 'quote'
                    parsed_data = self.__calc_data(feed_output['quote'])
                    parsed_data['instrument'] = symbols[feed_output['subscriptionId']]['instrument']
                    parsed_data['maxSpread'] = symbols[feed_output['subscriptionId']]['maxSpread']
                    parsed_data['url'] = symbols[feed_output['subscriptionId']]['url']
                    yield parsed_data
                #get bond
                if feed_output.get('bondData') is not None and \
                    symbols[feed_output['subscriptionId']]['bondData']:
                    yield {
                        'bond':[self.__build_data_bond(feed_output['bondData'])],
                        'instrument':symbols[feed_output['subscriptionId']]['instrument'],
                        'url':symbols[feed_output['subscriptionId']]['url']
                    }
                # get aux
                if feed_output.get('auxData') is not None and  \
                        symbols[feed_output['subscriptionId']]['auxData']:
                    yield {
                        'aux': [self.__build_data_aux(feed_output['auxData'])],
                        'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                        'url': symbols[feed_output['subscriptionId']]['url']
                    }
                # get options data
                if feed_output.get('optionData') is not None and \
                        symbols[feed_output['subscriptionId']]['optionData']:
                    yield {
                        'option': [self.__build_data_options(feed_output)],
                        'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                        'url': symbols[feed_output['subscriptionId']]['url']
                    }
                # additional workaround for oneshot subscription
                if oneshot:
                    symbols[feed_output['subscriptionId']]['cancelled'] = True
            elif feed_output['event'] == 'quote' or feed_output['event'] == 'trade':
                parsed_data = self.__calc_data(feed_output)
                parsed_data['instrument'] = symbols[feed_output['subscriptionId']]['instrument']
                parsed_data['maxSpread'] = symbols[feed_output['subscriptionId']]['maxSpread']
                parsed_data['url'] = symbols[feed_output['subscriptionId']]['url']
                yield parsed_data
            # aux
            elif feed_output['event'] == 'aux_data':
                yield {
                    'aux': [self.__build_data_aux(feed_output)],
                    'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                    'url': symbols[feed_output['subscriptionId']]['url']
                }
            # option data
            elif feed_output['event'] == 'option_data':
                yield {
                    'option': [self.__build_data_options(feed_output)],
                    'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                    'url': symbols[feed_output['subscriptionId']]['url']
                }
            # schedule update
            elif feed_output['event'] == 'schedule_status':
                yield {
                    'schedule': feed_output['active'],
                    'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                    'url': symbols[feed_output['subscriptionId']]['url']
                }
            # cancels
            elif feed_output['event'] == 'subscription_cancel' or \
                    feed_output['event'] == 'subscription_failure':
                symbols[feed_output['subscriptionId']]['cancelled'] = True
                yield {
                    'hasError': feed_output['event'] == 'subscription_failure',
                    'instrument': symbols[feed_output['subscriptionId']]['instrument'],
                    'message': feed_output.get('reason'),
                    'url': symbols[feed_output['subscriptionId']]['url']
                }
            # cancel if nothing to do here or there are no quotes
            if all(symbols[s].get('cancelled') for s in symbols):
                return
