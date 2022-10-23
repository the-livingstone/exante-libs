#!/usr/bin/env python3

import datetime
import json
import logging
import requests
from requests.adapters import HTTPAdapter


class HttpFeed:
    """Class to work with http feed"""

    headers = {
        'content-type': 'application/json'
    }
    qapi = ''
    url = ''
    env = ''

    def __init__(self, env='prod'):
        self.env = env
        if env == 'test':
            self.url = 'http://internal-gateways.test.zorg.sh:8081'
        elif env == 'load':
            self.url = 'http://gw-feed1.load.zorg.sh:8081'
        elif env == 'stage':
            self.url = 'http://gateways9.stage.zorg.sh:8081'
        elif env == 'cprod':
            self.url = 'http://cryptogw20.cprod.zorg.sh:8081'
        elif env == 'prod':
            self.url = 'http://internal-gateways20.prod.zorg.sh:8081'
        elif env == 'cstage':
            self.url = 'http://gateways10.cstage.zorg.sh:8081'
        else:
            raise RuntimeError('No HTTP feed for {} environment'.format(self.env))
        
        self.qapi = '{}/feed/quote'.format(self.url)
        self.session = requests.Session()
        self.session.mount(self.url, HTTPAdapter(max_retries=5))

    def __repr__(self):
        return 'HttpFeed({})'.format(repr(self.env))

    def delete(self, symbol):
        """
        delete symbols from gateway
        :param symbol: symbol
        :return response object
        """
        response = self.session.delete(self.qapi, headers=self.headers, json={
            'symbol': symbol
        })
        logging.debug(response.url)
        logging.debug(response.status_code)
        return response

    def error(self, symbol, message):
        """
        post error message
        :param symbol: symbol
        :param message: message
        :return: response object
        """
        response = self.session.post('{}/feed/error'.format(self.url),
                                     headers=self.headers, json={
            'symbol': symbol,
            'message': message
        })
        logging.debug(response.url)
        logging.debug(response.status_code)
        return response

    def post(self, symbol, bid=None, bid_size=1, ask=None, ask_size=1, ts=None):
        """
        method to post quote to feed
        :param symbol: symbol
        :param bid: bid
        :param bid_size: bid size
        :param ask: ask
        :param ask_size: ask size
        :param ts: quote timestamp
        :return: response object
        """
        payload = {
            'symbol': symbol,
            'timestamp': ts if ts else datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        }
        if ask is not None:
            payload['ask'] = [{'price': ask,
                               'size': ask_size}]
        else:
            payload['ask'] = []
        if bid is not None:
            payload['bid'] = [{'price': bid,
                               'size': bid_size}]
        else:
            payload['bid'] = []
        response = self.session.post(self.qapi, headers=self.headers, json=payload)
        logging.debug(payload)
        logging.debug(response.url)
        logging.debug(response.status_code)
        logging.debug(response.text)
        # if 'exante.feed-adapter.http-api.quote-old-format = true' in gw-feed-http.conf
        if response.status_code == 400:
            if ask is not None:
                payload['ask'] = {'levels': payload['ask']}
            if bid is not None:
                payload['bid'] = {'levels': payload['bid']}
            response = self.session.post(self.qapi, headers=self.headers, json=payload)
            logging.debug(payload)
            logging.debug(response.url)
            logging.debug(response.status_code)
        return response

    def quote(self, symbol, bid=None, ask=None, size=None, ts=None):
        """
        method to post quote to feed
        @remark deprecated
        :param symbol: symbol
        :param bid: bid
        :param ask: ask
        :param size: bid and ask size
        :param ts: quote timestamp
        :return: response object
        """
        logging.warning('This method deprecated, use self.post instead')
        return
