#!/usr/bin/env python3

import requests
from requests.utils import quote
from requests import exceptions
import json
import logging
from retrying import retry
from libs.authdb import get_session, keyload, NotAuthorized
from os import path
from datetime import datetime, timezone


def conerror(exc):
    exception = [exceptions.ConnectionError, exceptions.Timeout, exceptions.ConnectTimeout, exceptions.ReadTimeout]
    print(exc, type(exc))
    return any([isinstance(exc, x) for x in exception])


class BackOffice:
    """BackOffice wrapping class, default API 2.0"""

    def __init__(self, env='prod', user=None, password=None,
                 credentials=('%s/credentials.json' % path.expanduser('~')),
                 cafile=''):
        self.sessionId = None
        self.env = env
        # if env == 'prod':
        #     self.url = f'https://backoffice.exante.eu'
        # else:
        self.url = f'https://backoffice.{self.env}.zorg.sh'

        # try:
        #     f = open(cafile, 'r')
        #     f.close()
        #     self.cafile = cafile
        # except FileNotFoundError:
        #     self.logger.warning('Exante SSL certificate is not found!')
        #     self.cafile = False
        
        if user and password:
            self.sessionId = get_session(user, password, 'backoffice', self.env)
        else:
            self.sessionId = keyload(credentials, self.env, 'backoffice')

        if self.sessionId is None:
            raise NotAuthorized('Either credentials file or user-pass must be provided')

        self.headers = {'content-type': 'application/json',
                        'Accept': 'application/json',
                        'x-use-historical-limits': 'false',
                        'accept-encoding': 'gzip',
                        'X-Auth-SessionId': self.sessionId}
        self.session = requests.Session()
        self.session.verify = False
        self.session.mount(self.url, requests.adapters.HTTPAdapter())

    def __repr__(self):
        return 'BackOffice({})'.format(repr(self.env))

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def __request(self, method, handle, params=None, jdata=None, version='2.0', headers=None):
        """
        wrapper method for requests
        :param method: requests method to be invoked
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        api = '{0}/api/v{1}'.format(self.url, version)
        r = method(api + handle, params=params, json=jdata, headers=headers)
        self.logger.debug('headers: {}'.format(headers))
        self.logger.debug('full url: {}'.format(r.url))
        if params:
            self.logger.debug('passed params: {}'.format(params))
        if jdata:
            self.logger.debug('passed json: {}'.format(jdata))
        if r.status_code > 209:
            self.logger.error("server returns error code {} while requesting\n{}\n{}"
                          .format(r.status_code, r.url, r.text))
        return r

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def __stream(self, handle, chunk_size=1, version='2.0'):
        """
        get stream
        :param handle: backoffice api handle
        :return: iterator object
        """
        header = self.headers
        header['accept'] = 'application/x-json-stream'
        api = '{0}/api/v{1}/streams'.format(self.url, version)
        response = self.session.get(api + handle, stream=True, headers=header, timeout=60)
        for chunk in response.iter_lines(chunk_size=chunk_size):
            yield chunk.decode('utf8')

    def get(self, handle, params=None, version='2.0'):
        """
        wrapper method for requests.get
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        response = self.__request(method=self.session.get, handle=handle, params=params, version=version)
        return response.json()

    def delete(self, handle, params=None):
        """
        wrapper method for requests.delete
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: requests response object
        """
        return self.__request(method=self.session.delete, handle=handle, params=params)

    def post(self, handle, jdata=None, params=None, headers=None, version='2.0'):
        """
        wrapper method for requests.post
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        return self.__request(method=self.session.post, handle=handle, jdata=jdata, params=params, headers=headers, version=version)

    def put(self, handle, jdata, params=None):
        """
        wrapper method for requests.put
        :param params:
        :param handle: backoffice api handle
        :param jdata: json to pass with request
        :return: requests response object
        """
        return self.__request(method=self.session.put, handle=handle, jdata=jdata, params=params)

    def clients(self, show_archived=False, **kwargs):
        """
        retirieves list of all clients
        :param show_archived: specifies if archived clients should be in the output
        :return: unmodified list of all clients
        """
        params = {}
        if show_archived:
            params = {'showArchived': 'true'}
        for k, v in kwargs.items():
            params[k] = str(v)

        return self.get('/clients', params)

    def client_get(self, client):
        """
        retrieves specific client
        :param client: name of the client
        :return: data for given client
        """
        return self.get('/clients/{}'.format(client))

    def client_accounts(self, client, **kwargs):
        """
        retrieves all client's accounts
        :param client: name of client
        :return: list of accounts for give client
        """
        params = {}
        if client:
            params = {'clientId': client}
        for k, v in kwargs.items():
            params[k] = str(v)
        return self.get('/accounts', params)

    def prices_get(self, instrument, date):
        """
        gets prices for specified instrument on current date from core
        :param instrument: instrument
        :param date: date in YYYY-MM-DD
        :return: list of dicts
        """
        params={
            'symbolId':instrument
        }
        return self.get('/prices/{}'.format(date),params)

    def client_post(self, client, jdata):
        """
        modifies data of specific client
        :param client: name of the client
        :param jdata: json data to be posted
        :return: requests response object
        """
        return self.post('/clients/{}'.format(client), jdata)

    def client_create(self,jdata):
        """
        creates a new client
        :param jdata: json data to be posted
        :return: requests response object
        """
        return self.post('/clients',jdata)

    def client_archive(self, client):
        """
        archives given client
        :param client: name of the client to archive
        :return: requests response object
        """
        data = self.client_get(client)
        data['archived'] = True
        return self.client_post(client, data)

    def client_unarchive(self, client):
        """
        unarchives given client
        :param client: name of the client to unarchive
        :return: requests response object
        """
        data = self.client_get(client)
        data['archived'] = False
        return self.client_post(client, data)

    def accounts(self, show_archived=False, legal_entity=None, fields_list=['id']):
        """
        retirieves list of all accounts
        :param legal_entity:
        :param fields_list:
        :param show_archived: specifies if archived accounts should be in the output
        :return: unmodified list of all clients
        """
        params = {}

        if show_archived:
            params.setdefault('showArchived', True)
        if legal_entity:
            params.setdefault('legalEntity', legal_entity)
        if len(fields_list) > 0:
            params.setdefault('fields', ','.join(fields_list))
        return self.get('/accounts', params)

    def account_info(self, account):
        """
        retrieve account info
        :param account:
        :return:
        """
        return self.get('/accounts/{}'.format(account))

    def account_cash_conversion(self, account, from_ccy, to_ccy, amount):
        """
        convert one currency to another on account
        :param account: account ID
        :param to_ccy: to currency
        :param from_ccy: from currency
        :param amount: amount to convert
        :return: response object
        """
        jdata = {
            'from': [{'currency': from_ccy, 'amount': amount}],
            'to': to_ccy
        }
        return self.post('/accounts/{}/cash_conversion'.format(account), jdata)

    def account_cash_conversion_full(self, account, to_ccy):
        """
        convert all cash for account
        :param account: account ID
        :param to_ccy: to currency
        :return: post request
        """
        ccys = [
            c['currency'] for c in self.account_summary(account)['currency']
            if c['currency'] not in ('XSD', to_ccy)
        ]
        if ccys:
            jdata = {
                'from': [{'currency': ccy, 'amount': "all"} for ccy in ccys],
                'to': to_ccy
            }
            return self.post('/accounts/{}/cash_conversion'.format(account), jdata)
        else:
            self.logger.info('No ccy found for conversion')

    def account_create(self, account):
        """
        create new account
        :param account: requested account id
        :return: request response object
        """
        jdata = {
            "accountType": "Liabilities",
            "archived": False,
            "id": account,
            "status": "full_access",
            "clientId": account.split('.')[0]
        }
        return self.post('/accounts/', jdata)

    def account_get(self, account):
        """
        retrieves specific account
        :param account: requested account id
        :return: data for given account
        """
        return self.get('/accounts/{}'.format(account))

    def account_post(self, account, jdata):
        """
        modifies data of specific account
        :param account: requested account id
        :param jdata: dictionary
        :return: requests response object
        """
        return self.post('/accounts/{}'.format(account), jdata)

    def account_benchmark_get(self, account):
        """
        Get selected account benchmark settings
        :param account: requested account id
        :return: benchmark data for given account
        """
        return self.get('/accounts/{}/benchmark'.format(account))

    def account_benchmark_post(self, account, jdata):
        """
        Post selected account benchmark settings
        :param account: requested account id
        :param jdata: dictionary
        :return: requests response object
        """
        return self.post('/accounts/{}/benchmark'.format(account), jdata)


    def routingTags_post(self, account, tag):
        """
        modifies data of specific account by adding new routingTag
        :param account: requested account id
        :param tag: string which contains routing tag
        :return: requests response object
        """
        jdata = self.account_get(account)
        jdata['routingTags'].append(tag)
        return self.account_post(account, jdata)

    def account_archive(self, account):
        """
        archives given account
        :param account: name of the account to archive
        :return: requests response object
        """
        payload = {'archived': True}
        return self.account_post(account, payload)

    def account_unarchive(self, account):
        """
        unarchives given account
        :param account: name of the account to unarchive
        :return: requests response object
        """
        data = self.account_get(account)
        data['archived'] = False
        return self.account_post(account, data)

    def account_summary(self, account, currency='EUR'):
        """
        retrieves account summary for given account
        :param account: account id
        :param currency: account currency, default is EUR
        :return: unchanged account summary
        """
        return self.get('/accounts/{}/summary/{}'.format(account, currency))

    def account_summary_historical(self, account, date, currency='EUR'):
        """
        retrieves account summary for given account
        :param account: account id
        :param date: date for histrorical requests
        :param currency: account currency, default is EUR
        :return: unchanged account summary
        """
        return self.get('/accounts/{}/summary/{}/{}'.format(account, date, currency))

    def account_purge(self, account):
        """
        removes all positions from account
        :param account: account id to purge
        :return: bool with success confirmation
        """
        if 'prod' in self.url:
            raise RuntimeError('Prod is a bad place to try this')
        return self.post('/accounts/{}/close_all_positions'.format(account), '').ok

    def generate_commission_rule(self, name, currency, commission_type, rate, rule,
                                 only_if_no_trades=False, day=None, rebate_rate=None,
                                 comment=None, note=None):
        """
        generate commission rule
        :param name: name of rule
        :param currency: currency of the rule
        :param commission_type: type, fixed or percent
        :param rate: commission rate, decimal
        :param rule: rule, daily, weekly, monthly
        :param only_if_no_trades: do commissions only if no trades in this period
        :param day: apply rule, used if rule=weekly or monthly, integer
        :param rebate_rate: rebate rate, optional
        :param comment: comment for the client
        :param note: internal note
        :return: valid data for the requests
        """
        if commission_type not in ('fixed', 'percent'):
            raise RuntimeError('Invalid commission type {}'.format(commission_type))
        if rule not in ('daily', 'weekly', 'monthly'):
            raise RuntimeError('Invalid rule {}'.format(rule))
        if rule != 'daily' and day is None:
            raise RuntimeError('Day is required')
        payload = {
            'name': name,
            'currency': currency,
            'commissionType': commission_type,
            'rate': rate,
            'ifNoTrades': only_if_no_trades
        }
        if rebate_rate:
            payload['rebateRate'] = rebate_rate
        if comment:
            payload['transactionComment'] = comment
        if note:
            payload['internalNotes'] = note
        payload['rule'] = {
            'type': rule
        }
        if rule == 'weekly':
            payload['rule']['weekDay'] = day
        elif rule == 'monthly':
            payload['rule']['day'] = day
        return payload

    def account_regular_commissions_bind(self, account, commission_id, active):
        """
        bind regular commission to account
        :param account: account ID
        :param commission_id: existing commission
        :param active: is bind active
        :return: response from request
        """
        jdata = {
            'commissionId': commission_id,
            'accountId': account,
            'active': active
        }
        return self.post('/account_regular_commissions', jdata)

    def account_regular_commissions_unbind(self, bind_id):
        """
        unbind regular commission from account
        :param bind_id: bind ID to unbind
        :return: response from request
        """
        return self.delete('/account_regular_commissions/{}'.format(bind_id))

    def account_regular_commissions_get(self, account=None, commission_id=None,
                                        archived=False):
        """
        get list of applied commissions to account
        :param account: account ID for search
        :param commission_id: commission ID for search
        :param archived: show archived
        :return: requests response object
        """
        payload = {
            'showArchived': archived
        }
        if account:
            payload['accountId'] = account
        if commission_id:
            payload['commissionId'] = commission_id
        return self.get('/account_regular_commissions', params=payload)

    def account_commissions_get(self, account):
        """
        retrieve commissions set to specific account
        :param account: account to retrieve commissions
        :return: json with commissions
        """
        return self.get('/accounts/{}/commissions'.format(account))

    def account_commissions_post(self, account, jdata):
        """
        post commissions to specific account
        :param account: account to post commissions on
        :param jdata: json data with commissions to be posted
        :return: requests response object
        """
        return self.post('/accounts/{}/commissions'.format(account), jdata)

    def account_commissions_reset(self, account):
        """
        reset account commissions to default ones
        :param account: account to reset
        :return: requests response object
        """
        data = self.account_commissions_get(account)
        for i in data:
            i['override'] = False
        return self.account_commissions_post(account, data)

    def account_overnights_get(self, account):
        """
        retrieve overnights for specific account
        :param account: account to retrieve overnights
        :return: json with overnights
        """
        return self.get('/accounts/{}/rates/overnights'.format(account))

    def account_overnights_post(self, account, jdata):
        """
        post overnights to specific account
        :param account: account to post overnights on
        :param jdata: json data with overnights to be posted
        :return: requests response object
        """
        return self.post('/accounts/{}/rates/overnights'.format(account), jdata)

    def account_permissions_get(self, account):
        """
        retrieve account permissions
        :param account: account to get permissions for
        :return: json with account permissions
        """
        return self.get('/accounts/{}/permissions'.format(account))

    def account_permissions_post(self, account, jdata):
        """
        post permissions data to specific account
        :param account: account to post permissions
        :param jdata: json data to post
        :return: requests response object
        """
        return self.post('/accounts/{}/permissions'.format(account), jdata)

    def account_permissions_reset(self, account):
        """
        reset account permissions to defauls
        :param account: account to reset permissions
        :return: requests response object
        """
        return self.delete('/accounts/{}/permissions'.format(account))

    def account_leverage_rates_get(self, account):
        """
        retrieve account leverage rates overrides
        :param account: account to get leverage rates for
        :return: json with account leverage rates
        """
        return self.get('/accounts/{}/rates/leverages'.format(account))

    def account_leverage_rates_post(self, account, jdata):
        """
        post account leverage rates overrides
        :param account: account to post leverage rates for
        :param jdata: json data to post
        :return: requests response object
        """
        headers = self.headers.copy()
        headers['Accept'] = '*/*'
        return self.post('/accounts/{}/rates/leverages'.format(account), jdata, headers=headers)

    def account_limits_post(self, account, jdata):
        """
        retrieve account leverage rates overrides
        :param account: account to get leverage rates for
        :return: json with account leverage rates
        """
        return self.post('/accounts/{}/limits'.format(account), jdata)

    def account_users(self, account):
        """
        list all users, who have access to given account
        :param account: account to list users for
        :return: json with users with access to given account
        """
        return self.get('/user_accounts', params={'accountId': account})

    def user_client(self, user):
        """
        user clientId
        :param user: username
        :return: list with dict
        """
        return self.get('/user_client', params={'userId': user})

    def client_user(self, client):
        """
        client userId
        :param client: clientId
        :return: list with dict
        """
        return self.get('/user_client', params={'clientId': client})

    def user_permissions_get(self, user):
        """
        retrieve user permissions
        :param user: user to get permissions for
        :return: json with user permissions
        """
        return self.get('/users/{}/permissions'.format(quote(user)))

    def user_permissions_post(self, user, jdata):
        """
        post permissions data to specific user
        :param user:
        :param jdata: json data to post
        :return: requests response object
        """
        return self.post('/users/{}/permissions'.format(quote(user)), jdata)

    def user_permissions_set_get(self, id):
        """
        get user permissions set with id
        :param id: permissions set id
        :return: list with users dicts
        """
        params = {'permissionsSetId': id}
        return self.get('/user_permissions_sets', params)

    def user_permissions_set_delete(self, id):
        """
        delete user permission set with id
        :param id: permissionsetid
        :return: response
        """
        return self.delete('/user_permissions_sets/{id}'.format(id=id))

    def permissions_sets_get(self):
        """
        get user permissions set with id
        :return: list with users dicts
        """
        return self.get('/permissions/sets')

    def permissions_sets_overrides_get(self,id):
        """
        get user permissions set with id
        :param id: permissions set id
        :return: list with users dicts
        """
        params= {'withExpired':False}
        return self.get('/permissions/sets/{}/overrides'.format(id),params)

    def permissions_sets_overrides_post(self,id,data):
        """
        get user permissions set with id
        :param id: permissions set id
        :param data: data for permissions sets
        :return: list with users dicts
        """
        return self.post('/permissions/sets/{}/overrides'.format(id),jdata=data)

    def user_permissions_sets_get(self, user=None, permissionSetId=None):
        """
        gets permissions data for specific user or permissionSet
        :param user: userId
        :param permissionSetid: permission set id
        """
        params={}
        if user:
            params.update({'userId':user})
        if permissionSetId:
            params.update({'permissionsSetId':permissionSetId})

        return self.get('/user_permissions_sets', params)

    def mirroring_rules_get(self, targetAccount=None):
        """
        gets mirroring rules for specific acc if specified
        :param targetAccount: accountId
        """
        params={}
        if targetAccount:
            params.update(
                {'targetAccount':targetAccount}
            )
        return self.get('/mirroring_rules',params)

    def user_permissions_set_post(self, jdata):
        """
        post permissions data to specific user
        :param jdata: json data to post
        :return: requests response object
        """
        return self.post('/user_permissions_sets', jdata)

    def user_permissions_reset(self, user):
        """
        reset users permissions to default state
        :param user: user to reset permissions
        :return: requests response object
        """
        return self.delete('/users/{}/permissions'.format(quote(user)))

    def user_accounts(self, user, show_archived: bool = False):
        """
        retrieve a list of accounts user have access to
        :param user: user to request a list for
        :param show_archived: show archived accounts
        :returm: json with requested data
        """
        params = {'userId': user,
                  'showArchived': 'true' if show_archived else 'false'}
        return self.get('/user_accounts', params)

    def user_accounts_post(self, user, accounts: list, perm):
        """
        :param user: user to change perms
        :param accounts: accounts to change perms
        :param perm: valid: blocked, full_access, close_only, read_only
        :return: post json to backoffice
        """
        jdata = list()
        if type(accounts) != list:
            accounts = [accounts]
        if perm not in ['full_access', 'close_only', 'read_only', 'blocked']:
            raise TypeError
        for acc in accounts:
            jdata.append({
                "accountId": acc,
                "overrideAccountStatus": True,
                "status": perm,
                "userId": user
            })
        return self.post('/user_accounts/', jdata)

    def user_permissions_atp_get(self, user):
        """
        return 'special' permissions for user
        :param user: username
        :return: json
        """
        return self.get('/users/{}/permissions/atp'.format(user))

    def user_permissions_atp_post(self, user, jdata):
        """
        return 'special' permissions for user
        :param user: username
        :param jdata: json data to updload
        :return: json
        """
        return self.post('/users/{}/permissions/atp'.format(user), jdata)

    def access_link_get(self, _id):
        """
        retrieve data held by speciffic access id
        :param _id: id to retrieve data for
        :return: json data from geven id
        """
        return self.get('/user_accounts/{}'.format(_id))

    def access_link_post(self, _id, jdata):
        """
        modifies data held by specific access link
        :param _id: id of the link to modify
        :param jdata: data to upload
        """
        return self.post('/user_accounts/{}'.format(_id), jdata)

    def access_link_create(self, user, account, status, override=False):
        """
        creates new user account access link
        :param user: user to give access to
        :param account: account to give access to
        :param status: access level to give
        :param override: specifies if it should override default status
        """
        jdata = {
            'userId': user,
            'accountId': account,
            'status': status,
            'overrideAccountStatus': override
        }
        return self.post('/user_accounts/', jdata)

    def access_link_delete(self, _id):
        """
        remove access link between account and user
        :param _id: id of the link between user and account
        :return: requests response object
        """
        return self.delete('/user_accounts/{}'.format(_id))

    def default_commissions_get(self):
        """
        retrieve default commissions
        :return: json with default commissions data
        """
        return self.get('/commissions')

    def default_commissions_post(self, jdata):
        """
        update default commissions with given data
        :param jdata: json to upload
        :return: requests response object
        """
        return self.post('/commissions', jdata)

    def default_overnights_get(self):
        """
        retrieve default overnights
        :return: json with default overnights data
        """
        return self.get('/rates/overnights', version='2.1')

    def default_overnights_post(self, jdata):
        """
        update default overnights with given data
        :param jdata: json to upload
        :return: requests response object
        """
        return self.post('/rates/overnights', jdata, version='2.1')

    def default_markup_get(self):
        """
        retrieve default markup rates
        :return: json with default overnights data
        """
        return self.get('/rates/overnights/markup')

    def default_markup_post(self, jdata):
        """
        update default markup with given data
        :param jdata: json to upload
        :return: requests response object
        """
        return self.post('/rates/overnights/markup', jdata)

    def default_benchmark_get(self):
        """
        retrieve default benchmark rates
        :return: json with default benchmark rates data
        """
        return self.get('/rates/overnights/benchmark')

    def default_benchmark_post(self, jdata):
        """
        update default benchmark with given data
        :param jdata: json to upload
        :return: requests response object
        """
        return self.post('/rates/overnights/benchmark', jdata)

    def default_permissions_get(self, expired: bool = False):
        """
        retrieve default permissions
        :return: json woth default permissions data
        """
        params = {'withExpired': str(expired).lower()}
        return self.get('/permissions', params=params)

    def default_permissions_post(self, jdata):
        """
        update default permissions with given data
        :param jdata: json to upload
        :return: requests response object
        """
        return self.post('/permissions', jdata)

    def transfer_commission_get(self, currency):
        """
        retrieve transfer commission on specified currency
        :param currency:
        :return:
        """
        return self.get('/transfer/commissions/{}'.format(currency))

    def transfer_commission_post(self, currency, valmin: str, valper: str, valfix: str):
        """
        set transfer commission on specified currency
        :param currency:
        :param valmin: minimum commission
        :param valper: percentage commission
        :param valfix: fixed commission
        :return:
        """
        data = {
            "fixed": str(valfix),
            "min": str(valmin),
            "percent": str(valper)
        }
        return self.post('/transfer/commissions/{}'.format(currency), jdata=data)

    def transfer_commission_delete(self, currency):
        """
        delete transfer commission on specified currency
        :param currency:
        :return:
        """
        return self.delete('/transfer/commissions/{}'.format(currency))

    def transfer_commission_getall(self):
        """
        retrieve all transfer commissions
        :return:
        """
        return self.get('/transfer/commissions')

    def transfer_commission_postall(self, jdata: list):
        """
        mass update all transfer commissions
        :param jdata: list of dicts with format [...,{"currency": str, "fixed": str, "min": str, "percent": str}]
        :return:
        """
        return self.post('/transfer/commissions', jdata=jdata)

    def transaction_get(self, fr=None, to=None, account=None, fields=None, **kwargs):
        """
        retrive transactions list from bo
        :param fr: from timestamp
        :param to: to timestamp
        :param account: account name
        :param asset: asset name or regexp
        :param who: who made the transaction
        :param fields: return only given fields
        """
        params = {}

        if fr:
            params.update({'fromDate': fr.strftime('%Y-%m-%dT%H:%M:%SZ')})
        if to:
            params.update({'toDate': to.strftime('%Y-%m-%dT%H:%M:%SZ')})
        if account:
            params.update({'accountId': account})
        if fields:
            params.update({'fields': ','.join(fields)})
        for k, v in kwargs.items():
            params[k] = str(v)

        return self.get('/transactions', params=params)

    def transaction_post(self, data):
        """
        post transaction to backoffice
        :param data: json model
        [{
        "accountId": "string",
        "amount": "1000.0",
        "asset": "EUR",
        "operationType": "FUNDING/WITHDRAWAL",
        "price": "",
        "symbolId": "EUR",
        "useAutoCashConversion": "false"}]
        :return: requests response object
        """
        return self.post('/transactions', data)

    def transaction_rollback(self, trid):
        """
        rollback transaction with given id
        :param trid: id
        :return:
        """
        return self.post('/transaction/rollback/{}'.format(trid), jdata=None)

    def quotecache_quotes(self, params=None):
        """
        retrieves list of all quotes from bo
        :param params: additional filtering params
        :return: json with quotes data
        """
        return self.get('/quotecache/quotes', params=params)

    def quotecache_quote(self, symbol):
        """
        retrieves json with quote data for specific symbol
        :param symbol: symbolId of the symbol to retrieve quote
        :return: json with quote data
        """
        return self.quotecache_quotes({'symbolId': symbol})

    def cross_rates(self, fr: str, to: str, time: datetime = None):
        """
        retrieves json with cross rate from _fr_ currency to _to_ one
        :param fr: source currency
        :param to: destination currency
        :param time: crossrate at desired time
        :return: json with cross rate
        """
        params = {'from': fr,
                  'to': to,
                  'time': time.strftime("%Y-%m-%dT%H:%M:%SZ") if time else None}
        return self.get('/quotecache/crossrates', params=params)

    def global_summary(self, currency='EUR', date=None, group_accounts=False, symbol=None):
        """
        retrieves global summary
        :param currency:
        :param date:
        :param group_accounts: group by accounts
        :param symbol: global summary only for symbol
        :return: json with global summary
        """
        params = {}
        if date:
            url = '/summary/{}/{}'.format(date, currency)
        else:
            url = '/summary/{}'.format(currency)
        if group_accounts:
            params.setdefault('byAccounts', True)
        if symbol:
            params.setdefault('symbolId', symbol)
        return self.get(url, params=params)

    def global_metrics(self, currency='EUR', date=None, exclude_accounts=False):
        """
        retrieve global metric
        :param currency: report currency, default is EUR
        :param date: if specified, returns historical metrics
        :param exclude_accounts: is specified, exlude accounts from report
        :return:
        """
        if date:
            url = '/metrics/{}/{}'.format(date, currency)
        else:
            url = '/metrics/{}'.format(currency)
        params = {'excludeAccounts': 'true' if exclude_accounts else 'false'}
        return self.get(url, params=params)['metrics']

    def global_accounts(self):
        """
        retrieves global accounts
        :return: json with global accounts
        """
        params = {'byAccounts': 'true'}
        return self.get('/summary/EUR', params=params)

    def symbols(self, fields=None, params=None):
        """
        retrieve data for all symbols from bo
        :param fields: return only given fields
        :param params: additional params (if any)
        :return: json with symbols data
        """
        if fields:
            params.update({'fields': ','.join(fields)})
        return self.get('/symbols/', params=params)

    def symbol(self, symbol):
        """
        retrieve data for specific symbol in bo
        :param symbol: symbol :D
        :return: json with symbol data
        """
        params = {'prefixSymbolId': symbol}
        return self.get('/symbols', params=params)

    def lost_symbols(self):
        """
        retrieve lost symbols
        :return: list of lost symbols
        """
        # return self.get('/symbols/lost')
        # https://trello.com/c/GIuqjnqP/3184-api-symbols-used-symbols-lost-not-available-with-auth
        return self.get('/symbols/lost')

    def used_symbols(self):
        """
        retrieve list of symbols ever mentioned in account summary
        :return: list of used symbols
        """
        # return self.get('/symbols/used')
        # https://trello.com/c/GIuqjnqP/3184-api-symbols-used-symbols-lost-not-available-with-auth
        return self.get('/symbols/used')

    def symbol_default_overnight(self, symbol):
        """
        retrive default overnight markup rates for given symbol
        :param symbol: EXANTEId
        :return: list with dict
        """
        params = {'symbolId': symbol}
        return self.get('/rates/overnights/symbol', params=params)

    def interests(self):
        """
        retrieve default interests
        :return:
        """
        return self.get('/rates/interests')

    def interests_account_get(self, account):
        """
        retrieve default interests
        :return:
        """
        return self.get('/accounts/{}/rates/interests'.format(account))

    def interests_account_post(self, account, jdata):
        """
        retrieve default interests
        :return:
        """
        return self.post('/accounts/{}/rates/interests'.format(account), jdata)

    def intermonth_spread_get(self):
        """
        retrieve intermonth spread data
        :return: json with intermonth spread data
        """
        return self.get('/intermonth_spread_margin')

    def intermonth_spread_post(self, jdata):
        """
        update intermonth spreads with given data
        :param jdata: data to upload
        :return: requests response object
        """
        return self.post('/intermonth_spread_margin', jdata)

    def regular_commissions_create(self, jdata):
        """
        create regular commission rule
        :param jdata: commision rule data
        :return: response from request
        """
        return self.post('/regular_commissions', jdata)

    def regular_commissions_update(self, _id, jdata):
        """
        update existing commission rule
        :param _id: commission ID
        :param jdata: commission data
        """
        return self.post('/regular_commissions/{}'.format(_id), jdata)

    def risk_arrays_get(self, symbol=None):
        """
        retrieve risk arrays
        :param symbol: EXANTEId or symbol regexp
        :return: json with ra data
        """
        params = {'symbolId': symbol}
        return self.get('/riskarrays', params=params)

    def risk_arrays_history(self, symbol, since: datetime=None, till: datetime=None, limit=None ):
        """
        retrieve historical risk arrays
        :param symbol: EXANTEId
        :param since: start date for arrays
        :param till: end date for arrays
        :param limit: arrays returned. Default is all found
        :return: json with ra data
        """
        params = {'symbolIds': symbol}
        if since:
            params.update({'fromDate': since})
        if till:
            params.update({'toDate': till})
        if limit:
            params.update({'limit': limit})
        return self.get('/riskarrays/history', params=params)

    def risk_arrays_post(self, jdata):
        """
        update risk arrays with given data
        :param jdata: data to upload
        :return: requests response object
        """
        return self.post('/riskarrays', jdata)

    def stream_debug(self, handle: str, version='2.0'):
        """
        debug function to get user-defined stream
        :param handle: url of stream, should be smth like /permissions
        :param version: version of api, should be smth like 2.0
        :return:
        """
        streamlist = list()
        response = self.__stream(handle, version=version)
        for item in response:
            data = json.loads(item)
            self.logger.debug('Received data {}'.format(data))
            if data['$type'] not in {'sync', 'heartbeat'}:
                streamlist.append(data)
            elif data['$type'] == 'heartbeat':
                self.logger.debug('Heartbeat received')
            elif data['$type'] == 'sync':
                break
        return streamlist

    def stream_permissions(self):
        """
        get permissions stream
        :return: json data from stream

        returs
        [..,
        {'allowShort': False,
         'canTrade': True,
         'canView': None,
         'symbolId': 'OG.COMEX.Z2016.P1165'},
         ..]
        """
        permissions = list()

        response = self.__stream('/permissions', version='2.2')
        for item in response:
            data = json.loads(item)
            self.logger.debug('Received data {}'.format(data))
            if data['$type'] == 'account_permission' \
                    or data['$type'] == 'user_permission' \
                    or data['$type'] == 'default_permission':
                permissions.append(data)
            elif data['$type'] == 'heartbeat':
                self.logger.debug('Heartbeat received')
            elif data['$type'] == 'sync':
                break
        return permissions

    def stream_limits(self):
        """
        get limits stream
        :return: json data from stream

        returs
        [..,
        {'$type': 'limit',
        'accountId': '-4HY7JV8z-.123',
        'mode': 'upper_cap',
        'negativeLim': '-13.0',
        'positiveLim': '2.0',
        'symbolId': 'GBP/EUR.TEST.19X2020'},
         ..]
        """
        limits = list()

        response = self.__stream('/limits', version='2.0')
        for item in response:
            data = json.loads(item)
            self.logger.debug('Received data {}'.format(data))
            if data['$type'] not in {'heartbeat', 'sync'}:
                limits.append(data)
            elif data['$type'] == 'heartbeat':
                self.logger.debug('Heartbeat received')
            elif data['$type'] == 'sync':
                break
        return limits

    def tradeadapter_issues(self):
        """
        retrieve issues list for bo tradeadapter
        :return: list of issues
        """
        issues = self.get('/tradeadapter/issues')
        for i in issues:
            if 'trade' in i:
                i['trade'] = json.loads(i['trade'])
        return issues

    def tradeadapter_repost(self, issue, jdata):
        """
        repost issue to tradeadapter
        :param issue: id of the issue to fix
        :param jdata: trade data
        :return: requests response object
        """
        return self.post('/tradeadapter/issues/{}'.format(issue), jdata)

    def tradeadapter_repost_all(self):
        """
        repost all issues to tradeadapter
        :return: Boolean depending on operation success
        """
        return all([self.tradeadapter_repost(x['id'], x['trade']).ok for
                    x in self.tradeadapter_issues()])

    def server_info(self):
        """
        retrieve server info
        :return: json with server info
        """
        return self.get('/server_info')

    def ping(self):
        """
        check backoffice availiability
        :return: boolean
        """
        return 'version' in self.server_info().keys()

    def commission_groups(self):
        """
        retrieve list of commission groups
        :return: list of groups
        """
        return self.get('/commissions/groups')

    def commission_group_get(self, group_id):
        """
        retrieve commissions set to specific group
        :param group_id: id of group to retrieve commissions
        :return: json with commissions
        """
        return self.get('/commissions/groups/{}/overrides'.format(group_id))

    def commission_group_post(self, group_id, jdata):
        """
        post commissions to specific group
        :param group_id: id of group to post commissions on
        :param jdata: json data with commissions to be posted
        :return: requests response object
        """
        return self.post('/commissions/groups/{}/overrides'.format(group_id), jdata)

    def commission_group_add(self, group_name):
        """
        create group with specified name
        :param group_name:
        :return: json with name and id of created group
        """
        jdata = {
            'name': group_name
        }
        return self.post('/commissions/groups', jdata).json()

    def commission_group_rename(self, group_id, group_name):
        """
        rename existing commission group
        :param group_id: id of group that should be renamed
        :param group_name: new group name
        :return: requests response object
        """
        jdata = {
            'id': group_id, 'name': group_name
        }
        return self.post('/commissions/groups/{}'.format(group_id), jdata)

    def commission_group_remove(self, group_id):
        """
        remove commision group
        :param group_id: id of group that should be removed
        :return: requests response object
        """
        return self.delete('/commissions/groups/{}'.format(group_id))

    def open_positions(self, types=None, expired: bool = False, fields: list = None):
        """
        retrieve list of open positions
        :param types: type of symbols to retrieve
        :param expired: retrieve only positions of expired symbols
        :param fields: retrieve only selected fields, list from {type, expired, symbolId}
        :return: json symbols
        """
        params = {'type': types.lower() if types else None,
                  'expired': 'true' if expired else 'false',
                  'fields': ','.join(fields) if fields else None}
        return self.get('/symbols/open_positions', params=params)

    def cards_get(self, account=None, status=None, deleted: bool = False, le=None):
        """
        retrieve existed cards from backoffice
        :param account: if None returns all existed cards
        :param status: status filter, valid values: {'Deleted', 'Blocked', 'Active', 'Ready for activation',
        'Card is preparing', 'Card is requested'}
        :param deleted: if True returns even deleted cards (status==Deleted)
        :param le: LE filter, valid values: {'PLBCF', 'Malta', 'Cyprus'}
        :return: json cards
        """
        available_statuses = {'Deleted', 'Blocked', 'Active', 'Ready for activation',
                              'Card is preparing', 'Card is requested'}
        params = {'status': status if status in available_statuses else None,
                  'legalEntity': le if le in {'PLBCF', 'Malta', 'Cyprus'} else None,
                  'accountId': account if account else None,
                  'showDeleted': 'true' if deleted else 'false'}
        return self.get('/intercash_cards', params=params)

    def card_get_by_id(self, cardid):
        """
        retrieve card details via id
        :param cardid: id
        :return: json card
        """
        return self.get('/intercash_cards/{}'.format(cardid))

    def card_create(self, account, currency, issue=None, status=None, **kwargs):
        """
        create new card
        :param account: account
        :param currency: currency code
        :param issue: issueId of card, should be == cardid or intercash issueId
        :param status: valid statuses are {'Deleted', 'Blocked', 'Active', 'Ready for activation',
        'Card is preparing', 'Card is requested'}
        :param kwargs: additional fields, like san, pan, issueId or expiryDate
        :return:
        """
        payload = {'accountId': account,
                   'currency': currency.upper(),
                   'issueId': str(issue),
                   'status': 'Card is preparing' if status is None else status}
        for k, v in kwargs.items():
            payload[k] = str(v)
        return self.post('/intercash_cards', jdata=payload)

    def card_update(self, cardid, payload: dict):
        """
        update card details via id
        :param cardid: id
        :param payload: change card details with data provided
        :return: json card
        """
        return self.post('/intercash_cards/{}'.format(cardid), jdata=payload)

    def card_delete(self, cardid):
        """
        retrieve card details via id
        :param cardid: id
        :return: json card
        """
        return self.delete('/intercash_cards/{}'.format(cardid))

    def rebates_all(self):
        """
        get all rebates
        :return: [...,{"rebatesTo": account, "percent": "0.5", "accountId": originalAccount},...]
        """
        return self.get('/rebate')

    def rebate_from(self, account):
        """
        retrieve all accounts, which sends rebates to target account
        :param account:
        :return:
        """
        return self.get('/accounts/{}/rebate_for'.format(account))

    def rebate_to(self, account):
        """
        retrieve all accounts, sending rebates to from target account
        :param account:
        :return:
        """
        return self.get('/accounts/{}/rebate_accounts'.format(account))

    def rebate_add(self, account, settings: list or tuple):
        """
        add rebate setting
        :param account: target account
        :param settings: tuple ("rebate_account", "proportion") or list of tuples
        :return:
        """
        payload = list()
        if type(settings) == tuple:
            payload.append({"id": settings[0], "percent": str(settings[1])})
        else:
            for item in settings:
                payload.append({"id": item[0], "percent": str(item[1])})
        return self.post('/accounts/{}/rebate_accounts'.format(account), jdata=payload)

    def rebate_del(self, account, rebate_account):
        """
        deletes rebate settings
        :param account: target account
        :param rebate_account: rebate account
        :return:
        """
        return self.delete('/accounts/{}/rebate_account/{}'.format(account, rebate_account))

    def rebate_change(self, account, rebate_account, newvalue):
        """
        deletes rebate settings
        :param account: target account
        :param rebate_account: rebate account
        :param newvalue: new rebate percent to set
        :return:
        """
        return self.post('/accounts/{}/rebate_account/{}'.format(account, rebate_account), jdata=str(newvalue))

    def trades_get(self, order=None, account=None, user=None, symbol=None, regexp=None, symboltype=None,
                   start=None, end=None, limit=10, le=None, offset=None, reverse=False, fields=None, **kwargs):
        """
        method retrieve trades from bo
        :param order: original orderId
        :param account: single account or accounts
        :param user: user
        :param symbol: ExanteId of symbol
        :param symboltype: ExanteId type
        :param start: datetime or iso8601
        :param end: datetime or iso8601
        :param limit: trades returned. Default limit is 10
        :param le: LegalEntity filter, available are Malta, Cyprus, PLBCF or None
        :param offset: start from trade number
        :param reverse: if True starts from older trades, rather than from new ones
        :param kwargs: additional parameters
        :return:
        """
        payload = {'orderId': order,
                   'accountId': account,
                   'userId': user,
                   'symbolId': symbol,
                   'symbolType': symboltype,
                   'limit': limit,
                   'legalEntity': le,
                   'accountId_regexp': regexp}
        if type(start) == datetime:
            payload['beginDate'] = start.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            payload['beginDate'] = start
        if type(end) == datetime:
            payload['endDate'] = end.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            payload['endDate'] = end
        if offset:
            payload['start'] = offset
        if reverse:
            payload['order'] = 'asc'
        if fields:
            payload['fields'] = ','.join(fields)
        for k, v in kwargs.items():
            payload[k] = v
        return self.get('/trades', params=payload)

    def reload_prices(self, date, symbols: list):
        """
        reloads prices from external storage
        :param date: iso8601
        :param symbols: list of symbolIds
        :return:
        """
        return self.post('/prices/{}/reload'.format(date), jdata={'symbolIds': symbols})

    def drop_historical_cache(self):
        """
        drops historical cache
        :return:
        """
        params = {'allNodes': True}
        headers = {'X-Auth-SessionId': self.sessionId}
        return self.post('/quotecache/drop_historical_cache', params=params, headers=headers)

    def recalc_historical_metrics(self, date):
        """
        recalculates historical metrics for specified date
        :param date: iso8601
        :return:
        """
        headers = {'X-Auth-SessionId': self.sessionId}
        return self.post(handle='/recalc_historical_metrics/{}'.format(date), headers=headers)

    def feed_permissions_get(self):
        """
        List all known default feed permissions
        """
        return self.get('/feed_permissions')


    def users_feed_permissions_get(self, userId = None, marketDataGroup = None, feedPermission = None, feedType = None):
        """
        retrieve all known user feed permissions
        :param userId: authdb userId
        :param marketDataGroup: Market data group name
        :param feedPermission: feed permission name, valid values are {"realtime","delayed"}
        :param feedType: valid values are {"retail","professional"}
        :return: list of permissions
        """
        params = {}
        if userId:
            params.update({'userId': userId})
        if marketDataGroup:
            params.update({'marketDataGroup':marketDataGroup})
        if feedPermission:
            params.update({'feedPermission':feedPermission})
        if feedType:
            params.update({'feedType':feedType})

        return self.get('/users_feed_permissions',params=params)

    def feed_permissions_post(self, marketDataGroup, feedPermission = "realtime", currency = "EUR", professionalFeedPrice = 0, retailFeedPrice = 0, simpleSubscription = False ):
        """
        creates a new feed permission
        :param marketDataGroup: Market data group name
        :param feedPermission: feed permission name, valid values are {"realtime","delayed"}
        :param currency: fee currency, default is EUR
        """
        jdata=[{
            'marketDataGroup':marketDataGroup,
            'feedPermission':feedPermission,
            'currency':currency,
            'professionalFeedPrice':professionalFeedPrice,
            'retailFeedPrice':retailFeedPrice,
            'simpleSubscription':simpleSubscription
        }]

        return self.post('/feed_permissions', jdata)

    def user_feed_permissions_post(self, user, marketDataGroup, feedPermission = "realtime", feedType = "retail"):
        """
        updates/creates new user feed permissions
        :param user: userId
        :param feedPermission: "realtime" to turn on, or "delayed" to turn off
        :param feedType: "retail" or "professional"
        :param marketDataGroup: Market data group name

        """
        jdata=[{
            'userId':user,
            'marketDataGroup':marketDataGroup,
            'feedPermission':feedPermission,
            'feedType':feedType
        }]
        return self.post('/users_feed_permissions', jdata=jdata)

    def feed_permissions_id_post(self, permission_id, jdata):
        """
        update single data
        :param permission_id: permission id data
        :param jdata: data to post [dict with fields that should be update]
        :return: status
        """
        headers = dict(self.headers)
        headers.update({'x-check-account-permissions': 'true'})
        return self.post('/users_feed_permissions/{}'.format(permission_id), jdata=jdata, headers=headers)

    def feed_permissions_mass_post(self, jdata: list):
        """
        update/create data
        :param jdata: data to post [list of dict with fields: userId, marketDataGroup, feedPermission, feedType]
        :return: status
        """
        headers = dict(self.headers)
        headers.update({'x-check-account-permissions': 'true'})
        return self.post('/users_feed_permissions', jdata=jdata, headers=headers)

    def currencies_holidays(self, date_from=None, date_to=None):
        """
        list all available currencies holidays
        :param date_from: specifies date_from
        :param date_to: specifies date_to
        :return: unmodified list of dicts with currencies and dates
        """
        params = {}
        params['from'] = date_from
        params['to'] = date_to

        return self.get('/currencies/holidays', params)
