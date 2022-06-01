#!/usr/bin/env python3
# ex: ts=4 sw=4 et

import logging
import requests
from requests import exceptions
from urllib.request import quote as urlenc
from retrying import retry
from os import path
import json
from json import JSONDecodeError


def conerror(exc):
    exception = [exceptions.ConnectionError, exceptions.Timeout, exceptions.ConnectTimeout, exceptions.ReadTimeout]
    print(exc, type(exc))
    return any([isinstance(exc, x) for x in exception])


class NotAuthorized(Exception):
    pass


def keyload(cred, env, service):
    try:
        with open(cred) as file:
            loaded = json.load(file)
            logging.debug('Credentials file loaded')
            return loaded[env][service]
    except (IOError, JSONDecodeError) as e:
        logging.error('Credentials file {} cannot be read: {}'.format(cred, e))
    except KeyError:
        logging.error('Credentials file {} has no data for {} in {} env'.format(cred, service, env))
    return None


def get_session(user, password, service, env='prod', ttl=1800000, version='1.0'):
    """
    base method to authenticate
    :param user: username
    :param password: password
    :param service: allowed services
    :param env: environment
    :param ttl: ttl in ms or infinity
    :param version: version, current API is 1.0
    :return: sessionId
    """
    try:
        ttl = int(ttl)
    except ValueError:
        ttl = ttl
    jdata = {"username": str(user),
             "password": str(password),
             "service": service,
             "ttl": ttl}
    head = {'Accept': 'application/json',
            'Content-Type': 'application/json'}
    if env == 'prod':
        url = 'https://authdb.exante.eu/api/{}/auth/session'.format(version)
    else:
        url = 'https://authdb-{}.exante.eu/api/{}/auth/session'.format(env, version)
    logging.debug(jdata)
    resp = requests.post(url, json=jdata, headers=head)
    if resp.ok:
        return resp.json()['sessionid']
    else:
        raise NotAuthorized('%s: %s' % (resp.status_code, resp.text))


class AuthDB:
    """Class to work with authdb"""

    url = None

    def __init__(self, env='prod', version='1.0', user=None, password=None,
                 credentials=('%s/credentials.json' % path.expanduser('~'))):
        """
        class init
        :param env: environment
        """

        self.version = version
        self.sessionId = None
        self.env = env
        if self.env == 'prod':
            self.url = 'https://authdb.exante.eu/api/{}/auth'.format(self.version)
        elif self.env == 'cprod':
            self.url = 'https://authdb.gozo.pro/api/{}/auth'.format(self.version)
        else:
            self.url = 'https://authdb-{}.exante.eu/api/{}/auth'.format(self.env, self.version)
        if credentials:
            try:
                self.sessionId = keyload(credentials, self.env, 'backoffice')
            except KeyError:
                self.logger.debug('Provided file has no backoffice session for {}'.format(self.env))
        elif self.sessionId is None:
            if user and password:
                self.sessionId = get_session(user, password, 'backoffice', self.env)
        else:
            raise NotAuthorized('Either credentials file or user-pass must be provided')
        self.headers = {'Content-Type': 'application/json',
                        'X-Auth-SessionId': self.sessionId}
        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())
        self.services = ["api", "atp", "backoffice", "site", "symboldb", "auditlog", "symboldb_editor_backend"]

    def __repr__(self):
        return 'AuthDB({})'.format(repr(self.env))

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def __request(self, method, handle, params=None, data=None, head=None):
        """
        wrapper method for requests
        :param method: requests method to be invoked
        :param handle: authdb api handle
        :param params: additional parameters to pass with this request
        :param data: json to pass with request
        :return: requests response object
        """
        resp = method(self.url + handle, params=params, headers=head, json=data)
        self.logger.debug('full url: {}'.format(resp.url))
        if params:
            self.logger.debug('passed params: {}'.format(params))
        if data:
            self.logger.debug('passed json: {}'.format(data))
        try:
            return resp.json()
        except (ValueError, json.JSONDecodeError):
            return resp

    def _get(self, link, params=None):
        """
        custom requests.get method
        :param link: request link
        :param params: additional params to be passed
        :return: json of the response
        """
        return self.__request(method=self.session.get, handle=link, params=params, head=self.headers)

    def _post(self, link, data, params=None):
        """
        custom requests.post method
        :param link: request link
        :param data: request data
        :param params: additional params to be passed
        :return: json of the post request or response if could not parse to json
        """
        return self.__request(method=self.session.post, handle=link, params=params, data=data, head=self.headers)

    def _put(self, link, data, params=None):
        """
        custom requests.put method
        :param link: request link
        :param data: request data
        :param params: additional params to be passed
        :return: json of the post request or response if could not parse to json
        """
        return self.__request(method=self.session.put, handle=link, params=params, data=data, head=self.headers)

    def _delete(self, link, params=None):
        """
        custom requests.delete method
        :param link: link to be deleted
        :param params: additional params to be passed
        :return: response object
        """
        return self.__request(method=self.session.delete, handle=link, params=params, head=self.headers)

    def userlist(self, archived=False, usertype=None, services=None, fields: list=None):
        """
        method to get users list
        :param archived: show archived users, default False
        :param usertype: userType from ['exante','trader','attorney']
        :param services: single or list of services user should have access to
        :return: user list
        """
        if type(services) == list:
            services = ','.join(map(str, services))
        params = {'showArchived': archived,
                  'userType': usertype,
                  'hasAccessTo': services}
        if fields:
            params.update({'fields': ','.join(fields)})
        return self._get('/users', params=params)

    def userinfo(self, user):
        """
        method to get user information
        :param user: user name
        :return: user data
        """
        return self._get('/user/{}'.format(urlenc(user, safe='')))

    # rename this to proper {}_{} methods
    def create(self, user, password, usertype='trader'):
        """
        method which creates a new user
        :param user: user name
        :param password: user password
        :param usertype: type of user
        :return: self._post()
        """
        jdata = {'username': user,
                 'password': password,
                 'userType': usertype}
        return self._post('/user', data=jdata)

    def user_update(self, user, jdata):
        """
        method simply update userinfo
        :param user: username
        :param jdata: json with data to update
        :return: self._put()
        """
        return self._put('/user/{}'.format(user), data=jdata)

    def access(self, user, service='atp'):
        """
        method which tests access to user
        :param user: user name
        :param service: service to be tested
        :return: self.service()
        """
        if service not in self.services:
            raise ValueError
        tokens = self.userinfo(user)['tokens']
        maintoken = next(t for t in tokens if t['name'] == 'Main password')
        maintokenid = maintoken['id']
        self.logger.info('Main token is {}'.format(maintoken))
        return self.service(user, service, maintokenid)

    def service(self, user, service, tokenid):
        """
        method to login user
        :param user: user name
        :param service: service name
        :param tokenid: token ID
        :return: self.__post()
        """
        if service not in self.services:
            raise ValueError
        jdata = {'maintokenid': tokenid,
                 'tokens': [{
                     'tokenid': None,
                     'roles': ['Default']
                 }]}
        return self._post('/user/{}/service/{}'.format(urlenc(user), service), data=jdata)

    def service_del(self, user, service):
        """
        delete service from user
        :param user: username
        :param service: service name
        :return:
        """
        return self._delete('/user/{}/service/{}'.format(urlenc(user), service))

    def user_archive(self, user):
        """
        archives users
        :param user: user to archive
        :return: updated data
        """
        jdata = {"archived": True}
        return self._put('/user/{}'.format(urlenc(user)), data=jdata)

    def user_dearchive(self, user):
        """
        dearchives users
        :param user: user to dearchive
        :return: updated data
        """
        jdata = {"archived": False}
        return self._put('/user/{}'.format(urlenc(user)), data=jdata)

    def user_permissions_get(self, user):
        """
        return a list of users permissions
        :param user: user name
        :return: list of permissions
        """
        return self._get('/user/{}/permissions'.format(urlenc(user)))

    def user_permissions_post(self, user, operation, read_perm=False, write_perm=False):
        """
        post global metrics permissions
        :param user: user name
        :param operation: operation or json-data to be updated
        :param read_perm: read permissions
        :param write_perm: write permissions
        """
        if (type(operation) == dict) or (type(operation) == list):
            jdata = operation
        else:
            jdata = [{
                'operation': operation,
                'actions': {
                    'read': read_perm,
                    'write': write_perm
                }
            }]
        return self._put('/user/{}/permissions'.format(urlenc(user)), data=jdata)

    def token_update(self, user, value, token=None):
        """
        method to update token value (usually password)
        :param user: username
        :param token: tokenId to be updated, if not specified update main password
        :param value: new value
        :return: self._post()
        """
        jdata = {"value": str(value)}
        if not token:
            token = self.userinfo(user)['tokens'][0]['id']
        return self._put('/user/{}/token/{}'.format(user, token), data=jdata)

    def token_del(self, user, token):
        """
        method to delete token value
        :param user: username
        :param token: tokenId to be deleted
        :return: self._post()
        """
        return self._delete('/user/{}/token/{}'.format(user, token))

    def token_post(self, user, token_name, token_type='password', token_value='dummy123'):
        """
        method to add a new token
        :param name: token name
        :param type: default is password
        :param value: the token value
        :return: self._post()
        """
        data = {
                'name': token_name,
                'type': token_type,
                'value': token_value
        }

        return self._post('/user/{}/token'.format(user), data=data)

    def del2fa(self, user):
        """
        method deletes token from service site and token itself
        :param user: username
        :return:
        """
        userinfo = self.userinfo(user)
        tokenmain = None
        token2fa = None
        for x in userinfo['auths']:
            if x['service'] == 'site':
                token2fa = x['tokens'][0]['tokenid']
                tokenmain = x['maintokenid']
        if tokenmain is not None and token2fa is not None:
            self.service(user, 'site', tokenmain)
            self.token_del(user, token2fa)
        else:
            print('User {} has no TOTP token on service site'.format(user))
    def create_session(self, username, password, service, ttl = 31536000000):
        return self._post('/session',{
            'username':username,
            'password':password,
            'ttl':ttl,
            'service':service
        })