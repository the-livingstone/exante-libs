#!/usr/bin/env python3

import datetime
import logging
import re
import os

import requests
from requests import exceptions
from retrying import retry


def conerror(exc):
    exception = [exceptions.ConnectionError, exceptions.Timeout, exceptions.ConnectTimeout, exceptions.ReadTimeout]
    print(exc, type(exc))
    return any([isinstance(exc, x) for x in exception])


class Monitor:
    """Class working with Monitor"""

    url = None

    def __init__(self, env='prod', user=None, password=None):
        """
        class init method
        Authentication required to post requests.
        :param user: username to LDAP auth
        :param password: password to LDAP auth.
        :param env: environment
        """
        self.env = env
        self.user = user
        self.password = password
        self.headers = {'content-type': 'application/json', 'x-forwarded-user': 'support-libs/monitor.py'}
        self.url = 'http://monitor.{}.zorg.sh'.format(env)
        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())

    def __repr__(self):
        return 'Monitor({})'.format(repr(self.env))

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    def exretry(x):
        if(isinstance(x, requests.exceptions.Timeout)
        or isinstance(x, requests.exceptions.HTTPError)
        or isinstance(x, requests.exceptions.ConnectionError)):
            return True
        return False

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=exretry)
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
            self.logger.debug('passed params: {}'.format(params))
        if data:
            self.logger.debug('passed json: {}'.format(data))
        self.logger.debug('full url: {}'.format(self.url + handle))
        r = method(self.url + handle, params=params, headers=head, json=data, auth=(self.user, self.password))
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
            self.logger.error(f'Can not parse data from: {self.url}{link} - {resp}')
            return resp

    def _post(self, link, jdata=None, params=None):
        """
        custom requests.post method
        :param link: request link
        :param jdata: json data to post
        :param params: additional params to be passed
        :return: json of the response
        """
        resp = self.__request(method=self.session.post, handle=link, params=params, head=self.headers,
                              data=jdata)
        try:
            return resp.json()
        except ValueError:
            return resp

    def _put(self, link, jdata=None, params=None):
        """
        custom requests.post method
        :param link: request link
        :param jdata: json data to post
        :param params: additional params to be passed
        :return: json of the response
        """
        resp = self.__request(method=self.session.post, handle=link, params=params, head=self.headers,
                              data=jdata)
        try:
            return resp.json()
        except ValueError:
            return resp

    def _delete(self, link, params=None):
        """
        custom requests.post method
        :param link: request link
        :param params: additional params to be passed
        :return: json of the response
        """
        resp = self.__request(method=self.session.post, handle=link, params=params, head=self.headers)
        try:
            return resp.json()
        except ValueError:
            return resp

    @staticmethod
    def get_time():
        """
        method to get current time
        :return: current time in XML format
        """
        return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # modules
    def all_modules(self):
        """
        method to get all modules
        :return: all modules json
        """
        return self._get('/modules/')

    def indicator_history(self, module, path):
        """
        method to get indicator history
        :param module: module anme
        :param path: indicator path
        :return: indicator history json
        """
        return self._get('/modules/{}/indicators/{}/history'.format(module, '|'.join(path)))

    def indicator_status(self, module, path):
        """
        method to get current indicator status
        :param module: module anme
        :param path: indicator path
        :return: current indicator status
        """
        return self._get('/modules/{}/indicators/{}'.format(module, '|'.join(path)))

    def module_info(self, module, fold=True):
        """
        method to get current module information
        :param fold: grouping of indicators by path (may truncate info if too much indicators on same path)
        """
        return self._get(f"/modules/{module}{'?fold=false' if fold == False else ''}")

    def remove_module(self, module):
        """
        method to remove module for monitor
        :param module: module name
        :return: response of delete request
        """
        return self._delete('/modules/{}'.format(module))

    def update_indicator(self, module, path, indicator_type, description='',
                         status='OK', statusName='OK', statusType='OK', time=None):
        """
        the method creates new indicator. For statuses refer to
        https://bitbucket.org/exante/docs/wiki/Indicators
        :param description:
        :param module: module name
        :param path: indicator path
        :param indicator_type: type of an indicator
        :param status: indicator status
        :param statusName; indicator status name
        :param statusType: indicator status type
        :param time: indicator time in XML format
        :return: response of put or post request
        """
        if not time:
            self.logger.debug('Grab current time')
            time = self.get_time()
        if not type(path) is list:
            self.logger.debug('Path is not a list, converting to')
            path = list(path)
        data = {
            'indicators': [{
                'path': path,
                'state': {
                    'description': description,
                    'status': status,
                    'statusName': statusName,
                    'statusType': statusType,
                    'timestamp': time,
                },
                'type': indicator_type
            }]
        }
        # we need to put indicator if module doesn't exist
        if 'error' in self.module_info(module):
            response = self._put('/modules/{}'.format(module), jdata=data)
        else:
            response = self._post('/modules/{}'.format(module), jdata=data)
        return response


    def acknowledge_indicator(self, module, path, acknowledge=True, allInstances=False, description='', until=None, user='OS'):
        """
        the method creates new indicator. For statuses refer to
        https://bitbucket.org/exante/docs/wiki/Indicators
        :param module: module name
        :param path: indicator path
        :param allInstances: acknowledge indicator on all instances where present
        :param description: acknowledgement text
        :param user: user whose behalf acknowledgement marks. By default get system login
        :param until: time until indicator will be acknowledged
        :return: response of put or post request, or 'error' if cant get module_info(module)
        """
        # detect user for acknowledge
        if user == 'OS':
            try:
                self.user = os.getlogin()
            except Exception:
                self.user = 'TS.libs.monitor'
        if not type(path) is list:
            self.logger.debug('Path is not a list, converting to')
            path = list(path)
        data = {
                'path': path,
                'acknowledged': acknowledge,
                'allInstances': allInstances,
                'description': description,
                'until': until                
            }
        # return error if module not exists
        if 'error' in self.module_info(module):
            return 'error'
        else:
            response = self._post('/modules/{}/acknowledge'.format(module), jdata=data)
        return response

    # messages
    def all_messages(self):
        """
        method to get all incoming messages
        :return: self.get()
        """
        return self._get('/messages')

    def message(self, pattern='.*'):
        """
        method to find all message which matchs to the pattern
        :param pattern: pattern to filter message
        :return: all found messages
        """
        found_messages = []
        messages = self.all_messages()
        for message in messages['page']:
            if not re.match(pattern, message['subject']):
                continue
            self.logger.info('Found message matched to the pattern {}'.format(message))
            found_messages.append(message)
        return found_messages

    # filters
    def filt(self, pattern='.*'):
        """
        filtering function which will return only required type of modules
        :param pattern: pattern to filter message
        :return: list of all found modules
        """
        return [x for x in self.all_modules() if re.match(pattern, x['name'])]

    def active_gateways(self):
        """
        custom filter method
        :return: list of active gateways
        """
        return self.filt('^gw-(feed|broker).*')

    def active_bridges(self):
        """
        custom filter method
        :return: list of active bridges
        """
        return self.filt('^(feed|broker)-fix-bridge.*')

    def active_uiservers(self):
        """
        custom filter method
        :return: list of active ui servers
        """
        return self.filt('^ui-server')

    def fix_sessions(self):
        """
        function returns a dictionary with all current fix bridges and
        clients with statuses on them
        :return: list of fix sessions
        """
        bridges = [self.module_info(x['name']) for x in self.active_bridges()]
        response = []
        for bridge in bridges:
            self.logger.info('Found bridge {}'.format(bridge))
            sessions = [x for x in bridge['indicators'] if x['path'][0] == 'sessions']
            statuses = {
                session['path'][1]: session['state']['statusType'] for session in sessions
            }
            self.logger.info('Found statuses {}'.format(statuses))
            response.append({
                bridge['name']: statuses
            })
        return response

    # incidents
    def incidents(self):
        """
        method to get all open incidents
        :return: list of incidents
        """
        return self._get('/incidents/open')

    def change_incident_state(self, _id, state='open'):
        """
        method to change incident state
        :param _id: incident ID
        :param state: incident state, default is open
        :return: response of post request
        """
        if state not in ('open', 'closed', 'acknowledged'):
            self.logger.warning('State {} is not available'.format(state))
        data = {
            'state': state
        }
        return self._post('/incidents/{}'.format(_id), jdata=data)

    def change_incidents_state(self, pattern='.*', module='.*', state='open'):
        """
        method to change state of incidents which match to the pattern
        :param pattern: incident name regexp
        :param module: module name regexp
        :param state: incident state
        :return: response of post request
        """
        incidents = self.filt_incidents(pattern, module)
        for incident in incidents:
            self.change_incident_state(incident['id'], state)

    def create_incident(self, incident, key, module, subject, message='', time=None):
        """
        method to create an incident
        :param incident: incident name
        :param key: incident key
        :param module: module name to which inc should be created
        :param subject: incident subject
        :param message: incident details
        :param time: incident time in XML format, default is a current time
        :return: post response object
        """
        if not time:
            self.logger.debug('Grab current time')
            time = self.get_time()
        data = {
            'type': incident,
            'key': key,
            'module': module,
            'subject': subject,
            'message': message,
            'timestamp': time
        }
        return self._post('/incidents', jdata=data)

    def filt_incidents(self, pattern='.*', module='.*'):
        """
        filtering function which will return only required type of incidents
        :param pattern: incident subject regexp
        :param module: incident module regexp
        :return: list of found incidents
        """
        filtered = []
        all_incidents = self.incidents()
        for incident in all_incidents:
            if not re.match(module, incident['module']):
                continue
            if not re.match(pattern, incident['subject']):
                continue
            self.logger.info('Found incident matched to the pattern {}'.format(incident))
            filtered.append(incident)
        return filtered

    def get_stuck_orders(self, name):
        """
        get stuck orders from monitor
        """
        info = self.module_info(name)
        errors = [i for i in info['indicators'] if i['type'] == 'broker.stuck-order']
        orders = []
        for error in errors:
            descr = error['state']['description'].split(' ')
            prev = 'none'
            for comp in descr:
                if prev == '-':
                    orders.append(comp)
                prev = comp
        return orders

    def get_master_node_hostname(self, moduleName, moduleEnv='prod'):#gets master node from monitor
        for i in range(0,3):
            try:
                needed_modules=[module for module in self.all_modules() if moduleName in module['name'] and  module['properties']['environment']==moduleEnv] #takes all symboldb modules
                for module in needed_modules: #iterate through them and find master
                    if 'flair' in module['properties'] and module['properties']['flair']['text']=='master' and 'fullHostname' in module['properties']:
                        return module['properties']['fullHostname']# return hostname
            except:
                self.logger.info('Can\'t get valid answer from monitoring, retrying...')
        raise Exception('Can\'t get valid answer from monitoring! Giving up.')
