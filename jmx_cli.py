#!/usr/bin/env python3

import subprocess
import json
import uuid
import logging
import os
import requests
from libs.nexus import Nexus


class JmxCli:
    """
    Class working with https://bitbucket.org/exante/jmx-client-cli/src/dev/
    """
    #url = 'http://ci2.ghcg.com/webstart/management-client/management-client.jar'
    path = 'jmx-cli.jar'
    nexus_project_name = 'client'
    nexus_project_path = 'ghcg-internal/eu/exante/management'

    def __init__(self, env='prod', monitor_host='monitor.{}.zorg.sh'):
        self.jmx = None
        self.env = env
        if self.env in ['demo', 'prod', 'cprod']:
            self.monitor_host = monitor_host.format('prod')
        else:
            self.monitor_host = monitor_host.format('test')

    def check_app(self):
        """
        check if app exists
        :return: is application exist
        """
        if not os.path.isfile(self.path):
            Nexus().get_maven_app(self.nexus_project_path, self.nexus_project_name, self.path)

        return os.path.isfile(self.path)

    def _start_jmx(self):
        self.check_app()
        self.jmx = subprocess.Popen(['java', '-jar', 'jmx-cli.jar', '--ui json'],
                                    bufsize=1, universal_newlines=True,
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info('Started PID {}'.format(self.jmx.pid))

    def __del__(self):
        if self.jmx:
            self.jmx.stdin.close()
            self.jmx.stdout.close()
            self.jmx.stderr.close()
            logging.info('Terminate PID {}'.format(self.jmx.pid))
            self.jmx.terminate()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    def close(self):
        self.__del__()

    def _execute(self, command):
        """
        command = {'id': '1',
                   'address': {'monitorHost': 'monitor.prod.zorg.sh',
                               'name': 'gw-broker-demo@gateways-demo'},
                   'objectName': {'domain': 'eu.exante.broker',
                                  'properties': {'type': 'BrokerGateway'}},
                   'command': 'invoke',
                   'operation': 'markCancelled',
                   'arguments': ['comment', 'b70f7efc-75b5-4715-8717-63aaa49ef6ca']}
        """
        if not self.jmx:
            self._start_jmx()
        command = json.dumps(command) + '\n'
        logging.info('\n\nExecuting\n' + command)
        self.jmx.stdin.write(command)
        json_output = json.loads(self.jmx.stdout.readline())
        self.jmx.stdout.flush()
        return json_output
    
    def get_master_node_module_name(self, module_name, env):
        sdb_modules = [module for module in requests.get('http://{}/modules/'.format(self.monitor_host)).json()
                       if module_name in module['name'] and module['properties']['environment'] == env]
        for module in sdb_modules:
            if 'flair' in module['properties'] and module['properties']['flair']['text'] == 'active':
                return module['name']

    def mark_cancelled(self, module, order, reason='Stuck order'):
        """
        marks order cancelled
        :param module: module name from monitor like 'gw-broker-demo@gateways-demo'
        :param order: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :param reason: provide reason of cancellation, default is 'Stuck order'
        :return: jmx-cli response dict, or error dict
        """
        if 'broker-automation-engine' in module:
            domain = 'eu.exante.broker.automator'
            properties_type = 'OrderManager'
        else:
            domain = 'eu.exante.broker'
            properties_type = 'BrokerGateway'
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'type': properties_type}},
                   'command': 'invoke',
                   'operation': 'markCancelled',
                   'arguments': [reason, order]}
        return self._execute(command)

    def disable_risk_for_account(self,module,account):

        command = {'id': str(uuid.uuid4()),
                    'address': {'monitorHost': self.monitor_host,
                                'name': module},
                    'objectName': {'domain': 'eu.exante.broker',
                                    'properties': {'type': 'RiskAgent'}},
                    'command': 'invoke',
                    'operation': 'disableRiskForAccount',
                    'arguments': [account]}
        return self._execute(command)
    
    def enable_risk_for_account(self,module,account):

        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker',
                                  'properties': {'type': 'RiskAgent'}},
                   'command': 'invoke',
                   'operation': 'enableRiskForAccount',
                   'arguments': [account]}

        return self._execute(command)

    def emulate_counterparty_cancel(self, orderId, module, reason='Stuck order'):

        if 'broker-automation-engine' in module:
            domain = 'eu.exante.broker.automator'
            properties_type = 'OrderManager'
        else:
            domain = 'eu.exante.broker'
            properties_type = 'BrokerGateway'
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'type': properties_type}},
                   'command': 'invoke',
                   'operation': 'emulateCounterpartyCancel',
                   'arguments': [reason, orderId]}
        return self._execute(command)

    def unsubscribe(self, instrument, module):
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.feed',
                                  'properties': {'type': 'Subscriptions'}},
                   'command': 'invoke',
                   'operation': 'unsubscribe',
                   'arguments': [instrument]}
        return self._execute(command)

    def unsubscribe_all(self, regexp, module):
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.feed',
                                  'properties': {'type': 'Subscriptions'}},
                   'command': 'invoke',
                   'operation': 'unsubscribeAll',
                   'arguments': [regexp]}
        return self._execute(command)

    def place_on_initial(self, order):
        """
        places order on initial route
        :param order: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :return: jmx-cli response dict, or error dict
        """
        module = self.get_master_node_module_name('broker-automation-engine', self.env)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker.automator',
                                  'properties': {'type': 'OrderManager'}},
                   'command': 'invoke',
                   'operation': 'placeOnInitialRoute',
                   'arguments': [order]}
        return self._execute(command)

    def place_on_next(self, order):
        """
        places order on next route
        :param order: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :return: jmx-cli response dict, or error dict
        """
        module = self.get_master_node_module_name('broker-automation-engine', self.env)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker.automator',
                                  'properties': {'type': 'OrderManager'}},
                   'command': 'invoke',
                   'operation': 'placeOnNextRoute',
                   'arguments': [order]}
        return self._execute(command)

    def place_on_route(self, order, route):
        """
        places order on chosen route
        :param order: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :param route: uuid of sdbrouteId like 'AAF6ABC8-ADE7-7092-71D4-7ED2C4CAAC61'
        :return: jmx-cli response dict, or error dict
        """
        module = self.get_master_node_module_name('broker-automation-engine', self.env)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker.automator',
                                  'properties': {'type': 'OrderManager'}},
                   'command': 'invoke',
                   'operation': 'placeOnRoute',
                   'arguments': [order, route]}
        return self._execute(command)

    def get_order_state(self, order):
        """
        gets order state
        :param order: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :return: jmx-cli response dict, or error dict
        """
        module = self.get_master_node_module_name('broker-automation-engine', self.env)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker.automator',
                                  'properties': {'type': 'OrderManager'}},
                   'command': 'invoke',
                   'operation': 'getOrderState',
                   'arguments': [order]}
        return self._execute(command)

    def force_status_request(self, module, order_id):
        """
        order status request
        :param module: module name from monitor like 'gw-feed-idc@namo'
        :param order_id: uuid of order like 'b70f7efc-75b5-4715-8717-63aaa49ef6ca'
        :return: jmx-cli response dict, or error dict
        """
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.broker',
                                  'properties': {'type': 'BrokerGateway'}},
                   'command': 'invoke',
                   'operation': 'forceStatusRequest',
                   'arguments': [order_id]}
        return self._execute(command)

    def _logger_name_provider(self, module, exchange=None):
        """
        provides logger name for command arguments
        :param module: module name from monitor like 'gw-feed-idc@namo'
        :param exchange: specify for crypto feed adapters
        :return: logger name or error dict
        """
        if 'gw-feed-idc' in module:
            return 'eu.exante.idc.client.messages'
        elif 'crypto' in module:
            return 'eu.exante.feed.adapter.btc.{}'.format(exchange)
        elif 'broker-automation-engine' in module:
            # logger_name = 'eu.exante.broker.automator.order.helpers.ActivationHandler'
            return {'ROOT'}
        else:
            return 'fix.msg'

    def get_log_level(self, module, exchange=None):
        """
        returns current level of logging
        :param module: module name from monitor like 'gw-feed-idc@namo'
        :param exchange: specify for crypto feed adapters
        :return: jmx-cli response dict or error dict
        """
        logger_name = self._logger_name_provider(module, exchange)
        if type(logger_name) != str:
            return logger_name
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'ch.qos.logback.classic',
                                  'properties': {'Name': 'default',
                                                 'Type': 'ch.qos.logback.classic.jmx.JMXConfigurator'}},
                   'command': 'invoke',
                   'operation': 'getLoggerLevel',
                   'arguments': [logger_name]}
        return self._execute(command)

    def set_log_level(self, module, level, exchange=None):
        """
        sets logging level
        :param module: module name from monitor like 'gw-feed-idc@namo'
        :param level: 'TRACE' or 'DEBUG'
        :param exchange: specify for crypto feed adapters
        :return: jmx-cli response dict or error dict
        """
        logger_name = self._logger_name_provider(module, exchange)
        if type(logger_name) != str:
            return logger_name
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'ch.qos.logback.classic',
                                  'properties': {'Name': 'default',
                                                 'Type': 'ch.qos.logback.classic.jmx.JMXConfigurator'}},
                   'command': 'invoke',
                   'operation': 'setLoggerLevel',
                   'arguments': [logger_name, level]}
        return self._execute(command)

    def is_qring(self, module):
        """
        checks whether ui-server uses Qring for historical MarketData
        :param module: module name from monitor like 'ui-server@ui-server11-prod'
        :return: jmx-cli response dict or error dict
        """
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.atp.backend',
                                  'properties': {'type': 'HistoricalMDService'}},
                   'command': 'invoke',
                   'operation': 'enabled'}
        return self._execute(command)

    def switch_historical_source(self, module, new_source):
        """
        switches historical source on module to QRing or TickDB
        :param module: module name from monitor like 'ui-server@ui-server11-prod'
        :param new_source: choose 'qring' or 'tickdb'
        :return: jmx-cli response dict or error dict
        """
        if new_source not in ['qring', 'tickdb']:
            return {"error": "bad source"}
        set_qring = True if 'qring' == new_source else False
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.atp.backend',
                                  'properties': {'type': 'HistoricalMDService'}},
                   'command': 'invoke',
                   'operation': 'setQringEnabled',
                   'arguments': [set_qring]}
        return self._execute(command)

    def _fix_name_provider(self, module, session):
        """
        provides fix-sessions' domain and name for objectName dict in command
        :param session: session name like 'ABC1234_TRADE'
        :param module: module name from monitor like 'broker-fix-bridge-uat-two@atp2-demo'
        :return: tuple with session name and domain
        """
        if module.startswith('feed'):
            if module.endswith('demo'):
                session = 'FIX.4.4_EXANTE_FEED_UAT->' + session
            else:
                session = 'FIX.4.4_EXANTE_FEED->' + session
            domain = 'eu.exante.feed-fix-bridge.session'
        else:
            domain = 'eu.exante.broker-fix-bridge.session'
        return domain, session

    def block(self, module, session, reason='Raising errors'):
        """
        blocks fix-session on module
        :param session: session name like 'ABC1234_TRADE'
        :param module: module name from monitor like 'broker-fix-bridge-uat-two@atp2-demo'
        :param reason: provide reason of blocking, default is 'Raising errors'
        :return: jmx-cli response dict or error dict
        """
        domain, session = self._fix_name_provider(module, session)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'name': session}},
                   'command': 'invoke',
                   'operation': 'block',
                   'arguments': [reason]}
        return self._execute(command)

    def unblock(self, module, session):
        """
        unblocks fix-session on module
        :param session: session name like 'ABC1234_TRADE'
        :param module: module name from monitor like 'broker-fix-bridge-uat-two@atp2-demo'
        :return: jmx-cli response dict or error dict
        """
        domain, session = self._fix_name_provider(module, session)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'name': session}},
                   'command': 'invoke',
                   'operation': 'unblock'}
        return self._execute(command)

    def reset_client_errors(self, module, session):
        """
        reset client errors on broker-fix-bridge module
        :param session: session name like 'ABC1234_TRADE'
        :param module: module name from monitor like 'broker-fix-bridge-uat-two@atp2-demo'
        :return: jmx-cli response dict or error dict
        """
        domain, session = self._fix_name_provider(module, session)
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'name': session}},
                   'command': 'invoke',
                   'operation': 'resetClientErrors'}
        return self._execute(command)

    def block_atp_user(self, module, user_id, set_key='atp5block', data_key=True):
        """
        block ATP user on UI-server
        :param module: module name from monitor like 'ui-server@ui-server11-prod'
        :param user_id: userID for block like 'user@domain.ltd'
        :param set_key: key for block like 'atp5block'
        :param data_key: value for key like 'true'
        :return: jmx-cli response dict or error dict
        """
        domain = 'eu.exante.atp.backend'
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': domain,
                                  'properties': {'type': 'ClientDataStorageService'}},
                   'command': 'invoke',
                   'operation': 'set',
                   'arguments': [user_id, set_key, str(data_key).lower()]}
        return self._execute(command)

    def zeus_reload_snapshot(self, module):
        command = {'id': str(uuid.uuid4()),
                   'address': {'monitorHost': self.monitor_host,
                               'name': module},
                   'objectName': {'domain': 'eu.exante.symboldb.impl',
                                  'properties': {'type': 'zeus'}},
                   'command': 'invoke',
                   'operation': 'reloadSnapshot'}
        return self._execute(command)