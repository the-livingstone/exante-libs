#!/usr/bin/env python3

'''
https://docs.atlassian.com/software/jira/docs/api/REST/8.7.1/
'''

import logging
import argparse
import json
import requests
from requests import exceptions
from retrying import retry


def conerror(exc):
    exception = [exceptions.ConnectionError, exceptions.Timeout, exceptions.ConnectTimeout, exceptions.ReadTimeout]
    #print(exc, type(exc))
    return any([isinstance(exc, x) for x in exception])


class Jira:

    default_cred_file = '/etc/support/auth/jira.json'

    def __init__(self, args=None, cred_file=None, login=None, password=None, default_project='IS'):
        """
        class init method
        :param cred_file: auth credentials json
        :param login: auth login
        :param password: auth pasword
        :param default_project: default jira project id to send issues to
        """
        self.default_project = default_project
        if cred_file is None:
            cred_file = self.default_cred_file
        if args:
            cred_file = getattr(args, 'jira_auth', self.default_cred_file)
            self.default_project = getattr(args, 'jira_proj', default_project)
        if login and password:
            self.auth = (login, password)
        elif cred_file:
            try:
                with open(cred_file, "r") as cf:
                    creds = json.load(cf)
                cf.close()
                self.auth = (creds['login'], creds['password'])
            except Exception as e:
                raise Exception(f'Error while loading credentials file {cred_file}: {e}')
        else:
            raise Exception(f'No jira credentials provided!')

        self.api = 'https://jira.exante.eu/rest/api/2'
        self.headers = {
            'content-type': 'application/json',
            'Accept': 'application/json'
        }
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.mount(self.api, requests.adapters.HTTPAdapter(max_retries=3))

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    @classmethod
    def add_args(cls, parser, jproject='IS'):
        """
        classmethod to add standard jira arguments to argparse.parser
        :param parser: parser to add arguments to
        :param jproject: default jira project to send issues to
        """
        parser.add_argument(
            '--jira-proj',
            help=f'Jira project to send issues to (like: TECH or MO). Default is {jproject}',
            metavar='project_id',
            default=jproject)
        parser.add_argument(
            '--jira-auth',
            metavar='filename',
            help=f'Jira credentials file. Default is {cls.default_cred_file}',
            default=cls.default_cred_file)

    @retry(wait_exponential_multiplier=5000, stop_max_attempt_number=10,
           retry_on_exception=conerror)
    def __request(self, method, handle, params=None, jdata=None, headers=None, files=None):
        """
        wrapper method for requests
        :param method: requests method to be invoked
        :param handle: jira API handle
        :param params: additional parameters to pass with this request
        :param jdata: json to pass with request
        :return: requests response object
        """
        if headers is None:
            headers = self.headers
        r = method(self.api + handle, params=params, json=jdata, headers=headers, files=files)
        self.logger.debug(f'headers: {r.headers}')
        self.logger.debug(f'full url: {r.url}')
        if files:
            self.logger.debug(f'files: {files}')
        if params:
            self.logger.debug(f'passed params: {params}')
        if jdata:
            self.logger.debug(f'passed json: {jdata}')
        if r.status_code > 209:
            raise Exception(f'Server returns error code {r.status_code} while requesting: {r.url} Response: {r.text}')
        if r.text:
            return r.json()
        else:
            return {'status_code': r.status_code}

    def get(self, handle, params=None):
        """
        wrapper method for requests.get
        :param handle: jira API handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return self.__request(method=self.session.get, handle=handle, params=params)

    def delete(self, handle, params=None):
        """
        wrapper method for requests.delete
        :param handle: jira API handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return self.__request(method=self.session.delete, handle=handle, params=params)

    def post(self, handle, jdata=None, params=None, headers=None, files=None):
        """
        wrapper method for requests.post
        :param params:
        :param handle: jira API handle
        :param jdata: json to pass with request
        :return: json received from api
        """
        return self.__request(
            method=self.session.post,
            handle=handle,
            jdata=jdata,
            params=params,
            headers=headers,
            files=files)

    def put(self, handle, jdata, params=None):
        """
        wrapper method for requests.put
        :param params:
        :param handle: jira API handle
        :param jdata: json to pass with request
        :return: json received from api
        """
        return self.__request(method=self.session.put, handle=handle, jdata=jdata, params=params)

    def get_project_issue_types(self, project_id):
        """
        will return all issue types for certain project_id
        :param project_id: jira project_id for the new issue (example: TSDEV)
        :return: json received from api
        """
        return self.get(f'/issue/createmeta/{project_id}/issuetypes')

    def create_issue(self, header, message, project_id=None, issue_type='Task', labels=None, attachments=None):
        """
        create issue method
        :param header: header (or summary) for the new issue
        :param message: message (or description) for the new issue
        :param project_id: jira project_id for the new issue (example: TSDEV, TECH)
        :param issue_type: type of the issue: Bug, Task, Ticket... etc...
            List of types is specific for each project (use get_project_issue_types())
        :param labels: list of labels (tags) for this issue
        :param attachments: list of attachments (could be a list of filenames or file objects)
        :return: json received from api
        """
        data = {
           'fields': {
               'project': {
                  'key': project_id if project_id else self.default_project
               },
               'summary': header,
               'description': message,
               'issuetype': {
                  'name': issue_type
               }
           }
        }
        if labels is not None:
            data['fields']['labels'] = labels

        res = self.post('/issue', data)

        if attachments is not None:
            res['attachments'] = self.add_attachments(res['key'], attachments)
        self.logger.info(f'Jira issue created\nData: {res}')
        return res

    def add_attachments(self, issue, attachments):
        """
        create issue method
        :param issue: jira issue id (like TSDEV-666)
        :param attachments: list of attachments (could be a list of filenames or file objects)
        :return: list of jsons received from api for each attachment
        """
        headers = {'X-Atlassian-Token': 'no-check'}
        res = []
        for attachment in attachments:
            try:
                if isinstance(attachment, str):
                    file_handle = {'file': open(attachment, 'rb')}
                else:
                    file_handle = attachment
                res += self.post(f'/issue/{issue}/attachments', headers=headers, files=file_handle)
            except Exception as e:
                res += [f'Exception for {attachment}: {e}']
        return res

    def get_issue(self, issue):
        """
        get issue method
        :param issue: jira issue id (like TSDEV-666)
        :return: issue json
        """
        return self.get(f'/issue/{issue}')

    def delete_issue(self, issue):
        """
        delete issue method
        :param issue: jira issue id (like TSDEV-666)
        :return: json with status_code
        """
        return self.delete(f'/issue/{issue}')

    def get_comment(self, issue, comment_id=None):
        """
        get issue comment method
        :param issue: jira issue id (like TSDEV-666)
        :param comment_id: numerical jira comment_id - will return all comments for issue if None
        :return: response json
        """
        endpoint = ''
        if comment_id is not None:
            endpoint = f'/{comment_id}'
        return self.get(f'/issue/{issue}/comment{endpoint}')

    def post_comment(self, issue, comment, comment_id=None):
        """
        post issue comment method
        :param issue: jira issue id (like TSDEV-666)
        :param comment: text of the comment to post
        :param comment_id: numerical jira comment_id - will add new comment for issue if None,
            or will update existing otherwise
        :return: response json
        """
        if comment_id is not None:
            return self.put(f'/issue/{issue}/comment/{comment_id}', {'body': comment})
        return self.post(f'/issue/{issue}/comment', {'body': comment})

    def del_comment(self, issue, comment_id):
        """
        delete issue comment method
        :param issue: jira issue id (like TSDEV-666)
        :param comment_id: numerical jira comment_id
        :return: json with status_code
        """
        return self.delete(f'/issue/{issue}/comment/{comment_id}')

    def search(self, query=str(), params=dict(), project: str() = 'default'):
        """
        search issues method
        :param query: JQL search query
        :param project: Jira project
        :return: list of jsons of found issues
        """
        if project:
            if project == 'default':
                project = self.default_project
            if query:
                jql_query = f'project = "{project}" AND ' + query
            else:
                jql_query = f'project = "{project}"'
            payload = {'jql': jql_query}
        else:
            payload = {'jql': query}
        payload.update(params)
        return self.post(f'/search', payload)

    def update_issue(self, issue, payload):
        """
        update issue method
        :param issue: Jira issue id
        :param payload: dict of what to change
        :return: status of update
        """
        return self.put(f'/issue/{issue}', payload)

    def compile_update_payload(self, part, value, action='set', payload=dict()):
        """
        method to prepare data for update
        :param part: part to update, e.g. summary, status, labels, etc.
        :param value: part new value
        :param action: what to do, available opts: set, add, edit, remove
        :param payload: payload to be updated, empty for first part
        :return: updated payload
        """
        if action != 'set':
            if not payload.get('update'):
                payload.update({'update': dict()})
            if not payload['update'].get(part):
                payload['update'].update({part: list()})
            payload['update'][part].append({
                action: value
            })
        else:
            if not payload.get('fields'):
                payload.update({'fields': dict()})
            payload['fields'].update({
                part: value
            })
        return payload

    def get_transitions(self, issue):
        """
        method to get possible states for issue
        :param issue: Jira issue id
        :return: possible states description
        """

        return self.get(f'/issue/{issue}/transitions')

    def do_transition(self, issue, new_status, payload=dict()):
        """
        method to change issue status
        :param issue: Jira issue id
        :param new_status: new status id
        :param payload: any changes to do during transition
        :return: issue json
        """
        payload.update({
            'transition': {
                'id': new_status
            }
        })
        return self.post(f'/issue/{issue}/transitions', payload)