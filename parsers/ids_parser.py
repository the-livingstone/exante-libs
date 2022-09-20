from urllib import response
import requests
import json


class ICE_DATA_SERVESE:
    """ Class for using ICE Data Service API
    """

    DEFAULT_CRED_FILE = '/etc/support/auth/ids.json'

    url = {
        'base': 'https://api.icedataservices.com/',
        'auth': 'login'
    }


    def __init__(self, cred_file: dict = DEFAULT_CRED_FILE) -> None:
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        credentials = json.load(open(cred_file))
        self.__add_token(credentials=credentials)
        pass

    def __add_token(self, credentials: dict) -> str:
        """ Getting token from ICE DATA Service by credantionals

        Args:
            credentionals (dict): user password credantionals

        Returns:
            str: auth token
        """
        cred_string = json.dumps(credentials)
        response = self.__post(api='auth', payload=cred_string)
        token = response['token']
        self.headers.update({'Authorization': f'Bearer {token}'})

    def __post(self, api: str, payload: str):
        url = self.url + self.url[api]
        response = requests.post(url=url, data=payload, headers=self.headers)
        return response.json

