import logging
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed
)

class Ftx:
    url = 'https://ftx.com/api'

    def __init__(self):
        self.session = requests.Session()
        self.session.mount(self.url, requests.adapters.HTTPAdapter())

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")


    @retry(
        retry=retry_if_exception_type((
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout
        )),
        stop=stop_after_attempt(10),
        wait=wait_fixed(10)
    )
    def __request(self, method, handle: str, jdata: dict = None, **params):
        try:
            response = method(f"{self.url}/{handle}", params=params, json=jdata) # headers=headers)
            if response.ok:
                return response
            else:
                self.logger.error(
                    f"Error code {response.status_code} while requesting"
                    "\n"
                    f"{response.url}"
                    "\n"
                    f"{response.text}"
                )
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise e
                
    def get(self, handle, params=None):
        """
        wrapper method for requests.get
        :param handle: backoffice api handle
        :param params: additional parameters to pass with this request
        :return: json received from api
        """
        return self.__request(method=self.session.get, handle=handle, params=params)

    def search_future(
            self,
            ticker: str = None
        ):
        search = self.get('futures').json().get('result')
        futures = [x for x in search if x['type'] == 'future']
        if ticker:
            return [x for x in futures if x.get('underlying') == ticker]
        else:
            return futures

    def search_perpetual(
            self,
            ticker: str = None
        ):
        search = self.get('futures').json().get('result')
        perpetuals = [x for x in search if x['type'] == 'perpetual']
        if ticker:
            return [x for x in perpetuals if x.get('underlying') == ticker]
        else:
            return perpetuals



    def search_fx_spot(
            self,
            ticker: str = None
        ):
        pass