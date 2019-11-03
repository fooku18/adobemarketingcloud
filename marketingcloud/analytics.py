import typing
import json
from requests import Response

from . import jwt


class ResponseError(Exception):
    """Adobe Analytics failed API response error"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f'ErrorId: {self.message["errorId"]}\n' \
               f'ErrorCode:{self.message["errorCode"]}\n' \
               f'ErrorDescription:{self.message["errorDescription"]}'


class Analytics:
    """
    Adobe Analytics API implementation.
    """
    BASE_URL = 'https://analytics.adobe.io/api/{company_id}'

    def __init__(self, config: typing.Union[str, typing.TextIO]) -> None:
        self.session = jwt.JWTAuth(config)

    # Endpoint Block
    # Calculated Metrics
    def get_calculatedmetrics(self,
                              locale='en_US',
                              limit=10,
                              page=0,
                              sort_direction='ASC',
                              sort_property='id',
                              **kwargs):
        endpoint = '/calculatedmetrics'
        params = {
            'locale': locale,
            'limit': limit,
            'page': page,
            'sortDirection': sort_direction,
            'sortProperty': sort_property,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def create_calculatedmetrics(self,
                                 payload: typing.Union[str, dict],
                                 locale: str = 'en_US') -> Response:
        endpoint = '/calculatedmetrics'
        params = {
            'locale': locale
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('post', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_calculatedmetrics_functions(self, locale: str = 'en_US') -> Response:
        endpoint = '/calculatedmetrics/functions'
        params = {
            'locale': locale
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_calculatedmetrics_function(self, id: str, locale: str = 'en_US') -> Response:
        endpoint = f'/calculatedmetrics/functions/{id}'
        params = {
            'locale': locale
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def validate_calculatedmetrics(self,
                                   payload: typing.Union[str, dict],
                                   locale: str = 'en_US',
                                   migrating: bool = False) -> Response:
        endpoint = '/calculatedmetrics/validate'
        params = {
            'locale': locale,
            'migrating': migrating
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('post', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_calculatedmetric(self, id: str, locale: str = 'en_US'):
        endpoint = f'/calculatedmetrics/{id}'
        params = {
            'locale': locale
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def update_calculatedmetric(self,
                                id: str,
                                payload: typing.Union[str, dict],
                                locale: str = 'en_US'):
        endpoint = f'/calculatedmetrics/{id}'
        params = {
            'locale': locale
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('put', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def delete_calculatedmetric(self,
                                id: str,
                                locale: str = 'en_US'):
        endpoint = f'/calculatedmetrics/{id}'
        params = {
            'locale': locale
        }
        response = self.session.request('delete', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Collections
    def get_collection_suites(self, limit: int = 10, page: int = 0, **kwargs) -> Response:
        endpoint = '/collections/suites'
        params = {
            'limit': limit,
            'page': page,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_collection_suite(self, id: str) -> Response:
        endpoint = f'/collections/suites/{id}'
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}')
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Dateranges

    def get_dateranges(self,
                       locale: str = 'en_US',
                       limit: int = 10,
                       page: int = 0, **kwargs) -> Response:
        endpoint = '/dateranges'
        params = {
            'locale': locale,
            'limit': limit,
            'page': page,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_daterange(self, id: str, locale: str = 'en_US', **kwargs) -> Response:
        endpoint = f'/dateranges/{id}'
        params = {
            'locale': locale,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Dimensions

    def get_dimensions(self,
                       rsid: str,
                       locale: str = 'en_US',
                       classficable: bool = False,
                       **kwargs) -> Response:
        endpoint = '/dimensions'
        params = {
            'rsid': rsid,
            'locale': locale,
            'classificable': classficable,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_dimension(self,
                      id: str,
                      rsid: str,
                      locale: str = 'en_US',
                      **kwargs) -> Response:
        endpoint = f'/dimensions/{id}'
        params = {
            'rsid': rsid,
            'locale': locale,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Metrics
    def get_metrics(self,
                    rsid: str,
                    locale: str = 'en_US',
                    segmentable: bool = False,
                    **kwargs) -> Response:
        endpoint = '/metrics'
        params = {
            rsid: rsid,
            locale: locale,
            segmentable: segmentable,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_metric(self,
                   id: str,
                   rsid: str,
                   locale: str = 'en_US',
                   **kwargs):
        endpoint = f'/mertrics/{id}'
        params = {
            'rsid': rsid,
            'locale': locale,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Reports

    def reports(self, payload: typing.Union[str, dict]) -> Response:
        """Implementation of the /response endpoint
        Use the Adobe Analytics Reports creator of the workspace in order
        to get a valid payload for the request.
        TODO: link an explanation how to retrieve these inputs
        """
        endpoint = '/reports'
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('post', f'{self.BASE_URL}{endpoint}', json=payload)
        if not response:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Segments

    def get_segments(self,
                     locale='en_US',
                     filterByPublishedSegments='all',
                     limit=10,
                     page=0,
                     sort_direction='ASC',
                     sort_property='id',
                     **kwargs):
        endpoint = '/segments'
        params = {
            'locale': locale,
            'filterByPublishedSegments': filterByPublishedSegments,
            'limit': limit,
            'page': page,
            'sortDirection': sort_direction,
            'sortProperty': sort_property,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def create_segments(self,
                        payload: typing.Union[str, dict],
                        locale: str = 'en_US',
                        **kwargs) -> Response:
        endpoint = '/segments'
        params = {
            'locale': locale,
            **kwargs
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('post', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def validate_segment(self,
                         rsid: str,
                         payload: typing.Union[str, dict]) -> Response:
        endpoint = '/segments/validate'
        params = {
            'rsid': rsid
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('post', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def get_segment(self,
                    id: str,
                    locale: str = 'en_US',
                    **kwargs):
        endpoint = f'/segments/{id}'
        params = {
            'locale': locale,
            **kwargs
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def update_segment(self,
                       id: str,
                       payload: typing.Union[str, dict],
                       locale: str = 'en_US',
                       **kwargs) -> Response:
        endpoint = f'/segments/{id}'
        params = {
            'locale': locale,
            **kwargs
        }
        if isinstance(payload, str):
            payload = json.loads(payload)
        response = self.session.request('put', f'{self.BASE_URL}{endpoint}', params=params, json=payload)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def delete_segment(self,
                       id: str,
                       locale : str = 'en_US') -> Response:
        endpoint = f'/segments/{id}'
        params = {
            'locale': locale
        }
        response = self.session.request('delete', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    # Endpoint Block
    # Users

    def users(self, limit: int = 0, page: int = 0) -> Response:
        endpoint = '/users'
        params = {
            'limit': limit,
            'page': page
        }
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}', params=params)
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response

    def user_me(self) -> Response:
        endpoint = '/users/me'
        response = self.session.request('get', f'{self.BASE_URL}{endpoint}')
        if response.status_code != 200:
            raise ResponseError(response.json())
        return response