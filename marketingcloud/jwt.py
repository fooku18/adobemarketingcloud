import typing
import datetime
import json
import jwt
import re
import requests
from requests.auth import AuthBase
from requests import Response


class BearerAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, req, *args, **kwargs):
        req.headers['Authentication'] = f'Bearer {self.token}'


class ConfigInsufficientInformationError(Exception):
    """Config file has insufficient information and an authentication would fail"""
    def __init__(self, *args, **kwargs):
        pass


class AuthenticationError(Exception):
    """Error when jwt authentication request fails"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f'{self.message["error"]}\n{self.message["error_description"]}'


class InvalidMethodInvocation(Exception):
    """Raised if invalid requests method is tried to be invoked"""
    pass


class JWTAuth:
    """Implementation of the JWT Service Account Flow

    Arguments:
        config(Union[str, typing.TextIO])     : Path of file descriptor to the config file.
                                                A full description on how to create this file is available
                                                below. The config file needs to be in a valid json
                                                format. The constructor will throw an error otherwise.
        -------------------------------------------------------------
        The config file will need to be a valid json file with the following structure:
        {
            ### The following values can be extracted from the service account integration file
            "iss": "XXXXXX@AdobeOrg",
            "sub": "XXXXX@techacct.adobe.com",
            "aud": "https://ims-na1.adobelogin.com/c/XXXXXX"
            ### Adobe IO Scopes are keys that enable the account to access particular elements
            ### for the Adobe APIs. Make sure to specify all in this json. The below is an example
            ### for the Adobe Analytics scope.
            "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": true.
            ### Please fill the following infos
            "privateKeyPath": "/path/on/your/system",
            "clientSecret": "XXXXXXX",
            "companyId": "XXXXX"
        }

        A Public-Private Key must be created on your own.
        See how to create a key here:
        https://www.adobe.io/authentication/auth-methods.html#!AdobeDocs/adobeio-auth/master/AuthenticationOverview/ServiceAccountIntegration.md
        -------------------------------------------------------------

        endpoint(str, optional): JWT exchange endpoint, defaults to EXCHANGE_ENDPOINT
    """
    EXCHANGE_ENDPOINT = "https://ims-na1.adobelogin.com/ims/exchange/jwt"
    REQUIRED_FIELDS = 'iss sub aud privateKeyPath clientSecret companyId'.split(' ')

    def __init__(self,
                 config: typing.Union[str, typing.TextIO],
                 endpoint: str = EXCHANGE_ENDPOINT) -> None:
        self.session: requests.Session = None
        self.metascopes = {}
        self.endpoint = endpoint
        self.config = self._get_info(config)
        if not all([k in self.config for k in self.REQUIRED_FIELDS]):
            raise ConfigInsufficientInformationError(
                " ".join([k for k in self.REQUIRED_FIELDS if k not in self.config])
            )
        try:
            self.config['clientId'] = re.search(r'[^\/]+$', self.config['aud']).group()
        except Exception:
            raise TypeError('aud is wrongly formatted')
        for key in self.config.keys():
            if re.match("^https:", key):
                self.metascopes[key] = True

    def _get_info(self, data: typing.Union[str, typing.TextIO]) -> dict:
        """This method either parses a string to dict or reads a file an does the same"""
        dict_data = {}
        try:
            dict_data = data.read()
        except AttributeError:
            with open(data, 'r', encoding="utf8") as fd:
                dict_data = fd.read()
        finally:
            return json.loads(dict_data)

    def _generate_jwt(self) -> str:
        """Building a json web token from the data provided in config"""
        with open(self.config['privateKeyPath'], 'r') as fd:
            pk = fd.read()
        token = jwt.encode({
            "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=10),
            "iss": self.config['iss'],
            "sub": self.config['sub'],
            "aud": self.config['aud'],
            **self.metascopes
        }, pk, algorithm='RS256')
        return token

    def get_token(self) -> dict:
        """
        If successful this method returns an access token for further requests
        with the API. This token needs to be embedded in the HTTP header of the
        API method calls.

        Returns:
            access token(dict):
        """
        jwt_token = self._generate_jwt()
        payload = {
            "client_id": self.config['clientId'],
            "client_secret": self.config['clientSecret'],
            "jwt_token": jwt_token
        }
        result = requests.post(self.endpoint, data=payload)
        if result.status_code != 200:
            raise AuthenticationError(result.json())
        else:
            response = result.json()
        return response

    def _http_header(self, token: str) -> dict:
        """Returns a authenticated http header for API requests with Adobe"""
        return {
            "x-api-key": self.config['clientId'],
            "x-proxy-global-company-id": self.config['companyId'],
            "Authorization": f'Bearer {token}',
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def request(self, method: str, url: str, *args, **kwargs) -> Response:
        """
        This method serves as a proxy request for the final API method call. If the access_token
        expires, this method will automatically refresh the token and execute the API request
        for the original caller. Both positional parameters ``method`` and ``url`` are required as
        ``method`` will retrieve the method call from the underlying requests session and ``url``
        is a required parameter for all requests calls.
        *args and **kwargs will be passed to the underlying requests method

        Arguments:
            method(str): HTTP method for the API Call. Method will retrieve the underlying
                         requests library method for this query
            url(str)   : Required url parameter for requests http requests

        Returns:
            (Response) Response from the underlying requests method call
        """
        if not getattr(self, 'token_expiration', None) or \
                getattr(self, 'token_expiration') < datetime.datetime.utcnow():
            refresh_token = self.get_token()
            setattr(self, 'token_expiration', datetime.datetime.utcnow() +
                    datetime.timedelta(milliseconds=refresh_token['expires_in']))
            self.session = requests.Session()
            self.session.headers.update(self._http_header(refresh_token['access_token']))
        func = getattr(self.session, method, None)
        if not func:
            raise InvalidMethodInvocation()
        return func(url.format(company_id=self.config['companyId']), *args, **kwargs)
