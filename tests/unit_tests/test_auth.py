import os
import pytest
import requests
import datetime
from unittest.mock import mock_open, patch

from marketingcloud.jwt import JWTAuth, AuthenticationError, InvalidMethodInvocation


@pytest.fixture
def auth_client():
    fake_credentials = """{
            "iss": "XYZ@AdobeOrg",
            "sub": "XYZ@techacct.adobe.com",
            "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": true,
            "aud": "https://ims-na1.adobelogin.com/c/XYZ",
            "privateKeyPath": "/path/to/private/key",
            "clientSecret": "XYZ",
            "companyId": "XYZ",
            "clientId": "XYZ"
        }"""
    with patch('marketingcloud.jwt.open', mock_open(read_data=fake_credentials)):
        return JWTAuth("")


@pytest.fixture
def successful_token_response():
    class Response:
        status_code = 200

        def json(self):
            return {
                'token_type': 'bearer',
                'access_token': 'test_token',
                'expires_in': 86399994
            }

        def __call__(self, *args, **kwargs):
            return self

    return Response()


@pytest.fixture
def unsuccessful_token_response():
    class Response:
        status_code = 400

        def json(self):
            return {
                'error': 'error',
                'error_description': 'something went wrong'
            }

        def __call__(self, *args, **kwargs):
            return self

    return Response()


@pytest.fixture
def fake_private_key():
    return "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEowIBAAKCAQEAhkXLalByIpQrkkIHdF1kNiAgqG1+k9Dx3O446HfBSWApzxfV\n\
XIVR5uCZ8kaa1UFmj+HFqasn3XKGbz52BduzsgMdQmUoidflM6YwBGP/s0QquJ2x\n\
5/gn6seY8JHU/n9UfD9SGfADkS+CwbA6xZEM4o1ZGrmyz2Ru+9OSZSjjwpHd5Jnl\n\
ppPHLuWD6jNlxMpu+h0gBoVOQ16VT6yCQE/HAvC5u+QmLTV2ebHWQwdUhOCbuyGh\n\
UEIqpoUBNWzcnIEdIop2WXOiabUAF8Uh6GOL2WEvKAV+nV7fk910Q1JX4tWQDE3P\n\
1F04qSSc7soHyI1Sv/U8C7iJeDuTMnoCBJllFwIDAQABAoIBAApExC25QUFLu5lP\n\
22oWylcpVdYLqaZ8UELpJQkCP5Hw/MGNvQ96Uq0peByDMcwlWEagqZE0ObRB0e4o\n\
BLal+rQecNpnChagoDK2/u0XCLMY/3tm8/gdjk/yO8wKGxPrgPaPkSPSqzMrQwC1\n\
DYmWcjnRPYNBuF8L+0DfCU8bNW/nwt0qxLAstIxuvveShS8ISaKU2EjoAenWCOhA\n\
pCITKhkicXTQTSVoskiiM7qTFxm3S4KAg7HoZePROzJoCDT70sy8mJnqT3JQx+5F\n\
1uLmXnm0498YMqDVF1pc5tSF4nskGvuQsLrV6onG6jbhDb79ehtYRqaqhmhw/hib\n\
I+tUR8ECgYEA5lhyrVuKDSB9xMkKEe5m3G2qDJKuXc5eq1jrXJTwoWX0vtf7XksQ\n\
NIpjVkpzIND6fL01EGlK9+jxt7B/7dYPs1hrpnhv4aoErajVYKOwtTflwwYdhj8+\n\
SguegZAv6N+UE11y8/QehlFH0ldyO0HBWCCrMf8IrsDTXfWpyj2jQGkCgYEAlTok\n\
Q8nX1pn8GyeflYu9o5Bots+PSXEe1DCzB7OUdoErp/p/dhJ5Ckts7Gt2FTgrUcXx\n\
JyaATOGUJ59SIwbMi+FxSOgES9oIeBMccIc9SkQlH4RhPfwSrnxiB0Mi5KSHQKIA\n\
Bza8r3+g7dhZz2J4J+/erLvsrwwFmeFsfOl6yX8CgYEAoABiL/785t9h3VZUU15J\n\
PuZCD5e33NsjsVwDqPygJUxf9Eysg7QaXpSeKetvCyV+STVYbbzl4UyC0ricNEXU\n\
BBzwMeNIu/TQaRx0kztA3LAmPhC6Y2z8xIxLnu3cCaN8BPONjN1Ocrh07ivl4jlr\n\
pt6SbBkeG90/NO4W8a9c/bkCgYAuYLOEneaGu7Sue9INGDEH9ImWx0sw+AcsyzXY\n\
3ub1LY/z1NZoS7VyjZ58m6lHTv2nnG0mTcDyI+l3pvxQBnzrvFUI45LyQAEB0G62\n\
SlGyExu2f9349a6Yq++LckIV7Uxbuf1oQIrDwFazlNnUqjXNs67w4Dbe8E2NVZHy\n\
AF444QKBgFTE5IAKpWBhQRtk0+YEB9Oj8Ny+PyMNwo9UCkYJSQgD/XZ5oRYaUzxz\n\
yaIWRbDf80xoeVjdNZU5/2n9lbLRXj0RWillUklgKhhuoX9su/mL8n1fXpTcGjZz\n\
pu/inMjiBCS/cVdTfDf6VZywQ2NLDBJqWzo9E1xxapBPDSBuB5nQ\n\
-----END RSA PRIVATE KEY-----\n"


def test_get_info_from_path_transform(auth_client):
    assert isinstance(auth_client.config, dict)


def test_generate_jwt_token(auth_client, fake_private_key):
    with patch('marketingcloud.jwt.open', mock_open(read_data=fake_private_key)):
        auth_client.config['privateKeyPath'] = 'filepath'
        token = auth_client._generate_jwt()
    assert isinstance(token, bytes)


def test_successful_access_token_response(monkeypatch, auth_client, fake_private_key, successful_token_response):
    with patch('marketingcloud.jwt.open', mock_open(read_data=fake_private_key)):
        monkeypatch.setattr(requests, 'post', successful_token_response)
        assert 'access_token' in auth_client.get_token()


def test_error_on_unsuccessful_token_response(monkeypatch, auth_client, fake_private_key, unsuccessful_token_response):
    monkeypatch.setattr(requests, 'post', unsuccessful_token_response)
    with patch('marketingcloud.jwt.open', mock_open(read_data=fake_private_key)):
        with pytest.raises(AuthenticationError):
            auth_client.get_token()


def test_wrong_method_invocation(monkeypatch, auth_client, fake_private_key, successful_token_response):
    monkeypatch.setattr(requests, 'post', successful_token_response)
    with patch('marketingcloud.jwt.open', mock_open(read_data=fake_private_key)):
        with pytest.raises(InvalidMethodInvocation):
            auth_client.request("wrong_method", "fake_url")
