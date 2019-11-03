import os
import pytest

from marketingcloud.analytics import Analytics


@pytest.fixture
def analytics_client():
    path = os.path.join(
        os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        'credentials.json'
    )
    config_path = os.environ.get('MARKETINGCLOUD_CREDENTIALS', path)
    return Analytics(config_path)


def test_get_calulatedmetrics(analytics_client):
    res = analytics_client.get_calculatedmetrics()
    assert res.status_code == 200


def test_get_segments(analytics_client):
    res = analytics_client.get_segments()
    assert res.status_code == 200


def test_get_report(analytics_client):
    payload = """{
        "rsid": "dhlglobalrolloutprod",
        "globalFilters": [
            {
                "type": "dateRange",
                "dateRange": "2019-11-01T00:00:00.000/2019-12-01T00:00:00.000"
            }
        ],
        "metricContainer": {
            "metrics": [
                {
                    "columnId": "0",
                    "id": "metrics/visits"
                }
            ]
        },
        "dimension": "variables/daterangeday",
        "settings": {
            "countRepeatInstances": true,
            "limit": 10,
            "page": 0,
            "dimensionSort": "asc",
            "nonesBehavior": "return-nones"
        },
        "statistics": {
            "functions": [
                "col-max",
                "col-min"
            ]
        }
    }
    """
    res = analytics_client.reports(payload)
    assert res.status_code == 200


def test_users_method(analytics_client):
    res = analytics_client.users()
    assert res.status_code == 200


def test_users_me_method(analytics_client):
    res = analytics_client.user_me()
    assert res.status_code == 200
