import os
import pytest
import pandas
from unittest.mock import mock_open, patch

from marketingcloud.analytics_reports import Reports


payloads = (
    """{
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
    """,
    """{
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
                    "columnId": "metrics/visits:::0",
                    "id": "metrics/visits",
                    "filters": [
                        "STATIC_ROW_COMPONENT_1"
                    ]
                },
                {
                    "columnId": "metrics/pageviews:::2",
                    "id": "metrics/pageviews",
                    "filters": [
                        "STATIC_ROW_COMPONENT_3"
                    ]
                }
            ],
            "metricFilters": [
                {
                    "id": "STATIC_ROW_COMPONENT_1",
                    "type": "segment",
                    "segmentId": "Single_Page_Visits"
                },
                {
                    "id": "STATIC_ROW_COMPONENT_3",
                    "type": "segment",
                    "segmentId": "Single_Page_Visits"
                }
            ]
        },
        "settings": {
            "countRepeatInstances": true
        },
        "statistics": {
            "functions": [
                "col-max",
                "col-min"
            ]
        }
    }
    """,
    """{
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
                    "id": "cm1214_5aec40c4373fa864abcbe786",
                    "sort": "desc"
                }
            ]
        },
        "dimension": "variables/server",
        "settings": {
            "countRepeatInstances": true,
            "limit": 50,
            "page": 0,
            "nonesBehavior": "exclude-nones"
        },
        "statistics": {
            "functions": [
                "col-max",
                "col-min"
            ]
        }
    }   
    """
)

chunks = (
    {
    "totalPages": 1,
    "firstPage": True,
    "lastPage": True,
    "numberOfElements": 30,
    "number": 0,
    "totalElements": 30,
    "columns": {
        "dimension": {
            "id": "variables/daterangeday",
            "type": "time"
        },
        "columnIds": [
            "0"
        ]
    },
    "rows": [
        {
            "itemId": "1191001",
            "value": "Nov 1, 2019",
            "data": [
                385351
            ]
        },
        {
            "itemId": "1191002",
            "value": "Nov 2, 2019",
            "data": [
                219369
            ]
        },
        {
            "itemId": "1191003",
            "value": "Nov 3, 2019",
            "data": [
                86859
            ]
        },
        {
            "itemId": "1191004",
            "value": "Nov 4, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191005",
            "value": "Nov 5, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191006",
            "value": "Nov 6, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191007",
            "value": "Nov 7, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191008",
            "value": "Nov 8, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191009",
            "value": "Nov 9, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191010",
            "value": "Nov 10, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191011",
            "value": "Nov 11, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191012",
            "value": "Nov 12, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191013",
            "value": "Nov 13, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191014",
            "value": "Nov 14, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191015",
            "value": "Nov 15, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191016",
            "value": "Nov 16, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191017",
            "value": "Nov 17, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191018",
            "value": "Nov 18, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191019",
            "value": "Nov 19, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191020",
            "value": "Nov 20, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191021",
            "value": "Nov 21, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191022",
            "value": "Nov 22, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191023",
            "value": "Nov 23, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191024",
            "value": "Nov 24, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191025",
            "value": "Nov 25, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191026",
            "value": "Nov 26, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191027",
            "value": "Nov 27, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191028",
            "value": "Nov 28, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191029",
            "value": "Nov 29, 2019",
            "data": [
                0
            ]
        },
        {
            "itemId": "1191030",
            "value": "Nov 30, 2019",
            "data": [
                0
            ]
        }
    ],
    "summaryData": {
        "filteredTotals": [
            691086
        ],
        "totals": [
            691086
        ],
        "col-max": [
            385351
        ],
        "col-min": [
            86859
        ]
    }
},
    {
    "totalPages": 0,
    "firstPage": True,
    "lastPage": True,
    "numberOfElements": 0,
    "number": 0,
    "totalElements": 0,
    "columns": {
        "columnIds": [
            "metrics/visits:::0",
            "metrics/pageviews:::2"
        ]
    },
    "summaryData": {
        "filteredTotals": [
            276654,
            276654
        ],
        "totals": [
            276654,
            276654
        ]
    }
},
    {
    "totalPages": 1,
    "firstPage": True,
    "lastPage": True,
    "numberOfElements": 12,
    "number": 0,
    "totalElements": 12,
    "columns": {
        "dimension": {
            "id": "variables/server",
            "type": "string"
        },
        "columnIds": [
            "0"
        ]
    },
    "rows": [
        {
            "itemId": "3910959580",
            "value": "www.logistics.dhl.ru",
            "data": [
                0
            ]
        },
        {
            "itemId": "3621147856",
            "value": "dscsocialresponsibility.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "3143510376",
            "value": "dhlexpressshipping.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "2992096587",
            "value": "dhl.lookbookhq.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "2895133182",
            "value": "logistics-uat.dhl.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "2752427215",
            "value": "gmgshippinggh.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "2687361157",
            "value": "www.dhl.couriers-express.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "1844528748",
            "value": "logistics-test.dhl.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "1600118480",
            "value": "www.logistics.dhl",
            "data": [
                0
            ]
        },
        {
            "itemId": "1332760244",
            "value": "www.dhlexpress-online.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "522414283",
            "value": "falconexpressons.com",
            "data": [
                0
            ]
        },
        {
            "itemId": "149891183",
            "value": "www.fastbarq.com",
            "data": [
                0
            ]
        }
    ],
    "summaryData": {
        "filteredTotals": [
            0
        ],
        "totals": [
            0
        ],
        "col-max": [
            0
        ],
        "col-min": [
            0
        ]
    }
}
)


@pytest.fixture
def reports_client():
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
        return Reports("")


@pytest.fixture
def test_payload():
    return payloads[0]


@pytest.fixture
def test_response_chunk():
    return chunks[0]


@pytest.mark.parametrize('payload, expected',
                         [(payloads[0], 'metrics/visits'),
                          (payloads[1], 'metrics/visits|metrics/pageviews'),
                          (payloads[2], 'cm1214_5aec40c4373fa864abcbe786')])
def test_correct_table_columns(monkeypatch, test_response_chunk, payload, expected):
    def fake_get(self, payload):
        return test_response_chunk

    def fake_init(self):
        self.analytics_client = None

    monkeypatch.setattr(Reports, '_get', fake_get)
    monkeypatch.setattr(Reports, '__init__', fake_init)
    reports_client = Reports()
    table = reports_client._create_table(payload, False)
    assert "|".join(table.columns) == expected


@pytest.mark.parametrize('chunk, expected',
                         [(chunks[0], 30),
                          (chunks[1], 1),
                          (chunks[2], 12)])
def test_correct_table_columns(monkeypatch, chunk, expected):
    def fake_get(self, payload):
        yield chunk

    def fake_init(self):
        self.analytics_client = None

    monkeypatch.setattr(Reports, '_get', fake_get)
    monkeypatch.setattr(Reports, '__init__', fake_init)
    reports_client = Reports()
    table = reports_client._create_table(payloads[0], False)
    assert len(table.rows) == expected


@pytest.mark.parametrize('payload, chunk, columns',
                         [
                             (payloads[0], chunks[0], ['metrics/visits']),
                             (payloads[1], chunks[1], ['metrics/visits', 'metrics/pageviews']),
                             (payloads[2], chunks[2], ['cm1214_5aec40c4373fa864abcbe786']),
                         ])
def test_get_pandas_dataframe(monkeypatch, payload, chunk, columns):
    def fake_get(self, payload):
        yield chunk

    def fake_init(self):
        self.analytics_client = None

    monkeypatch.setattr(Reports, '_get', fake_get)
    monkeypatch.setattr(Reports, '__init__', fake_init)
    reports_client = Reports()
    df = reports_client.get_dataframe(payload, False)
    assert all(df.columns == columns)
    assert isinstance(df, pandas.DataFrame)