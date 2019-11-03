import typing
import pandas
import json

from .analytics import Analytics


class _Table:
    """This class serves as an abstraction layer for the received data from the /reports endpoint
    Received data will be modelled in this 2-d like table and can later be transformed into more advanced
    data models, ie pandas.Dataframe or alike.
    """
    columns: typing.List[str]
    rows: typing.List[typing.Tuple[str, typing.List[typing.Union[int, float]]]]
    dimension: str

    def __init__(self,
                 id: int,
                 analytics_client: Analytics) -> None:
        self.id = id

    def process_response(self, chunk: dict) -> None:
        if 'rows' in chunk:
            rows = [(row['value'], row['data']) for row in chunk['rows']]
            if not getattr(self, 'rows', None):
                self.rows = rows
            else:
                self.rows += rows
            self.dimension = chunk['columns']['dimension']['id']
        else:
            self.rows = [("Total", chunk['summaryData']['totals'])]

    def process_payload(self, payload: typing.Union[str, dict]) -> None:
        """TODO: retrieve custom metrics names with analytics_client from /calculatedmetrics/{id}
           TODO: retrive custom segment names with analytics_client from /segments/{id}
        """
        if isinstance(payload, str):
            payload = json.loads(payload)
        self.columns = [metric['id'] for metric in payload['metricContainer']['metrics']]

    def __repr__(self):
        return f'<Table {self.columns}>'

class Reports:
    tables = []

    def __init__(self, config: typing.Union[str, typing.TextIO]) -> None:
        self.analytics_client = Analytics(config)

    def _update_page_settings(self, payload: dict) -> dict:
        """Increments payloads settings.page value by one if not last page"""
        if not 'settings' in payload:
            payload['settings'] = {}
        if not 'page' in payload['settings']:
            payload['settings']['page'] = 0
        payload['settings']['page'] += 1
        return payload

    def _get(self,
             payload: typing.Union[str, dict]) -> typing.Generator[dict, None, None]:
        """Requests the /report endpoint with the payload provided"""
        if isinstance(payload, str):
            payload = json.loads(payload)
        while True:
            response = self.analytics_client.reports(payload)
            response_dict = response.json()
            self._update_page_settings(payload)
            yield response_dict
            # lastPage indicates the last response chunk
            # break the loop if this flag is set in the response
            if response_dict['lastPage']:
                break

    def _create_table(self,
                      payload: typing.Union[str, dict],
                      all_pages: bool) -> _Table:
        """Creates a new intermediate table format _Table"""
        table = _Table(len(self.tables) + 1, self.analytics_client)
        table.process_payload(payload)
        for chunk in self._get(payload):
            table.process_response(chunk)
            if not all:
                break
        self.tables.append(table)
        return table

    def get_dataframe(self,
                      payload: typing.Union[str, dict],
                      all_pages: bool = True) -> pandas.DataFrame:
        """Requests the Adobe Analytics /reports endpoint with the provided payload data
        and returns a pandas.DataFrame object.
        if 'all_pages' is set to False, only the first page will be requested. Otherwise
        this method will continue requesting following pages until the lastPage flag is set
        """
        table = self._create_table(payload, all_pages)
        data = [row[1] for row in table.rows]
        index = [row[0] for row in table.rows]
        columns = table.columns
        df = pandas.DataFrame(data, columns=columns, index=index)
        return df
