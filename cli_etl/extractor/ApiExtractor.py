from logging import Logger
import certifi
import pandas as pd
import json
import urllib3

from extractor.Extractor import Extractor


class ApiExtractor(Extractor):
    def _extract_from_http(self):
        parameters = {'key': 'D9A13880-58EA-3A24-8CC2-A08A2C9A0E34',
                      'source_desc': 'SURVEY',
                      'agg_level_desc': 'NATIONAL',
                      'commodity_desc': 'CATTLE',
                      'unit_desc': 'HEAD',
                      'freq_desc': 'ANNUAL',
                      'statisticcat': 'COWS, INCL CALVES - INVENTORY'
                      }
        self.logger.info(f"Starting extraction from {self.source}")
        api_data = pd.DataFrame()
        with urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()) as http:
            response = http.request('GET', self.source, fields=parameters)

            if response.status == 200:
                read_response = json.loads(response.data.decode('utf-8'))
                api_data = pd.json_normalize(read_response['data'])
                self.logger.info(f"{len(api_data)} records extracted from {self.source}")

            else:
                self.logger.error(f"Error code {response.status}")

        return api_data

    def extract(self):
        return self._extract_from_http()
        pass

    def __init__(self, source: str, logger: Logger):
        self.source = source
        self.logger = logger
