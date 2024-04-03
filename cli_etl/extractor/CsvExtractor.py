from logging import Logger
import pandas as pd
from extractor.Extractor import Extractor


class CsvExtractor(Extractor):

    def _extract_from_csv(self):
        self.logger.info(f"Starting extraction from {self.source}")
        # read CSV
        try:
            csv_data = pd.read_csv(self.source)
            self.logger.info(f"{len(csv_data)} records extracted from {self.source}")
            return csv_data
        except Exception as exp:
            self.logger.error(f"{exp} occurred while extracting {self.source}")


    def extract(self):
        return self._extract_from_csv()

    def __init__(self, source: str, logger: Logger):
        self.source = source
        self.logger = logger
