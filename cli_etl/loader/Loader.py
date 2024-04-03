import pandas as pd
from abc import ABC, abstractmethod


class Loader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass
