import pandas as pd
from abc import ABC, abstractmethod
class Transformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass