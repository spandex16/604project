from extractor.Extractor import Extractor
from loader.Loader import Loader
from transformer.Transformer import Transformer


class ETLPipeline:
    data = None
    transformed_data = None

    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def load(self):
        return self.loader.load(self.transformed_data)

    def transform(self):
        self.transformed_data = self.transformer.transform(self.data)
        return self

    def extract(self):
        self.data = self.extractor.extract()
        return self
