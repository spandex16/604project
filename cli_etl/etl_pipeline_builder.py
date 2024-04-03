from etl_pipeline import ETLPipeline
from extractor.Extractor import Extractor
from transformer.Transformer import Transformer


class ETLPipelineBuilder:
    extractor = None
    transformer = None
    loader = None

    def add_extractor(self, extractor: Extractor):
        self.extractor = extractor
        return self

    def add_transformer(self, transformer: Transformer):
        self.transformer = transformer
        return self

    def add_loader(self, loader):
        self.loader = loader
        return self

    def build(self):
        return ETLPipeline(self.extractor, self.transformer, self.loader)