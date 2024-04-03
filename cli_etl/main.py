import argparse
import logging

from etl_pipeline_builder import ETLPipelineBuilder
from extractor.ApiExtractor import ApiExtractor
from extractor.CsvExtractor import CsvExtractor
from loader.DatabaseLoader import DatabaseLoader
from transformer.transformer_strategies import StrategyTransformer


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['csv', 'api'], type=str, required=True, help="Source mode")
    parser.add_argument('--src', choices=['https://quickstats.nass.usda.gov/api/api_GET', 'C:\\Repo\\604project\\fao_data_crops_data.csv'], type=str, required=True, help="Source file/api address")
    parser.add_argument('--dest', type=str, default='agriculture', required=True, help="Destination database table")
    return parser.parse_args()



if __name__ == '__main__':
    args = get_args()
    print(args)

    logger = logging.getLogger("ETL Pipeline")
    logging.basicConfig(filename='ETL.log', format='%(asctime)s:%(levelname)s:%(message)s', encoding='utf-8',
                        level=logging.DEBUG)
    builder = ETLPipelineBuilder()
    if args.mode == 'csv':
        extractor = CsvExtractor(args.src, logger)
    elif args.mode == 'api':
        extractor = ApiExtractor(args.src, logger)
    else:
        raise Exception("Unsupported source mode")

    loader = DatabaseLoader(args.dest, logger)
    transformer = StrategyTransformer(logger)
    pipeline = builder.add_extractor(extractor).add_transformer(transformer).add_loader(loader).build()

    data_table = pipeline.extract().transform().load()
    print(f"{len(data_table)} rows in database")
