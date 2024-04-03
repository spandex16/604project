from logging import Logger

import pandas as pd

from transformer.Transformer import Transformer


class StrategyTransformer(Transformer):
    @staticmethod
    def comma_separated_to_number(value):
        delim = ','
        temp = value.split(delim)
        output = ''
        for each in temp:
            output = output + each
        return output

    def _cow_data_strategy(self, cow_data):
        self.logger.info("Starting cow data transformation")
        try:
            cow_data.dropna(axis="index", thresh=4, inplace=True)
        except Exception as e:
            self.logger.error(f"{e} while dropping na values")
        try:
            cow_data = cow_data[cow_data['statisticcat_desc'] == 'INVENTORY, AVG']
            cow_data = cow_data[['country_name', 'Value', 'unit_desc', 'year', 'commodity_desc']]
            cow_data.loc[cow_data['country_name'] == 'UNITED STATES', 'country_name'] = 'United States of America'
            cow_data['Value'] = cow_data['Value'].apply(StrategyTransformer.comma_separated_to_number)

            cow_data_names = {'country_name': 'Country_Name', 'year': 'Year', 'unit_desc': 'Unit',
                              'commodity_desc': 'Product'}
            cow_data.rename(columns=cow_data_names, inplace=True)

            convert_to = {'Year': 'int', 'Value': 'int', 'Country_Name': 'string', 'Product': 'string',
                          'Unit': 'string'}
            cow_data = cow_data.astype(convert_to)
            self.logger.info("Cow data transformation successful")
        except Exception as e:
            self.logger.error(f"{e} occurred while transforming cow data")
        return cow_data

    def _crop_data_strategy(self, crop_data):
        self.logger.info("Starting crop data transformation")
        try:
            crop_data.dropna(axis="index", thresh=4, inplace=True)
        except Exception as e:
            self.logger.error(f"{e} while dropping na values")
        try:
            crop_data = crop_data[crop_data['element'].isin(["Production Quantity"])]
            crop_data = crop_data[crop_data['country_or_area'] == "United States of America"]
            crop_data = crop_data[['country_or_area', 'year', 'unit', 'value', 'category']]

            crop_data_names = {'country_or_area': 'Country_Name', 'year': 'Year', 'unit': 'Unit', 'value': 'Value',
                               'category': 'Product'}
            crop_data.rename(columns=crop_data_names, inplace=True)

            convert_to = {'Year': 'int', 'Value': 'int', 'Country_Name': 'string', 'Product': 'string',
                          'Unit': 'string'}
            crop_data = crop_data.astype(convert_to)
            self.logger.info("Crop data transformation successful")
        except Exception as e:
            self.logger.error(f"{e} occurred while transforming crop data")
        return crop_data

    def _get_strategy(self, df: pd.DataFrame):
        cols = set(df.columns.values)
        if cols >= {'country_or_area', 'year', 'unit', 'value', 'category'}:
            return self._crop_data_strategy
        elif cols >= {'country_name', 'year', 'unit_desc', 'commodity_desc'}:
            return self._cow_data_strategy
        return lambda a: a

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        fun = self._get_strategy(df)
        return fun(df)

    def __init__(self, logger: Logger):
        self.logger = logger
