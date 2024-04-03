from logging import Logger

import pandas as pd
import sqlalchemy
from loader.Loader import Loader


class DatabaseLoader(Loader):

    @staticmethod
    def _get_connection_string():
        import os
        user_name = 'karol_talbot'
        password = os.environ.get('SqlPass')
        return f'mysql+mysqlconnector://{user_name}:{password}@datasciencedb.ucalgary.ca/{user_name}'

    def _create_output_table(self):
        self.logger.info(f'Creating {self.destination_table} table')
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS karol_talbot.{self.destination_table} (
                Country_Name VARCHAR(255),
                Year INT,
                Unit VARCHAR(50),
                Value INT,
                Product VARCHAR(255),
                INDEX idx_country_year (Country_Name, Year)
            )
            PARTITION BY RANGE (Year) (
                PARTITION p0 VALUES LESS THAN (1990),
                PARTITION p1 VALUES LESS THAN (2000),
                PARTITION p2 VALUES LESS THAN (2010),
                PARTITION p3 VALUES LESS THAN (2020),
                PARTITION p4 VALUES LESS THAN MAXVALUE
            );
        '''
        engine = sqlalchemy.create_engine(DatabaseLoader._get_connection_string())
        with engine.connect() as con:
            try:
                con.execute(sqlalchemy.text(create_table_query))
                self.logger.info(f"{self.destination_table} table created successfully")
            except Exception as e:
                self.logger.error(f"{e} occurred while creating {self.destination_table} table")
        engine.dispose()

    def load(self, df: pd.DataFrame):
        self._create_output_table()
        self.logger.info(f"Starting loading")
        try:
            engine = sqlalchemy.create_engine(DatabaseLoader._get_connection_string())
            df.to_sql(self.destination_table, con=engine, if_exists='append', index=False)
            self.logger.info(f"Data loaded successfully")
        except Exception as e:
            self.logger.error(f"{e} occurred while loading data")

        data = pd.DataFrame()
        try:
            data = pd.read_sql_table("agriculture", engine)
        except Exception as e:
            self.logger.error(f"{e} occurred while reading data from database")
        engine.dispose()
        return data

    def __init__(self, destination_table: str, logger: Logger):
        self.destination_table = destination_table
        self.logger = logger
