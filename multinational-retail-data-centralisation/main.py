from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import datetime
import pandas as pd
import pycountry 
if __name__ == "__main__":
    connectorRDS = DatabaseConnector()
    engineRDS = connectorRDS.retrieve_engine('db_creds.yaml')
    connectorPostgres = DatabaseConnector()
    enginePSQL = connectorPostgres.init_db_engine("postgres","house1998","localhost","sales_data","5432")
    extractor = DataExtractor(engine=engineRDS)
    df = extractor.read_rds_table('legacy_users')
    df = df.set_index('index')
    cleaner = DataCleaning()
    df = cleaner.clean_columns(df)
    connectorPostgres.upload_to_db(df,'dim_users',overwrite=True)

