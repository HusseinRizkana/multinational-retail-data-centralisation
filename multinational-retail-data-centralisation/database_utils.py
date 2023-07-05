import yaml
import sqlalchemy as db
import pandas as pd
class DatabaseConnector:

    def __init__(self):
        self.db_engine = None
        self.table = None

    def read_db_creds(self,cred_path):
        with open(cred_path,"r") as f:
            cred_dic = yaml.safe_load(f)
            username = cred_dic['RDS_USER']
            password = cred_dic['RDS_PASSWORD']
            host = cred_dic['RDS_HOST']
            database = cred_dic['RDS_DATABASE']
            port = cred_dic['RDS_PORT']
            return username,password,host,database,port
        
    def init_db_engine(self,*args):
        
        url_object = db.URL.create("postgresql+psycopg2",
                                username=args[0],password=args[1],host=args[2],database=args[3], port = args[4])
        self.db_engine=db.create_engine(url_object)
        return self.db_engine
    
    def retrieve_engine(self,creds):
        return self.init_db_engine(*self.read_db_creds(creds))
    
    def upload_to_db(self, df: pd.DataFrame, table_name: str,  overwrite: bool=False):
        """
        Uploads a Pandas DataFrame to SQL database using SQLAlchemy.

        Args:
            df: A Pandas DataFrame to upload to SQL database.
            engine: An SQLAlchemy Engine object for the database connection.
            table_name: A string representing the name of the table in which the data is to be uploaded.
            overwrite: A boolean flag indicating whether to append or overwrite the data in the table. Default value is False, i.e. append to table.

        Returns:
            None
        """
        if overwrite:
            mode = "replace"
        else:
            mode = "append"
        
        return df.to_sql(table_name, self.db_engine, if_exists=mode, index=False)



