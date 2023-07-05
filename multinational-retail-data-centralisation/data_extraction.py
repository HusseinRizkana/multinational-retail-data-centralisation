import sqlalchemy as db
import pandas as pd

class DataExtractor:
    def __init__(self, engine):
        """
        Initialize the DataExtractor object.

        Args:
            engine: SQLAlchemy engine object for connecting to the database.
        """
        self.db_engine = engine

    def list_db_tables(self):
        """
        List all tables in the connected database.

        This method uses SQLAlchemy's inspect function to retrieve
        the schema names and table names from the database. It then
        prints the schema name and table name for each table in the database.
        """
        inspector = db.inspect(self.db_engine)
        schemas = inspector.get_schema_names()

        for schema in schemas:
            print("schema: %s" % schema)

            for table_name in inspector.get_table_names(schema=schema):
                print(table_name)

    def read_rds_table(self, table_name):
        """
        Read a specific table from the connected database.

        This method connects to the database using the engine object,
        loads the specified table, executes a select query on the table,
        fetches all rows, and converts them into a pandas DataFrame.

        Args:
            table_name: The name of the table to read from the database.

        Returns:
            A pandas DataFrame containing the data from the specified table.
        """
        with self.db_engine.connect() as conn:
            metadata = db.MetaData()
            table = db.Table(table_name, metadata, autoload_with=self.db_engine)
            query = db.select(table)
            res = conn.execute(query)
            rows = res.fetchall()
            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)


    
