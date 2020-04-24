from sqlalchemy.engine import create_engine
import pandas as pd

from model import CreateDatabase

class Extractions(CreateDatabase):

    def __init__(self, url, output_path, name_db):
        engine = create_engine(url)
        self.orders = pd.read_sql_table('orders', con=engine)
        self.merchants = pd.read_sql_table('merchants', con=engine)
        super().__init__(output_path, name_db)
