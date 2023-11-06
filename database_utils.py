import psycopg2  # the appropriate database library for database system
import yaml
from yaml.loader import SafeLoader

class DatabaseConnector:

    def __init__(self,file_name = None):
        self.file_name = file_name

    def read_db_creds(self):
        with open(self.file_name) as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data 