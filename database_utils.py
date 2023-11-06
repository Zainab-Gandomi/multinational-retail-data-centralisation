import psycopg2  # the appropriate database library for database system


class DatabaseConnector:

    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        pass

    def upload_data(self, data, table_name):
        pass