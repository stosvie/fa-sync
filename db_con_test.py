import sqlalchemy as sa
import pyodbc
from urllib import parse


class DbTest:
    engine = None
    connection = None
    schema = 'fs'
    
    def __init__(self):
        self.engine = None
        self.schema = 'fs'
        self.connection = None

    def connect(self, server, dbname, username, pwd):

        connecting_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;TDS_Version=8.0;Port=1433;'
        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)

        self.engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        self.connection = self.engine.connect()


    def terminate(self):
        self.connection.close()


sqldb = DbTest()

sqldb.connect('tcp:woo.database.windows.net,1433', 'BYWS', 'boss', 's7#3QzOsB$J*^v3')
sqldb.terminate()
