import sqlalchemy as sa
# import pyodbc

import pymssql as mssql
from urllib import parse



class DbTest:
    engine = None
    connection = None
    schema = 'fs'
    
    def __init__(self):
        self.engine = None
        self.schema = 'fs'
        self.connection = None


    def connect2(self, server, dbname, username, pwd):

        connecting_string = 'DRIVER={SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;TDS_Version=8.0;Port=1433;'
        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)

        self.engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        self.connection = self.engine.connect()

    def connect(self, server, dbname, username, pwd):

        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)
                                        
        self.engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        
        self.connection = self.engine.connect()


    def connect_p(self, server, dbname, username, pwd):

        connecting_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;TDS_Version=8.0;Port=1433;'
        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)

        

        driver = 'mysql'
        url = '{}://{}:{}@{}/{}'
    
        url = url.format(driver, parse.quote_plus(username), parse.quote_plus(pwd), server, dbname)
        # next line is for django app
        # logger.warning("="*20+'\nabout to connect to alchemy:::' + url + '\n' + "="*20)
    
        # The return value of create_engine() is our connection object
        self.engine = sa.create_engine(url,echo=True) # echo for debugging queries


        #conn_str = r'mssql+pymssql://(local)\SQLEXPRESS/myDb'
        
        #cs = f"mssql+pymssql://{username}:{pwd}@{server}/{dbname}"
        #self.engine = sa.create_engine(cs)
        self.connection = self.engine.connect()
        #        server=server,
        #        port=1433,
        #        user='{0}@{1}'.format(username, server),
        #        password=pwd,
        #        database=dbname)

    def terminate(self):
        self.connection.close()


sqldb = DbTest()

host='srdjans.sg-host.com'
db ='dberhjrc79rrxj'
user='uejdurd87jk8d'
pwd='$#3&I[j1bvA2'
sqldb.connect2('tcp:woo.database.windows.net,1433', 'BYWS', 'boss', 's7#3QzOsB$J*^v3')
# sqldb.connect_p(host,db,user,pwd)
sqldb.terminate()
