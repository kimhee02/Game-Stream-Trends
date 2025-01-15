import redshift_connector
from utils.fastapi_dbconfig import DB_CONFIG

class DB():
    def __init__(self):
        HOST = DB_CONFIG["host"]
        DATABASE = DB_CONFIG["database"]
        PORT = DB_CONFIG["port"]
        USER = DB_CONFIG["user"]
        PASSWORD = DB_CONFIG["password"]

        self.conn = redshift_connector.connect(
            host=HOST,
            database=DATABASE,
            port=PORT,
            user=USER,
            password=PASSWORD
        )
        print("Connected to Redshift")
    
        self.cur = self.conn.cursor()

    def execute_query(self, query: str):
        self.cur.execute(query)
        return self.cur.fetchall()

    def __del__(self):
        if self.cur:
            self.cur.close()
            print("Cursor closed")
        if self.conn:
            self.conn.close()
            print("Connection closed")