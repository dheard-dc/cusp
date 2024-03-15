import os
from dotenv import load_dotenv
from databricks import sql
import logging

logger = logging.getLogger(__name__)

load_dotenv()

class Brix():
    def __init__(self, *args, **kwargs):
        # Accept passed in connection details, and default to ENV VAR versions if not supplied
        self.server_hostname = kwargs.get('server_hostname', os.getenv("DATABRICKS_SERVER_HOSTNAME"))
        self.http_path       = kwargs.get('http_path', os.getenv("DATABRICKS_HTTP_PATH"))
        self.access_token    = kwargs.get('access_token', os.getenv("DATABRICKS_TOKEN"))       
    
    def get_connection(self, *args, **kwargs):
        logger.info(f"Connecting to databricks host {self.server_hostname}...")
        connection = sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        ) 
        logger.info("Connected.")
        return connection
  
    def execute_query(self, sql: str):
        logger.info("Running query...")
        logger.debug(f"SQL: {sql}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                logger.info(f"Retrieved {len(result)} record(s).")
                
        out = [ row.asDict() for row in result]
        return out
      
