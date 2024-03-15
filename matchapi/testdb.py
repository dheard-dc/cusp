import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

print(f"Connecting to databricks host {os.environ['DATABRICKS_SERVER_HOSTNAME']}...")
with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:
  print("Connected.")
  
  with connection.cursor() as cursor:
    print("Retrieving data from rules table...")
    cursor.execute("SELECT * FROM cme_fuzzy.rules")
    result = cursor.fetchall()

    print(f"Retrieved {len(result)} record(s):")
    for row in result:
      print(f"- {row.asDict()['Rule_ID']}")
      
print("Execution complete.")