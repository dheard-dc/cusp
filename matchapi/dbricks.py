import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, orm, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

load_dotenv()

DATABRICKS_DATABASE_URI = f'databricks+connector://token:{os.getenv("DATABRICKS_TOKEN")}@{os.getenv("DATABRICKS_SERVER_HOSTNAME")}:{os.getenv("DATABRICKS_SERVER_PORT")}/{os.getenv("DATABRICKS_DB")}'

engine = create_engine(
    DATABRICKS_DATABASE_URI,
    connect_args={
        "http_path": os.getenv("DATABRICKS_HTTP_PATH")
    }
)

Session = orm.sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def session():
    db = Session()
    try:
        yield db
    finally:
        db.close()
        
EntityBase = declarative_base(bind=engine)


