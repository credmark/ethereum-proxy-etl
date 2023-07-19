import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_PORT = int(POSTGRES_PORT, base=10) if POSTGRES_PORT else None
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')


ETH_NODE_URL = os.getenv('ETH_NODE_URL')

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
