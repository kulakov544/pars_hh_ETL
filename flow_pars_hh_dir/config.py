from dotenv import load_dotenv
import os

load_dotenv()
db_url = os.getenv("db_url")
access_token = os.getenv("access_token")
