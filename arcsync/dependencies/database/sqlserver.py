import pyodbc
from dotenv import loadenv
import os


loadenv()

DRIVER = os.getenv("DRIVER")
SERVER=os.getenv("SERVER")
USER=os.getenv("USER")
DATABASE =os.getnv("DATABASE")
PASSWORD=os.getenv("PASSWORD")

conn_string = f'DRIVER={DRIVER};SERVER={SERVER};PORT=1433;DATABASE={DATABASE};UID={USER};PWD={PASSWORD};\
                        Encrypt=yes;TrustServerCertificate=no;'

try:
    conn = pyodbc.connect(conn_string)
    print("Connected to the database successfully!")
except Exception as e:
    print("Error connecting to the database:", e)
