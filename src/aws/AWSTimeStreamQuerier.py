import src.aws.Query as Query
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name ="us-east-2"

DATABASE_NAME = "RawSensorData"
TABLE_NAME = "SensorTableFD"

timestream_query = Query.TimestreamQuery()
query = "SELECT * FROM " + DATABASE_NAME + "." + TABLE_NAME
df = timestream_query.run_query(query)
print(df)
