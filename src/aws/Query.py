import boto3
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

DATABASE_NAME = "RawSensorData"
TABLE_NAME = "SensorTableFD"

access_key_id = AWS_ACCESS_KEY_ID
secret_access_key = AWS_SECRET_ACCESS_KEY
region_name ="us-east-2"

class TimestreamQuery:
    def __init__(self):
        self.client = boto3.client(
            "timestream-query",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
        )

    def run_query(self, query_string):
        try:
            page_iterator = self.client.get_paginator("query").paginate(QueryString=query_string)
            rows = []
            for page in page_iterator:
                rows.extend(self.__parse_query_result(page))
            return pd.DataFrame(rows)
        except Exception as err:
            print("Exception while running query:", err)
            return None

    def __parse_query_result(self, query_result):
        column_info = query_result["ColumnInfo"]
        data = []
        for row in query_result["Rows"]:
            data.append(self.__parse_row(column_info, row))
        return data

    def __parse_row(self, column_info, row):
        row_data = {}
        for j in range(len(column_info)):
            info = column_info[j]
            datum = row["Data"][j]
            row_data[info.get("Name", "")] = self.__parse_datum(info, datum)
        return row_data

    def __parse_datum(self, info, datum):
        if datum.get("NullValue", False):
            return None

        column_type = info["Type"]

        if "TimeSeriesMeasureValueColumnInfo" in column_type:
            return self.__parse_time_series(info, datum)
        elif "ArrayColumnInfo" in column_type:
            array_values = datum["ArrayValue"]
            return [self.__parse_datum(info["Type"]["ArrayColumnInfo"], value) for value in array_values]
        elif "RowColumnInfo" in column_type:
            row_column_info = info["Type"]["RowColumnInfo"]
            row_values = datum["RowValue"]
            return self.__parse_row(row_column_info, row_values)
        else:
            return datum["ScalarValue"]

    def __parse_time_series(self, info, datum):
        time_series_data = []
        for data_point in datum["TimeSeriesValue"]:
            time_series_data.append(
                {
                    "time": data_point["Time"],
                    "value": self.__parse_datum(
                        info["Type"]["TimeSeriesMeasureValueColumnInfo"], data_point["Value"]
                    ),
                }
            )
        return time_series_data
