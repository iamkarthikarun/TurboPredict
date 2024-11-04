import boto3
from botocore.exceptions import ClientError
import joblib  # For loading XGBoost model
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import Query
from xgboost import XGBRegressor

scaler = MinMaxScaler()

def preprocess_dataframe(df, selected_features=['time_cycles',
 'T24',
 'T30',
 'T50',
 'P15',
 'P30',
 'Nf',
 'Nc',
 'Ps30',
 'phi',
 'NRf',
 'BPR',
 'htBleed',
 'W31',
 'W32']):
  # Select features (if specified)
  if selected_features:
    df = df[selected_features]

  # Apply MinMaxScaler
  
  df_scaled = scaler.fit_transform(df)
  print(df_scaled)
  return df_scaled

def make_prediction(engine_class, unit_nr):
    try:
        # Initialize AWS clients (replace with your setup)
        timestream_client = Query.TimestreamQuery()

        # Replace with your Timestream details
        DATABASE_NAME = "RawSensorData"
        TABLE_NAME = "SensorTableFD"

        # Retrieve data from Timestream
        query = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}"
        response_df = timestream_client.run_query(query)
        #print(response_df)
        filtered_df = response_df[(response_df['engine_class'] == str(engine_class)) & (response_df['unit_nr'] == str(unit_nr))]
        print(filtered_df)

        # Sort the filtered DataFrame by the time_cycles column in ascending order
        sorted_df = filtered_df.sort_values(by='time_cycles', ascending=True)
        processed_data = preprocess_dataframe(sorted_df)
        new_xgb = XGBRegressor()
        # Load XGBoost model (replace with your model loading logic)
        #model_path = "xgb.h5"  # Replace with your model path
        new_xgb.load_model('xgb.h5')
        xgb_prediction = np.round(new_xgb.predict(processed_data)).astype(int)
        return xgb_prediction

    except ClientError as e:
        print(f"Error: {e}")
        return None  # Handle errors or return a specific value

if __name__ == "__main__":
    prediction = make_prediction(1,1)
    print(f"XGBoost Prediction: {prediction}")
