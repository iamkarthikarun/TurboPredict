import time
import os
import json
from kafka import KafkaConsumer, TopicPartition
import boto3
from boto3 import client
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
def save_timestamp_to_storage(timestamp):
  """
  Saves the timestamp (Unix epoch) to a local text file as an integer.

  Args:
      timestamp (int): The Unix epoch timestamp.
  """
  timestamp_file = "last_timestamp.txt"
  with open(timestamp_file, "w") as f:
    f.write(str(timestamp))

def get_last_timestamp_from_storage():
  """
  Retrieves the last timestamp (Unix epoch) from a local text file.

  Returns:
      int: The last timestamp stored in the file as an integer, or None if not found.
  """
  timestamp_file = "last_timestamp.txt"
  if os.path.exists(timestamp_file):
    with open(timestamp_file, "r") as f:
      timestamp_str = f.read().strip()
      if timestamp_str:
        return float(timestamp_str)
  return None

DATABASE_NAME = "RawSensorData"
TABLE_NAME = "SensorTableFD"

INTERVAL = 1 # Seconds

session = boto3.Session()
client = boto3.client('timestream-write',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name="us-east-2")

def prepare_common_attributes(unit_no, engine_class):

    dimensions = [
            {'Name': 'unit_nr', 'Value': str(unit_no)},
            {'Name': 'engine_class', 'Value':str(engine_class)}
        ]
    
    common_attributes = {
        'Dimensions': dimensions,
        'MeasureName': 'Sensor Data',
        'MeasureValueType': 'MULTI'
    }
    return common_attributes


def prepare_record(timestamp):
    record = {
        'Time': str(timestamp),
        'MeasureValues': []
    }
    return record


def prepare_measure(measure_name, measure_value):
    measure = {
        'Name': measure_name,
        'Value': str(measure_value),
        'Type': 'DOUBLE'
    }
    return measure

@staticmethod
def _print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])

def write_records(records, common_attributes):
    try:
        result = client.write_records(DatabaseName=DATABASE_NAME,
                                            TableName=TABLE_NAME,
                                            CommonAttributes=common_attributes,
                                            Records=records)
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords HTTPStatusCode: %s" %
              (len(records), status))
    except client.exceptions.RejectedRecordsException as err:
      _print_rejected_records_exceptions(err)
    except Exception as err:
      print("Error:", err)


def write_to_AWS_TS():

    print("writing data to database {} table {}".format(
        DATABASE_NAME, TABLE_NAME))

    KAFKA_BROKER_ADDRESS = "localhost:9092"

    consumer = KafkaConsumer("RawSensorData",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    bootstrap_servers=[KAFKA_BROKER_ADDRESS],
    consumer_timeout_ms=1000,
    max_poll_records=25)

    consumer.subscribe('RawSensorData')
    partition = TopicPartition('RawSensorData', 0)

    last_timestamp = get_last_timestamp_from_storage()
    print(last_timestamp)
    if last_timestamp:
        # Seek to the offset corresponding to the last timestamp (approximate)
        # This might not be perfect due to potential message ordering issues
        rec_in  = consumer.offsets_for_times({partition:last_timestamp})
        print(rec_in[partition].offset)
        #rec_out = consumer.offsets_for_times({partition:current_times})
        #print(rec_in, rec_out)
        consumer.seek(partition, rec_in[partition].offset+1)
        """offsets = consumer.offsets_for_times({partition: last_timestamp})
        if offsets:
            offset_val = list(offsets.values())[0]
            print(offset_val)
            print(partition)
            consumer.seek(partition, offset_val)
        else:
            print(f"No offsets found for timestamp: {last_timestamp}")"""
    records = []
    try:
        for msg in consumer:
            try:
                # Check if message is a dictionary
                if isinstance(msg.value, dict):
                    message_dict = msg.value
                    print(message_dict['timestamp'])
                    print(type(message_dict['timestamp']))
                else:
                    # Decode JSON if not already a dictionary
                    message_dict = json.loads(msg.value.decode("utf-8"))
                if(float(last_timestamp) >= float(message_dict['timestamp'])):
                   break
                print(f"Received message: {message_dict}")
                common_attributes = prepare_common_attributes(message_dict['unit_nr'], message_dict['engine_class'])
                record = prepare_record(message_dict['timestamp'])
                #print(record)
                record['MeasureValues'].append(prepare_measure('time_cycles', message_dict['time_cycles']))
                #print(message_dict['setting_1'], type(message_dict['setting_1']))
                record['MeasureValues'].append(prepare_measure('setting_1', message_dict['setting_1']))
                #print(message_dict['setting_2'], type(message_dict['setting_2']))
                record['MeasureValues'].append(prepare_measure('setting_2', message_dict['setting_2']))
                #print(message_dict['setting_3'], type(message_dict['setting_3']))
                record['MeasureValues'].append(prepare_measure('setting_3', message_dict['setting_3']))
                record['MeasureValues'].append(prepare_measure('T2', message_dict['T2']))
                #print(message_dict['T2'], type(message_dict['T2']))
                record['MeasureValues'].append(prepare_measure('T24', message_dict['T24']))
                record['MeasureValues'].append(prepare_measure('T30', message_dict['T30']))
                record['MeasureValues'].append(prepare_measure('T50', message_dict['T50']))
                record['MeasureValues'].append(prepare_measure('P2', message_dict['P2']))
                record['MeasureValues'].append(prepare_measure('P15', message_dict['P15']))
                record['MeasureValues'].append(prepare_measure('P30', message_dict['P30']))
                record['MeasureValues'].append(prepare_measure('Nf', message_dict['Nf']))
                record['MeasureValues'].append(prepare_measure('Nc', message_dict['Nc']))
                record['MeasureValues'].append(prepare_measure('epr', message_dict['epr']))
                record['MeasureValues'].append(prepare_measure('Ps30', message_dict['Ps30']))
                record['MeasureValues'].append(prepare_measure('phi', message_dict['phi']))
                record['MeasureValues'].append(prepare_measure('NRf', message_dict['NRf']))
                record['MeasureValues'].append(prepare_measure('NRc', message_dict['NRc']))
                record['MeasureValues'].append(prepare_measure('BPR', message_dict['BPR']))
                record['MeasureValues'].append(prepare_measure('farB', message_dict['farB']))
                record['MeasureValues'].append(prepare_measure('htBleed', message_dict['htBleed']))
                record['MeasureValues'].append(prepare_measure('Nf_dmd', message_dict['Nf_dmd']))
                record['MeasureValues'].append(prepare_measure('PCNfR_dmd', message_dict['PCNfR_dmd']))
                record['MeasureValues'].append(prepare_measure('W31', message_dict['W31']))
                record['MeasureValues'].append(prepare_measure('W32', message_dict['W32']))
                # Write updated CSV data to S3
                records.append(record)
                last_timestamp = message_dict['timestamp']  # Update last timestamp
                save_timestamp_to_storage(last_timestamp)  # Save for future consumption

            except json.JSONDecodeError:
                print("Error: Message is not valid JSON. Skipping message.")
            except Exception as e:
                print(f"Error processing message: {e}")
        if records:
            write_records(records=records, common_attributes=common_attributes)
            records = []
            print("Data processed and written to S3")
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("Exiting program...")
        consumer.close()

write_to_AWS_TS()
