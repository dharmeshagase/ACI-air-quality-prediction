import time
import logging
import sys
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_record(record):
    """
    Preprocess the record by replacing missing values (-200) with None and standardizing date/time formats.
    """
    processed = {}
    for key, value in record.items():
        # Check for missing value represented as -200
        # The -200 can be string or a number, hence we check for both and replace it with None
        # We are converting -200 to None because in the consumer code we will use pandas and it will be
        # easier for such libraries to detect None as null or NaN rather than -200
        # We are also doing this pre-processing in the producer itself to ensure that every value streamed to the 
        # kafka is already cleansed and the missing values are properly handled. This also helps to maintain 
        # data integrity across the further process/pipeline.
        # One more reason for converting to None is avoiding miscalculations or misinterpretations in the 
        # statistical analysis or predictive models.
        if value == -200 or value == "-200":
            processed[key] = None
        else:
            processed[key] = value

        # Create standardized datetime that will be easy to convert in EDA
        if 'Date' in processed and 'Time' in processed:
            try:
                # Only process if both date and time are not None
                if processed['Date'] is not None and processed['Time'] is not None:
                    date_str = str(processed['Date']).strip()
                    time_str = str(processed['Time']).strip()
                
                    # Parsing date and time in the specific dataset format
                    if '/' in date_str and '.' in time_str:
                        # Here we convert DD/MM/YYYY to datetime components
                        day, month, year = map(int, date_str.split('/'))
                        # Convert HH.MM.SS to time components
                        hour, minute, second = map(int, time_str.split('.'))
                    
                    # Create ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
                    # This format is will help for pd.to_datetime() function in EDA
                    processed['datetime_iso'] = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"
            except Exception as e:
                logging.warning(f"Could not standardize date/time: {e}")
    return processed

def has_measurement_data(record):
    """
    Check if a record has at least one valid measurement field.
    """
    measurement_fields = [
        'CO(GT)', 'PT08.S1(CO)', 'NMHC(GT)', 'C6H6(GT)', 
        'PT08.S2(NMHC)', 'NOx(GT)', 'PT08.S3(NOx)', 'NO2(GT)', 
        'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'
    ]
    
    for field in measurement_fields:
        if field in record and record[field] is not None:
            return True
    return False

def main():
    # Kafka configuration 
    # The kafka topic is created by the name "air_quality_topic"
    kafka_bootstrap_servers = ['localhost:9092']
    topic_name = 'air_quality_topic'
    
    # Initialize Kafka producer with JSON serialization
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logging.info("Kafka Producer connected successfully.")
    except Exception as e:
        logging.error("Failed to connect to Kafka: %s", e)
        sys.exit(1)
    
    # Load dataset using pandas
    # Error handling implemented for any errors
    try:
        # Reading the data from the AirQualityUCI.csv file
        df = pd.read_csv('data/AirQualityUCI.csv', sep=';', decimal=',')
        # Drop any trailing empty columns if present
        # Part of cleansing the data before it is sent to the kafka topic
        df = df.iloc[:, :-2]
        # Filter out completely empty rows
        df = df.dropna(how='all')
        #Logging the cleansed data
        logging.info("Dataset loaded successfully with shape: %s", df.shape)
    except Exception as e:
        #Logging the error and exiting
        logging.error("Error reading CSV file: %s", e)
        sys.exit(1)
    
    total_records = len(df)
    records_sent = 0
    records_skipped = []
    # Stream each record with a delay to the kafka topic
    for index, row in df.iterrows():
        record = row.to_dict()
        processed_record = preprocess_record(record)

        try:
            # Add record metadata
            processed_record['_record_index'] = int(index)
            processed_record['_producer_timestamp'] = datetime.now().isoformat()
            producer.send(topic_name, value=processed_record)
            records_sent+=1
            logging.info("Sent record %s to topic '%s'", index, topic_name)
        except Exception as e:
            logging.error("Error sending record %s: %s", index, e)
        # Adding delay of 1s to simulate real-time readings
        time.sleep(1)
    

    # Ensure all messages are sent before closing the producer
    producer.flush()
    logging.info("All records have been sent.")
    print("Records skipped",records_skipped)
    print("Records Sent",records_sent)
    print("Total records",total_records)

if __name__ == "__main__":
    main()
