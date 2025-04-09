# import csv
# import datetime
# import logging
# import os
# import sys
# import json
# from kafka import KafkaConsumer

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def main():
#     kafka_bootstrap_servers = ['localhost:9092']
#     topic_name = 'air_quality_topic'
#     csv_file = 'air_quality_streamed.csv'
    
#     # Initialize Kafka consumer along with JSON deserialization
#     try:
#         consumer = KafkaConsumer(
#             topic_name,
#             bootstrap_servers=kafka_bootstrap_servers,
#             auto_offset_reset='earliest',
#             enable_auto_commit=True,
#             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#         )
#         logging.info("Kafka Consumer connected and subscribed to topic '%s'.", topic_name)
#     except Exception as e:
#         logging.error("Failed to connect Kafka consumer: %s", e)
#         sys.exit(1)
    
#     # Determine if header needs to be written by checking if file exists
#     write_header = not os.path.exists(csv_file)

#     # Open CSV file for appending data (remains open throughout the consumer loop)
#     with open(csv_file, 'a', newline='') as file:
#         writer = csv.writer(file)
        
#         if write_header:
#             # Customize header based on your dataset fields; adjust if you have additional fields.
#             writer.writerow(['Timestamp', 'Date', 'Time', 'CO(GT)', 'PT08.S1(CO)', 'NMHC(GT)',
#                              'C6H6(GT)', 'PT08.S2(NMHC)', 'NOx(GT)', 'PT08.S3(NOx)', 'NO2(GT)',
#                              'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'])
#             file.flush()  # Ensure header is written immediately
    
                            
#     # For loop to continuously read messages from the topic
#     for message in consumer:
#         try:
#             data = message.value
#             #Logging the received record 
#             logging.info("Received record: %s", data)
#             row = [
#                     data.get('Date'),
#                     data.get('Time'),
#                     data.get('CO(GT)'),
#                     data.get('PT08.S1(CO)'),
#                     data.get('NMHC(GT)'),
#                     data.get('C6H6(GT)'),
#                     data.get('PT08.S2(NMHC)'),
#                     data.get('NOx(GT)'),
#                     data.get('PT08.S3(NOx)'),
#                     data.get('NO2(GT)'),
#                     data.get('PT08.S4(NO2)'),
#                     data.get('PT08.S5(O3)'),
#                     data.get('T'),
#                     data.get('RH'),
#                     data.get('AH')
#                 ]
#             writer.writerow(row)
#             file.flush()  # Flush after each write to ensure data is written
#         except Exception as e:
#             logging.error("Error processing received message: %s", e)

# if __name__ == "__main__":
#     main()

import csv
import datetime
import logging
import os
import sys
import json
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    kafka_bootstrap_servers = ['localhost:9092']
    topic_name = 'air_quality_topic'
    csv_file = 'air_quality_streamed.csv'
    
    # Initialize Kafka consumer with JSON deserialization
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info("Kafka Consumer connected and subscribed to topic '%s'.", topic_name)
    except Exception as e:
        logging.error("Failed to connect Kafka consumer: %s", e)
        sys.exit(1)
    
    # Determine if header needs to be written by checking if file exists
    write_header = not os.path.exists(csv_file)
    
    # Open CSV file for appending data (remains open throughout the consumer loop)
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        
        if write_header:
            # Write header based on dataset fields
            writer.writerow(['Consumer_Timestamp','Producer_timestamp', 'Date', 'Time','Datetime','Record Index', 'CO(GT)', 'PT08.S1(CO)', 'NMHC(GT)',
                             'C6H6(GT)', 'PT08.S2(NMHC)', 'NOx(GT)', 'PT08.S3(NOx)', 'NO2(GT)',
                             'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'])
            file.flush()  # Ensure header is written immediately
        
        # For loop to continuously read messages from the topic
        for message in consumer:
            try:
                data = message.value
                # Log the received record
                logging.info("Received record: %s", data)
                # Add a timestamp to each record
                _consumer_timestamp = datetime.datetime.now().isoformat()
                row = [
                    _consumer_timestamp,
                    data.get('_producer_timestamp'),
                    data.get('Date'),
                    data.get('Time'),
                    data.get('datetime_iso'),
                    data.get('_record_index'),
                    data.get('CO(GT)'),
                    data.get('PT08.S1(CO)'),
                    data.get('NMHC(GT)'),
                    data.get('C6H6(GT)'),
                    data.get('PT08.S2(NMHC)'),
                    data.get('NOx(GT)'),
                    data.get('PT08.S3(NOx)'),
                    data.get('NO2(GT)'),
                    data.get('PT08.S4(NO2)'),
                    data.get('PT08.S5(O3)'),
                    data.get('T'),
                    data.get('RH'),
                    data.get('AH')
                ]
                writer.writerow(row)
                file.flush()  # Flush after each write to ensure data is written
            except Exception as e:
                logging.error("Error processing received message: %s", e)

if __name__ == "__main__":
    main()
