Phase 1 Documentation: Kafka Setup and Data Streaming

1. Kafka Setup Process

1.1 Installation and Configuration

Apache Kafka in KRaft Mode:
To simplify cluster management, Kafka was configured to run in KRaft (Kafka Raft Metadata mode), which eliminates the need for ZooKeeper. Kafka (v3.0.0 or newer) was downloaded and installed on the development environment.

The Kafka broker is started using the KRaft mode configuration (e.g., by running bin/kafka-server-start.sh config/kraft/server.properties).

The environment was verified by checking the broker logs and ensuring the broker is listening on the default port (9092).

Topic Creation:
A dedicated topic named air_quality_topic was created for streaming air quality data:
bin/kafka-topics.sh --create --topic air_quality_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

1.2 Producer and Consumer Implementation

Producer Implementation:
A Python-based Kafka producer was developed using the kafka-python library. Key functionalities include:

Data Reading: The UCI Air Quality dataset is read from a CSV file using Pandas.

Preprocessing:
The producer code implements data preprocessing (detailed in the next md doc) to handle missing values and standardize datetime formats.

Real-Time Simulation:
A delay (time.sleep(1)) is implemented between sending records to simulate hourly sensor readings.

Error Handling and Logging:
The producer uses try/except blocks for error handling and logs key events (successful record transmissions, errors) using Python’s logging module.

Consumer Implementation:
Similarly, a Kafka consumer was developed to continuously read messages from the air_quality_topic. The consumer:

Deserializes incoming JSON messages.

Processes the data (e.g., stores messages in a CSV file for later analysis).

Implements error handling to log and manage any message processing issues.

1.3 Challenges and Resolutions
Dependency Issues:
During initial setup, issues with installing kafka-python were encountered in the development environment (e.g., in WSL). This was resolved by ensuring Python 3 and the necessary development packages (python3-dev, build-essential) were installed, and by creating an isolated virtual environment.

Environment Configuration for Kafka:
Ensuring Kafka runs in KRaft mode (and not the legacy ZooKeeper mode) required updating configuration files and command-line options. Logs were carefully reviewed to confirm that the broker was properly configured and that topics were successfully created.

Data Ingestion and Robustness:
The producer script was updated with additional error handling and logging to ensure that if a record fails to send, the error is logged, and the script continues processing subsequent messages. This robustness is key in a real-time streaming environment.

Reference: Solutions were informed by guidance from ChatGPT when diagnosing pip installation errors and handling environment-specific configuration challenges.