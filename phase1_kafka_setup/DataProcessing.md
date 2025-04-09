Data Preprocessing Strategy

2.1 Preprocessing Objectives
Given that the UCI Air Quality dataset includes 9,358 hourly records with 15 columns (including date/time and pollutant measurements) and has missing values marked as –200, the preprocessing strategy focused on:

Consistency: Ensuring that the date and time information is correctly merged and sorted.

Handling Missing Values: Replacing –200 values with None to allow libraries such as Pandas to correctly identify nulls and apply imputation.

Standardization: Converting date and time fields into standardized ISO formats (ISO 8601) to facilitate downstream conversion using functions like pd.to_datetime().

2.2 Implementation Details
Missing Value Handling:
The producer applies a preprocessing function preprocess_record(record). For each record:

Each field is checked: if the value equals –200 (either as a string or numeric), it is replaced with None.

This conversion helps in ensuring that subsequent analysis or statistical modeling (e.g., when performing forward-fill imputation) correctly recognizes missing data.

Datetime Standardization:
Within the preprocessing function:

The separate Date and Time fields are trimmed and parsed.

A new field datetime_iso is created in ISO 8601 format (YYYY-MM-DDTHH:MM:SS) to standardize timestamps.

Ensuring Data Integrity:
Preprocessing is applied on the producer side so that each record streamed into Kafka is already cleansed. This prevents errors later in the pipeline and supports the overall integrity of the data used for downstream analysis and modeling.

2.3 Justification
This preprocessing approach was chosen because:

It maintains the continuity of the time series data by handling missing values early.

The standardization of date and time ensures that downstream tools in Python, such as Pandas and time series libraries, can efficiently parse and manipulate the data.

Converting –200 values to None enables intuitive imputation techniques and avoids misinterpretation of missing data as legitimate measurements in statistical analyses.

Reference: The preprocessing strategy was refined with the help of ChatGPT to ensure that missing values and inconsistent datetime formats were handled robustly