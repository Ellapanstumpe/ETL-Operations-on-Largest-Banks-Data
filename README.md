# ETL-Operations-on-Largest-Banks-Data
This project is for Extract, Transform, and Load (ETL) operations on data related to the largest banks. The data is extracted from a Wikipedia page, transformed by converting currency values using exchange rates, and loaded into a SQLite database. Meanwhile, maintains logs to track progress during different stages.

Usage

Follow the steps below to use and understand the provided code:

Dependencies Installation:

Install the required Python libraries by running:

python3.11 -m pip install pandas
python3.11 -m pip install numpy
python3.11 -m pip install bs4

Exchange Rate Data:

Ensure you have a CSV file (exchange_rate.csv) with exchange rates for currencies. You can run follow command on your terminal to retreive the file :
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

Run the Code:

Execute the Python script etl_operations.py to initiate the ETL process.

Logs and Outputs:

Check the log file (code_log.txt) for progress updates and timestamped messages.
The transformed data is saved to a CSV file (Largest_banks_data.csv).
The data is also loaded into a SQLite database (Banks.db) with the table name Largest_banks.
SQL Queries:

Predefined SQL queries are executed at the end of the process. Check the log for query outputs.

Files

etl_operations.py: The main Python script for ETL operations.
exchange_rate.csv: CSV file containing currency exchange rates.
Largest_banks_data.csv: Output CSV file with transformed data.
Banks.db: SQLite database containing the loaded data.
code_log.txt: Log file capturing progress updates.

Acknowledgments

This code was developed to demonstrate ETL processes on web-scraped banking data. Feel free to modify and adapt it to your specific use case.

License
This project is licensed under the MIT License - see the LICENSE file for details.

