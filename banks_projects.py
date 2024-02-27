# Importing the required libraries
import pandas as pd
import numpy as np
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

# Function to log progress with timestamp


def log_progress(msg):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timeformat)

    with open(logfile, 'a') as f:
        f.write(timestamp + ' : ' + msg + '\n')

# Function to extract data from a given URL and parse HTML


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    tables = data.find_all('tbody')[0]
    rows = tables.find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            anchor_data = col[1].find_all('a')[1]
            if anchor_data is not None:
                data_dict = {
                    'Name': anchor_data.contents[0],
                    'MC_USD_Billion': col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    # Cleaning and converting data
    USD_list = list(df['MC_USD_Billion'])
    USD_list = [float(''.join(x.split('\n'))) for x in USD_list]
    df['MC_USD_Billion'] = USD_list

    return df

# Function to transform data using exchange rates


def transform(df, exchange_rate_path):
    csvfile = pd.read_csv(exchange_rate_path)

    # creating dictionary that corresponding currencies are mapping to their  rate values
    exchange_rate_dict = csvfile.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [
        np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [
        np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [
        np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]

    return df

# Function to save DataFrame to CSV


def load_to_csv(df, output_path):
    df.to_csv(output_path)

# Function to save DataFrame to SQLite database


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Function to execute and print SQL queries


def run_query(query_statements, sql_connection):
    for query in query_statements:
        print(f"Query: {query}")
        query_output = pd.read_sql(query, sql_connection)

        # Check if the query result is a DataFrame
        if isinstance(query_output, pd.DataFrame):
            with pd.option_context('display.max_columns', None):
                print(query_output, '\n')
        else:
            print("Result:", query_output.iloc[0, 0]
                  if not query_output.empty else None, '\n')


# Define required entities and initiate the ETL process
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path = 'exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(db_name)
query_statements = [
    'SELECT * FROM Largest_banks',
    'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
    'SELECT Name from Largest_banks LIMIT 5'
]
logfile = 'code_log.txt'
output_csv_path = 'Largest_banks_data.csv'

# Log the start of the ETL process
log_progress('Preliminaries complete. Initiating ETL process.')

# Extract data from the URL
df = extract(url, table_attribs)

# Log the completion of data extraction
log_progress('Data extraction complete. Initiating Transformation process.')

# Transform data using exchange rates
df = transform(df, exchange_rate_path)

# Log the completion of data transformation
log_progress('Data transformation complete. Initiating loading process.')

# Save data to CSV file
load_to_csv(df, output_csv_path)

# Log the completion of saving data to CSV
log_progress('Data saved to CSV file.')

# Log the initiation of SQL connection
log_progress('SQL Connection initiated.')

# Save data to SQLite database and execute queries
load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as table. Executing queries.')

# Execute predefined SQL queries
run_query(query_statements, conn)

# Log the completion of the entire process
log_progress('Process Complete.')

# Close the SQL connection
conn.close()
log_progress('Server Connection closed')
