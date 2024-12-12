import csv
import sqlite3
import os

def csv_to_sqlite(csv_file, db_file, encoding='utf-8'):
    # Check if CSV file exists
    if not os.path.isfile(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        return
    # Detect delimiter automatically
    try:
        with open(csv_file, 'r', encoding=encoding, errors='ignore') as file:
            sample = file.read(1024)  # Read a sample of the file to guess the delimiter
            detected_delimiter = csv.Sniffer().sniff(sample).delimiter
    except Exception as e:
        print(f"Error detecting delimiter: {e}")
        return

    # Connect to SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Read CSV file
    try:
        with open(csv_file, 'r', encoding=encoding, errors='ignore') as file:
            csv_reader = csv.reader(file, delimiter=detected_delimiter)
            headers = next(csv_reader)  # Get column names from the first row

            # Remove spaces from headers
            headers = [header.replace(" ", "_") for header in headers]


            # Create table
            table_name = os.path.splitext(os.path.basename(csv_file))[0]  # Use CSV filename as table name
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{header} TEXT' for header in headers])})"
            cursor.execute(create_table_sql)

            # Insert data
            insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['?' for _ in headers])})"
            cursor.executemany(insert_sql, csv_reader)

        # Commit changes and close connection
        conn.commit()
        print(f"CSV data has been successfully imported into {db_file}")

    except Exception as e:
        print(f"Error during import: {e}")

    finally:
        conn.close()

#csv_to_sqlite(r'C:\Users\bob\PycharmProjects\BABot\foo.csv', 'toto.sqlite')
