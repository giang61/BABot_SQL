import csv
import sqlite3
import os
import re  # To sanitize table names

def csv_to_sqlite(csv_file, db_file, encoding='utf-8', delimiter=None):
    # Check if the CSV file exists
    if not os.path.isfile(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        return

    # If the delimiter is not specified, try to detect it
    if delimiter is None:
        try:
            with open(csv_file, 'r', encoding=encoding, errors='ignore') as file:
                sample = file.read(4096)  # Read a sample of the file to guess the delimiter
                delimiter = csv.Sniffer().sniff(sample).delimiter
            print(f"Detected delimiter: '{delimiter}'")
        except Exception as e:
            print(f"Error detecting delimiter: {e}. Defaulting to ',' (comma).")
            delimiter = ','  # Default delimiter

    # Connect to SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Read the CSV file
    try:
        with open(csv_file, 'r', encoding=encoding, errors='ignore') as file:
            csv_reader = csv.reader(file, delimiter=delimiter)
            headers = next(csv_reader)  # Get column names from the first row

            # Clean headers to make them SQL-compliant
            headers = [header.strip().replace(" ", "_").replace("-", "_").replace(".", "_") for header in headers]

            # Sanitize the table name to ensure it's valid in SQLite
            table_name = os.path.splitext(os.path.basename(csv_file))[0]
            table_name = re.sub(r'\W+', '_', table_name)  # Replace invalid characters with underscores
            print(f"Sanitized table name: {table_name}")

            # Infer data types by analyzing sample rows
            data_types = infer_column_types(csv_reader, len(headers))
            print(f"Inferred data types: {data_types}")

            # Create a table with appropriate columns and inferred types
            create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{header} {data_type}' for header, data_type in zip(headers, data_types)])})"""
            cursor.execute(create_table_sql)

            # Reset reader to start inserting rows
            file.seek(0)
            next(csv_reader)  # Skip headers again

            # Insert data row by row, respecting missing values
            insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['?' for _ in headers])})"
            for row in csv_reader:
                # Pad or truncate rows to match header count
                row = row[:len(headers)] + [None] * (len(headers) - len(row))
                # Replace empty strings with None for proper NULL handling
                processed_row = [None if value.strip() == '' else value for value in row]
                cursor.execute(insert_sql, processed_row)

        # Commit changes and close connection
        conn.commit()
        print(f"CSV data has been successfully imported into {db_file}")

    except Exception as e:
        print(f"Error during import: {e}")

    finally:
        conn.close()


def infer_column_types(csv_reader, num_columns, sample_size=100):
    """
    Infer data types for each column based on a sample of the data.

    Args:
        csv_reader (iterator): CSV reader object.
        num_columns (int): Number of columns in the CSV.
        sample_size (int): Number of rows to sample.

    Returns:
        list: A list of inferred data types for each column.
    """
    column_types = ["INTEGER"] * num_columns  # Start assuming all columns are INTEGER

    for _ in range(sample_size):
        try:
            row = next(csv_reader)
        except StopIteration:
            break  # End of file

        for i, value in enumerate(row):
            if value.strip() == '':
                continue  # Skip empty values
            if column_types[i] == "TEXT":
                continue  # Once TEXT, it stays TEXT
            try:
                int(value)
                continue  # Still INTEGER
            except ValueError:
                try:
                    float(value)
                    column_types[i] = "REAL"  # Upgrade to REAL
                except ValueError:
                    column_types[i] = "TEXT"  # Upgrade to TEXT

    return column_types


# Example usage
#csv_to_sqlite('data/point_virgule/p2-arbres-fr.csv', 'data/temp/toto.sqlite', delimiter=';')
