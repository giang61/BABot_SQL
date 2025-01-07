import pandas as pd
import sqlite3
import os
import re


def excel_to_sqlite(excel_file, db_file):
    """
    Imports all sheets of an Excel file into an SQLite database, with data type inference.

    Parameters:
        excel_file (str): Path to the Excel file.
        db_file (str): Path to the SQLite database file.
    """
    # Check if Excel file exists
    if not os.path.isfile(excel_file):
        print(f"Error: Excel file '{excel_file}' not found.")
        return

    # Connect to SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        # Read all sheets from the Excel file into a dictionary of DataFrames
        excel_sheets = pd.read_excel(excel_file, sheet_name=None)  # `sheet_name=None` loads all sheets

        # Loop through each sheet
        for sheet_name, df in excel_sheets.items():
            print(f"Processing sheet: {sheet_name}")

            # Clean column headers
            headers = list(df.columns)
            headers = [
                re.sub(r'^(\d+)', '', header)  # Replace leading digits with ''
                for header in headers
            ]
            headers = [re.sub(r'\W+', '_', header) for header in headers]  # Replace non-alphanumeric characters
            df.columns = headers  # Update DataFrame column names

            # Sanitize table name (combine Excel file name and sheet name, replacing non-alphanumeric characters)
            base_name = os.path.splitext(os.path.basename(excel_file))[0]
            table_name = f"{base_name}_{sheet_name}"
            table_name = re.sub(r'\W+', '_', table_name).replace('temp_', '')
            print(f"Table name: {table_name}")

            # Infer column types and create the table
            column_types = infer_sqlite_column_types(df)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_table_sql += ', '.join([f"{header} {col_type}" for header, col_type in zip(headers, column_types)]) + ")"
            cursor.execute(create_table_sql)

            # Insert data into table
            insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['?' for _ in headers])})"

            # Convert empty strings or NaN values to None (NULL in SQLite)
            df = df.where(pd.notnull(df), None)
            cursor.executemany(insert_sql, df.values.tolist())

        # Commit changes after processing all sheets
        conn.commit()
        print(f"All sheets from '{excel_file}' have been successfully imported into '{db_file}'")

    except Exception as e:
        print(f"Error during import: {e}")

    finally:
        print("Closing connection")
        conn.close()


def infer_sqlite_column_types(df):
    """
    Infers SQLite-compatible column types from a pandas DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to infer types from.

    Returns:
        list: A list of SQLite column types (`TEXT`, `INTEGER`, `REAL`).
    """
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'REAL',
        'object': 'TEXT',
        'bool': 'INTEGER',  # SQLite has no BOOLEAN type, so we use INTEGER
        'datetime64[ns]': 'TEXT',  # Store dates as TEXT in ISO 8601 format
    }

    column_types = []
    for dtype in df.dtypes:
        sqlite_type = type_mapping.get(str(dtype), 'TEXT')  # Default to TEXT for unknown types
        column_types.append(sqlite_type)

    return column_types


# Example usage:
#excel_to_sqlite("data/excel/Liste de tâches.xlsx", "data/temp/output.sqlite")
