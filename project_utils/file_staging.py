import tkinter as tk
from tkinter import filedialog
import pandas as pd
import chardet
import psycopg2
from psycopg2 import sql
from datetime import datetime
import re

def snake_case(s):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

def determine_file_type(file_path):
    return 'xlsx' if file_path.endswith('.xlsx') else 'csv'

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read()
    return chardet.detect(raw_data)['encoding']

def read_file(file_path, file_type, encoding):
    if file_type == 'xlsx':
        return pd.read_excel(file_path)
    else:
        return pd.read_csv(file_path, encoding=encoding)

def prompt_user(prompt):
    return input(prompt)

def identify_columns(df):
    print(df.head())
    i_col = prompt_user("Enter the column name for 'i': ")
    source_col = prompt_user("Enter the column name for 'source': ")
    supplier_name_col = prompt_user("Enter the column name for 'supplier_name': ")
    return i_col, source_col, supplier_name_col

def rename_columns(df, i_col, source_col, supplier_name_col):
    rename_dict = {i_col: 'i', source_col: 'source', supplier_name_col: 'supplier_name'}
    df = df.rename(columns=rename_dict)
    df.columns = [snake_case(col) if col not in ['i', 'source', 'supplier_name'] else col for col in df.columns]
    return df

def create_table_and_import(df, customer, project):
    conn = psycopg2.connect("postgresql://overlord:password@localhost:5432/csu")
    cur = conn.cursor()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"{customer}_{project}_source_{timestamp}"

    # Create table
    columns = [f"{col} TEXT" for col in df.columns]
    create_table_query = sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.SQL, columns))
    )
    cur.execute(create_table_query)

    # Strip spaces from column names
    df.columns = [col.strip() for col in df.columns]

    # Import data
    for _, row in df.iterrows():
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, df.columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
        )
        cur.execute(insert_query, tuple(row))

    conn.commit()
    cur.close()
    conn.close()

def main():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")])
    if not file_path:
        print("No file selected. Exiting.")
        return

    file_type = determine_file_type(file_path)
    encoding = detect_encoding(file_path)
    df = read_file(file_path, file_type, encoding)

    customer = prompt_user("Enter customer name: ")
    project = prompt_user("Enter project name: ")

    i_col, source_col, supplier_name_col = identify_columns(df)
    df = rename_columns(df, i_col, source_col, supplier_name_col)

    create_table_and_import(df, customer, project)
    print(f"Data imported successfully to table: {customer}_{project}_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

if __name__ == "__main__":
    main()