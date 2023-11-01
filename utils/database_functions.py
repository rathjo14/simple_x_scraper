import sqlite3
from typing import Tuple, List, Any

class SqliteTableObject():
    def __init__(self, database_filepath : str, table : str, columns : List[str], types : List[str], primary_keys : List[str] = None):
        self.database_filepath = database_filepath
        self.table = table
        self.columns = columns
        self.types = types
        if len(self.columns)!= len(self.types):
            print("Error len of cols != len of types.")
            raise
        self.primary_keys = primary_keys
        self.connection, self.cursor = self.get_connection()
        self.create()


    def get_connection(self):
        connection = sqlite3.connect(self.database_filepath)
        cursor = connection.cursor()
        return connection, cursor
    
    def create(self):
        column_type_string = ", ".join(["{} {}".format(self.columns[idx], self.types[idx]) for idx in range(len(self.columns))])
        primary_key_string = "PRIMARY KEY ({})".format(",".join(self.primary_keys))
        create_statement = "CREATE TABLE IF NOT EXISTS {} ({}, {})".format(self.table, column_type_string, primary_key_string)
        self.cursor.execute(create_statement)
        self.connection.commit()

    def insert(self, row : List[Any]):
        upsert_statement = "ON CONFLICT({}) DO UPDATE SET {}".format(",".join(self.primary_keys), ",".join([column + "=excluded." + column for column in self.columns]))
        insert_statement = "INSERT INTO {}({}) VALUES({}) {}".format(self.table, ",".join(self.primary_keys), ",".join(["?"]*len(self.columns)), upsert_statement)
        self.cursor.execute(insert_statement, row)
        self.connection.commit()

    def insert_many(self, row : List[Tuple[Any]]):
        upsert_statement = "ON CONFLICT({}) DO UPDATE SET {}".format(",".join(self.primary_keys), ",".join([column + "=excluded." + column for column in self.columns]))
        insert_statement = "INSERT INTO {}({}) VALUES({}) {}".format(self.table, ",".join(self.primary_keys), ",".join(["?"]*len(self.columns)), upsert_statement)
        self.cursor.executemany(insert_statement, row)
        self.connection.commit()  
