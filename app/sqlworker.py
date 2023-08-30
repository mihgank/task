import sqlite3

import pandas as pd


class SqlWorker:

    """
    Controller for SQLite database
    """

    def create_table(self, tablename: str) -> None:
        """
        Create table if it doesn't exist
        """
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {tablename}(
            player_id INTEGER PRIMARY KEY,
            timestamp TEXT,
            event_id INTEGER,
            error_id INTEGER,
            json_server TEXT,
            json_client TEXT
          )"""
        )

    def insert_to_table(self, tablename: str, df: pd.DataFrame):
        df.to_sql(tablename, self.connection, if_exists="replace", index=False)

    def get_dataframe(self, tablename: str):
        """
        Get dataframe from the table in connected database
        """
        try:
            df = pd.read_sql_query(f"SELECT * FROM {tablename}", self.connection)
        except sqlite3.DatabaseError as db_err:
            print(db_err)
        return df

    def __init__(self, filepath: str):
        self.connection = sqlite3.connect(filepath)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()
