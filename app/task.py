import pandas as pd
from memory_profiler import profile

from .csvworker import CsvWorker
from .datatypes import client_datatype, server_datatype
from .sqlworker import SqlWorker


@profile
def main_task():
    database = SqlWorker("private\cheaters.db")
    database.create_table("result")

    # get data from files
    filter_time = input("Time for filtering (like 2021-04-02): ")
    server_df = CsvWorker("private\server.csv", server_datatype).time_filter(
        filter_time
    )
    client_df = CsvWorker("private\client.csv", client_datatype).time_filter(
        filter_time
    )

    cheaters_df = database.get_dataframe("cheaters")

    # merge all needed fields
    merged_df = pd.merge(
        server_df,
        client_df,
        on="error_id",
        how="inner",
        suffixes=("_server", "_client"),
    ).merge(cheaters_df, on="player_id", how="left")

    # exclude rows
    merged_df = merged_df.drop(
        merged_df[
            (merged_df["ban_time"].isnull())
            & (
                pd.to_datetime(merged_df["timestamp_server"], unit="s")
                - pd.to_datetime(merged_df["ban_time"])
                > pd.Timedelta(days=1)
            )
        ].index
    )

    merged_df.rename(
        columns={
            "timestamp_server": "timestamp",
            "description_server": "json_server",
            "description_client": "json_client",
        },
        inplace=True,
    )

    selected_columns = [
        "timestamp",
        "player_id",
        "error_id",
        "json_server",
        "json_client",
    ]

    database.insert_to_table("result", merged_df[selected_columns])
