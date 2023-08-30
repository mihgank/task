import pandas as pd


class CsvWorker:
    """
    Representation of csv file with data
    """

    def time_filter(self, date: str):
        """
        Return filtered dataframe (arg don't mutate)
        """

        day_start = pd.Timestamp(f"{date} 00:00:00")
        day_end = day_start + pd.Timedelta(days=1)

        return self.df[
            self.df.apply(
                lambda row: pd.to_datetime(row["timestamp"]) >= day_start
                and pd.to_datetime(row["timestamp"]) < day_end,
                axis=1,
            )
        ]

    def __init__(self, filepath: str, datatypes=None):
        if datatypes:
            self.df = pd.read_csv(filepath, dtype=datatypes)
        else:
            self.df = pd.read_csv(filepath, parse_dates=["timestamp"])
