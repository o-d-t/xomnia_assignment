import sqlite3
from typing import List

import pandas as pd


class ShippySQLite:
    """Database abstraction for Shippy application, residing as sqllite3 db.

    pd.Dataframe is used for input and output data type.
    The class exposes three methods:
    * read_table_to_df(table_name)
    * write_df_to_table(df, table_name, if_exists)
    * read_raw_csv(csv, if_exists)

    """

    def __init__(self,
                 database_name: str = 'shippy/db/shippydb.sqlite'):
        """ Initialize ShippyDB object.

        Args:
            database_name: Name of the database to initialize, default 'shippydb.sqlite'.

        """
        self.con = sqlite3.connect(database_name)

    def read_table_to_df(self,
                         table_name: str,
                         parse_dates: List[str] = None) -> pd.DataFrame:
        """Read raw data from the database.

        Args:
            table_name: Name of the table to read.
            parse_dates: Specify the column(s) to parse as datetime, default None.

        Returns:
            Table contents as pandas dataframe.

        """
        return pd.read_sql_query(f"SELECT * FROM {table_name}", self.con, parse_dates=parse_dates)

    def write_df_to_table(self,
                          df: pd.DataFrame,
                          table_name: str,
                          if_exists: str = 'append'):
        """Write data to database.

        Args:
            df: Dataframe to write to the database.
            table_name: Name of the table in database that data will be written into.
            if_exists: Writing behavior.

        Returns:
            None

        """
        df.to_sql(table_name, self.con, if_exists=if_exists)
