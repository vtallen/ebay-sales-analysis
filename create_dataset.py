import argparse
from datetime import datetime

import pandas as pd
import glob
import os.path
import json
from typing import List, Callable

# TODO:
# - Add parsing of sold items to sum how many of each type of film was sold in each timeframe


def find_quantity(title: str, regex: List[str]) -> int:
    return 0
    pass


def find_roll_type(title: str, regex: List[str]) -> str:
    return ""


def load_csvs(folder: str) -> pd.DataFrame:
    result: pd.DataFrame = pd.DataFrame()

    path: str = os.path.abspath(folder)

    filenames: list[str] = glob.glob(path + "/*.csv")

    for filename in filenames:
        # Skip the header lines ebay puts in its report exports
        df = pd.read_csv(filename, skiprows=11)

        # We are only interested in orders, not in bank account payouts
        df = df.drop(df[df["Type"] != "Order"].index)  # pyright: ignore

        # Concatenate the dataframes into one big dataframe
        result = pd.concat([result, df], ignore_index=True)

    # Remove any duplicate rows, then convert the date columns to dates
    # Finally, sort the data by date
    result.drop_duplicates(subset=["Order number"], inplace=True)
    result["Transaction creation date"] = pd.to_datetime(
        result["Transaction creation date"]
    )
    result.sort_values(by=["Transaction creation date"], inplace=True)

    return result


def compare_daily(current_date: datetime, row_date: datetime) -> bool:
    return current_date != row_date


def compare_monthly(current_date: datetime, row_date: datetime) -> bool:
    return current_date.month != row_date.month


def compare_weekly(current_date: datetime, row_date: datetime) -> bool:
    return current_date.isocalendar()[1] != row_date.isocalendar()[1]


def fill_row(
    month: int,
    day: int,
    year: int,
    weekday: int,
    week_number: int,
    sales: float,
    rolls_xx: int,
    rolls_250D: int,
    rolls_500T: int,
) -> dict:
    return {
        "month": str(month),
        "day": str(day),
        "year": str(year),
        "weekday": str(weekday),
        "week number": str(week_number),
        "sales": sales,
        "xx rolls sold": str(rolls_xx),
        "250D rolls sold": str(rolls_250D),
        "500T rolls sold": str(rolls_500T),
    }


def create_daily_sales_dataset(
    rawdata: pd.DataFrame, compare_func: Callable[[datetime, datetime], bool]
) -> pd.DataFrame:

    result: pd.DataFrame = pd.DataFrame(
        {
            "month": [],
            "day": [],
            "year": [],
            "weekday": [],
            "week number": [],
            "sales": [],
            "xx rolls sold": [],
            "250D rolls sold": [],
            "500T rolls sold": [],
        }
    )

    current_date = None  # pyright : ignore
    current_sum = 0

    for index, row in rawdata.iterrows():
        row_date = row["Transaction creation date"]
        if current_date is None:
            current_date = row_date

        if compare_func(current_date, row_date):  # pyright: ignore
            result = result._append(
                fill_row(
                    current_date.month,  # pyright: ignore
                    current_date.day,  # pyright: ignore
                    current_date.year,  # pyright: ignore
                    current_date.weekday(),  # pyright: ignore
                    current_date.isocalendar()[1],  # pyright: ignore
                    current_sum,
                    0,
                    0,
                    0,
                ),
                ignore_index=True,
            )

            # Clear the counters and set the initial sales sum
            current_date = row_date
            current_sum = 0.0

        try:
            current_sum += float(row["Item subtotal"])
        except ValueError:
            print("no value for item subtotal in row" + str(index))

    result = result._append(
        fill_row(
            current_date.month,  # pyright: ignore
            current_date.day,  # pyright: ignore
            current_date.year,  # pyright: ignore
            current_date.weekday(),  # pyright: ignore
            current_date.isocalendar()[1],  # pyright: ignore
            current_sum,
            0,
            0,
            0,
        ),
        ignore_index=True,
    )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="dataset generator")
    parser.add_argument("-d", "--dir", type=str, default="./")

    args = parser.parse_args()

    all_data: pd.DataFrame = load_csvs(args.dir)

    all_data.to_csv("raw_data_combined.csv", float_format="%.2f", index=False)

    daily_sales = create_daily_sales_dataset(all_data, compare_daily)
    daily_sales.to_csv("daily.csv", float_format="%.2f", index=False)
    daily_sales.head()

    monthly_sales = create_daily_sales_dataset(all_data, compare_monthly)
    monthly_sales.to_csv("monthly.csv", float_format="%.2f", index=False)

    weekly_sales = create_daily_sales_dataset(all_data, compare_weekly)
    weekly_sales.to_csv("weekly.csv", float_format="%.2f", index=False)
