import argparse
from datetime import datetime

import pandas as pd
import glob
import os.path
import json
import re
from typing import List, Callable, Dict, Any
import logging

logger = logging.getLogger(__name__)

# TODO:
# - Add parsing of sold items to sum how many of each type of film was sold in each timeframe
# - Add cmd line arguments for daily, weekly, monthly
# - Add cmd line argument for logging file name


# finds how many rolls a sold listing had in it
def regex_multi_search(title: str, regex_dict: Dict[str, List[str]]):
    for result_str in regex_dict.keys():
        for reg_str in regex_dict[result_str]:
            found = re.search(reg_str, title)
            if found:
                return result_str

    return None


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


def zero_dict(dictionary: Dict[str, int]):
    for key in dictionary.keys():
        dictionary[key] = 0


def create_timeframe_dataset(
    rawdata: pd.DataFrame,
    compare_func: Callable[[datetime, datetime], bool],
    quantity_regex_dict: Dict[str, List[str]],
    type_regex_dict: Dict[str, List[str]],
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
    current_sales = 0
    current_roll_sales = {
        "XX": 0,
        "500T": 0,
        "250D": 0,
    }

    for index, row in rawdata.iterrows():
        row_date = row["Transaction creation date"]
        if current_date is None:
            current_date = row_date

        if compare_func(current_date, row_date):  # pyright: ignore
            roll_type = regex_multi_search(str(row["Item title"]), item_type_regex_dict)
            quantity = regex_multi_search(str(row["Item title"]), quantity_regex_dict)

            if roll_type == None:
                print(
                    "Error: could not determine roll type. Title:",
                    row["Item title"],
                )
                continue

            if roll_type in ("XX100", "500T100", "250D100"):
                roll_type = roll_type[:-3]
                quantity = 16

            if quantity == None:
                print(
                    "Error: could not determine roll quantity. Title:",
                    row["Item title"],
                )
                print("\t\t\t", roll_type, quantity)
                continue

            current_roll_sales[roll_type] += int(quantity)

            result = result._append(
                fill_row(
                    current_date.month,  # pyright: ignore
                    current_date.day,  # pyright: ignore
                    current_date.year,  # pyright: ignore
                    current_date.weekday(),  # pyright: ignore
                    current_date.isocalendar()[1],  # pyright: ignore
                    current_sales,
                    0,
                    0,
                    0,
                ),
                ignore_index=True,
            )

            # Clear the counters and set the initial sales sum
            current_date = row_date
            current_sum = 0.0
            zero_dict(current_roll_sales)

        try:
            current_sales += float(row["Item subtotal"])
        except ValueError:
            print("no value for item subtotal in row" + str(index))

    result = result._append(
        fill_row(
            current_date.month,  # pyright: ignore
            current_date.day,  # pyright: ignore
            current_date.year,  # pyright: ignore
            current_date.weekday(),  # pyright: ignore
            current_date.isocalendar()[1],  # pyright: ignore
            current_sales,
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

    quantity_regex_dict = {
        "2": ["2 36exp"],
        "3": ["3x", "3 36exp rolls", "3 24exp"],
        "4": ["4 36exp"],
        "5": ["5 36exp", "5 24exp"],
        "6": ["6x", "6 36exp rolls"],
        "10": ["10x", "10 36exp rolls"],
        "15": ["15x 36 exposure"],
        # For the sake of my profit calculations, a 100' bulk roll of 35mm cinema film yields 16 36 exp casettes of film
        "16": ["XX 100' roll", "500T 100' roll", "250D 100' roll"],
    }

    # This bit feels a little sketchy. My regex_multi_search function returns the first match it finds in the list of
    # regex expressions. As such, if an item title will match multiple regex expressions, then the most specific one
    # needs to appear first in the list or unexpected output will result
    item_type_regex_dict = {
        "XX100": ["XX 100' roll"],
        "500T100": ["500T 100' roll"],
        "250D100": ["250D 100' roll"],
        "XX": ["Kodak Double X"],
        "500T": ["Kodak Vision3 500T"],
        "250D": ["Kodak Vision3 250D"],
    }

    daily_sales = create_timeframe_dataset(
        all_data, compare_daily, quantity_regex_dict, item_type_regex_dict
    )
    daily_sales.to_csv("daily.csv", float_format="%.2f", index=False)
    daily_sales.head()

    monthly_sales = create_timeframe_dataset(
        all_data, compare_monthly, quantity_regex_dict, item_type_regex_dict
    )
    monthly_sales.to_csv("monthly.csv", float_format="%.2f", index=False)

    weekly_sales = create_timeframe_dataset(
        all_data, compare_weekly, quantity_regex_dict, item_type_regex_dict
    )
    weekly_sales.to_csv("weekly.csv", float_format="%.2f", index=False)

    print(
        regex_multi_search(
            "35mm Color Negative Film - 6 36exp rolls - Kodak Vision3 500T",
            item_type_regex_dict,
        )
    )
