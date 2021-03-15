import pandas as pd
from datetime import datetime
from random import uniform, randint
import pandas as pd
import uuid


def get_date_formats(
    start_date: str, end_date: str, freq: str = "M", sep: str = ""
) -> list:
    """[summary]
    Aim: To get the dates in the requested format

    Sample:
        M : [2020-01-31, 2020-02-29(28)]
        MS : [2020-01-01, 2020-02-01]
        sep: 
            "" -> [20200101, 20200201]
            "_" -> [2020_01_01, 2020_02_01]

    Args:
        start_date (str): start date, example: 2020-01-01
        end_date (str): end date, example: 2021-01-01
        freq (str, optional): Defaults to "M".
        sep (str, optional):  Defaults to "".

    Returns:
        list: [description]
    """
    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    dates_list = pd.date_range(date_start, date_end, freq=freq).to_list()

    final_dates = [
        sep.join(str(month_start.date()).split("-")) for month_start in dates_list
    ]
    return final_dates


def generate_data(n: int, date_of_service: str) -> dict:
    """[summary]
    Aim: to get the data generated with n number of records and for a date

    Args:
        n (int): Takes a number to get the number of records. Example n = 20
        date_of_service (str): Date when the service has been occured. Example 2020-01-01

    Returns:
        dict: return the data in dictionary
    """
    claim_id_list = [f"claim{str(uuid.uuid4())[:8]}" for _ in range(n)]
    paid_amount_list = [str(round(uniform(10, 1000), 2)) for _ in range(n)]
    date_of_service_list = [date_of_service for _ in range(n)]
    days_supply_list = [randint(1, 99) for _ in range(n)]

    data = {
        "claim_id": claim_id_list,
        "paid_amount": paid_amount_list,
        "date_of_service": date_of_service_list,
        "days_supply": days_supply_list,
    }
    return data
