from functions import get_date_formats, generate_data
import argparse
import pandas as pd
import os
import sys
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_dir = f"{os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))}/logs"
filename = "generator.log"
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
filehandler = logging.FileHandler(f"{log_dir}/{filename}")
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
filehandler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_formatter = logging.Formatter(
    "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    "%m-%d-%Y %H:%M:%S",
)
stream_handler.setFormatter(stream_formatter)

logger.addHandler(filehandler)
logger.addHandler(stream_handler)


def main(args):

    dates_list = get_date_formats(
        start_date=f"{args.year}-01-01",
        end_date=f"{args.year}-12-01",
        freq=args.freq,
        sep=args.sep,
    )
    logger.info(dates_list)
    file_root_location = f"{os.getcwd()}"
    file_dir = args.dir
    claims_file_location = f"{file_root_location}/{file_dir}"

    if not os.path.exists(claims_file_location):
        os.mkdir(claims_file_location)
    for date in dates_list:
        claims_file = f"claimsdata_{date}.txt"
        final_file_location = f"{claims_file_location}/{claims_file}"
        logger.info(final_file_location)
        claims_data = pd.DataFrame(generate_data(n=args.num, date_of_service=date))
        claims_data.to_csv(
            final_file_location, header=None, index=None, sep="\t", encoding="utf-8"
        )


if __name__ == "__main__":

    """
    Execution:
        1) Default values: python3 generator.py 
        2) Custome values: python3 generator.py -y 2020 -f M -s "" -d data -n 20

    """
    parser = argparse.ArgumentParser(description="File Generator Job")
    parser.add_argument(
        "-y",
        "--year",
        nargs="?",
        type=str,
        help="Input year to generate files. Default is 2020",
        default="2020",
    )
    parser.add_argument(
        "-f",
        "--freq",
        nargs="?",
        type=str,
        help="Input frequency of dates M or MS. Default MS",
        default="MS",
    )
    parser.add_argument(
        "-s",
        "--sep",
        type=str,
        nargs="?",
        help="Input seperator. Default is _",
        default="_",
    )
    parser.add_argument(
        "-d",
        "--dir",
        type=str,
        nargs="?",
        help="Input seperator. Default is data folder at root level",
        default="data",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        nargs="?",
        help="Input seperator. Default is 20",
        default=20,
    )
    args = parser.parse_args()
    try:
        main(args)
    except Exception as e:
        logger.exception(str(e))
