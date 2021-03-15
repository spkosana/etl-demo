import argparse
import pandas as pd
from pprint import pprint
import json
from datetime import datetime
import time
import os
import csv
import shutil
import uuid
from project_logging import getlogger
import sqlite3
from claims_ddl import create_ddl_list

logger = getlogger(filename=__file__)


def get_file_data(schema: str, datafile: str) -> dict:
    cols_df = pd.read_csv(schema)
    file_cols = (cols_df[cols_df.columns.to_list()[0]]).to_list()
    data_df = pd.read_csv(datafile, header=None, sep="\t")
    current_cols = data_df.columns.to_list()
    rename_cols = dict(zip(current_cols, file_cols))
    data_df = data_df.rename(columns=rename_cols)
    data = json.loads(data_df.to_json(orient="records", indent=4))
    try:
        for record in data:
            yield {**record, "load_date": datetime.now()}
    except Exception as e:
        logger.info(str(e))


def get_target_file_locations(datafile, data_dir, processed_dir_location):
    processed_time = datetime.now().strftime("%H%M%S")
    pre_processed_file = datafile.split("/")[-1]
    post_processed_file = pre_processed_file.replace(".txt", f"_{processed_time}.txt")
    post_processed_err_file = pre_processed_file.replace(
        ".txt", f"_{processed_time}_err.csv"
    )
    source_pre_processed_file = f"{data_dir}/{pre_processed_file}"
    target_post_processed_file = f"{processed_dir_location}/{post_processed_file}"
    target_err_processed_file = f"{processed_dir_location}/{post_processed_err_file}"
    return (
        source_pre_processed_file,
        target_post_processed_file,
        target_err_processed_file,
    )


def create_table_sqlite(db_file, tables_list):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    for table in tables_list:
        cursor.execute(table)
    conn.commit()
    return conn, cursor


def main(args):

    schema = args.spec
    datafile = args.datafile

    data_table_name = datafile.split("/")[-1].split("_")[0]
    logger.info(data_table_name)

    data_dir = os.path.dirname(os.path.realpath(datafile))
    processed_dir_location = f"{data_dir}/processed"
    db_file = f"{args.target}/claims.db"
    conn, cursor = create_table_sqlite(db_file=db_file, tables_list=create_ddl_list)
    cursor.execute("DELETE FROM claimsdata_stage ")
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    if not os.path.exists(processed_dir_location):
        os.mkdir(processed_dir_location)
    rejected_records_list = []

    records_count_list = []
    before_load_time = datetime.now()
    data_date_value = []
    for n, record in enumerate(get_file_data(schema, datafile)):

        table_cols_tuple = tuple(record.keys())
        col_params = "(" + str((len(record.keys()) - 1) * "?,") + "?)"

        insert_query = f""" 
                        INSERT INTO claimsdata_stage{table_cols_tuple} 
                        VALUES{col_params}
    
                    """

        try:
            cursor.execute(insert_query, (tuple(record.values())))
            conn.commit()
            records_count_list.append(n)
            data_date_value.append(record["date_of_service"])
        except Exception as e:
            rejected_records_list.append(record)
            logger.exception(str(e))
            continue

    source, target, target_err = get_target_file_locations(
        datafile=datafile,
        data_dir=data_dir,
        processed_dir_location=processed_dir_location,
    )

    if len(rejected_records_list) != 0:
        with open(target_err, "w") as content:
            dict_writer = csv.DictWriter(content, rejected_records_list[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(rejected_records_list)
    else:
        target_err = "Its Whiskey Time :)"

    cursor.execute(f"select count(*) from {data_table_name}_stage")

    rejected_records_count = len(rejected_records_list)
    file_source_count = len(records_count_list)
    stage_table_count = cursor.fetchone()[0]

    load_query = f"""
                    INSERT INTO {data_table_name}
                    Select * 
                    FROM {data_table_name}_stage;
                 """
    cursor.execute(load_query)
    conn.commit()
    logger.info(before_load_time)
    cursor.execute(
        f"""
        select count(*) from {data_table_name} where date_of_service='{data_date_value[0]}' 
        and load_date like '{(str(before_load_time).split(".")[0])[:-3]}%'
        
        """
    )

    target_count = cursor.fetchone()[0]

    if file_source_count != target_count:
        quality_check_result = "Fail"
    else:
        quality_check_result = "Pass"

    jobs_meta = {
        "load_id": str(uuid.uuid4())[:8],
        "data_filename": datafile.split("/")[-1],
        "spec_file": schema.split("/")[-1],
        "spec_file_location": f"{os.path.abspath(schema)}",
        "etl_file": os.path.abspath(__file__),
        "source_location": source,
        "file_source_count": file_source_count,
        "processed_file_location": target,
        "stage_table_count": stage_table_count,
        "target_count": target_count,
        "quality_check_result": quality_check_result,
        "rejected_records_count": rejected_records_count,
        "reject_records_location": target_err,
        "record_created_date": datetime.now(),
    }

    job_meta_tuple = tuple(jobs_meta.keys())
    job_meta_values = "(" + str((len(jobs_meta.keys()) - 1) * "?,") + "?)"

    jobs_meta_insert = f""" 
                        INSERT INTO jobs_meta{job_meta_tuple} 
                        VALUES{job_meta_values}
    
                    """

    logger.info(jobs_meta)
    cursor.execute(jobs_meta_insert, (tuple(jobs_meta.values())))
    conn.commit()
    shutil.move(source, target)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    """
    Execution:
        1) Default values: python3 etl.py -s specs/claimsspec1.txt -d data/claimsdata_2020_01_01.txt 

    """
    parser = argparse.ArgumentParser(description="ETL Job")
    parser.add_argument(
        "-s", "--spec", nargs="?", type=str, help="Input specification filename"
    )
    parser.add_argument(
        "-d", "--datafile", nargs="?", type=str, help="Input data file name"
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        nargs="?",
        help="Database Connection",
        default=os.getcwd(),
    )

    args = parser.parse_args()
    start_time = time.time()
    try:
        main(args)
    except Exception as e:
        logger.exception(str(e))

    total_time_sec = time.time() - start_time
    total_time_mins = total_time_sec / 60
    logger.info(
        f"script took {total_time_sec} seconds in other words {total_time_mins} minutes"
    )
