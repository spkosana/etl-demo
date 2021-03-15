import logging
import sys
import os


def getlogger(log_dir="logs", filename="dataplatform"):

    # logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    filename = filename.split(".")[0]
    file_root_location = f"{os.getcwd()}"
    log_file_directory = f"{file_root_location}/{log_dir}/"
    log_file_location = f"{file_root_location}/{log_dir}/{filename}.log"

    if not os.path.exists(log_file_directory):
        os.mkdir(log_file_directory)

    filehandler = logging.FileHandler(log_file_location)
    filehandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
        "%m-%d-%Y %H:%M:%S",
    )
    filehandler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_formatter = logging.Formatter(
        "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
        "%m-%d-%Y %H:%M:%S",
    )
    stream_handler.setFormatter(stream_formatter)

    logger.addHandler(filehandler)
    logger.addHandler(stream_handler)

    return logger
