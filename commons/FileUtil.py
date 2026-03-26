import logging
import os
from pathlib import Path
import polars as pl

logger = logging.getLogger(__name__)

def replace_delimiter(local_path, input_del, output_del):
    logger.info("changing delimiter for provide start....")

    if check_gz_extension(local_path):
        logger.info("stop changing delimiter, because target file is gz")
        return

    logger.info('input_del: %s', input_del)
    logger.info('provide_delimiter: %s', output_del)

    directory, filenm = os.path.split(local_path)
    file_basnm, file_extension = os.path.splitext(filenm)
    tmp_out = file_basnm + "_tmp" + file_extension
    tmp_fullpath = Path(directory, tmp_out)

    logger.info("Change delimiter started. local_path: %s", local_path)

    df = pl.read_csv(
        local_path,
        separator=input_del,
        has_header=False,
        infer_schema_length=0,
        quote_char=None
    )

    df.write_csv(
        tmp_fullpath,
        separator=output_del,
        include_header=False,
        quote_style="never"
    )

    os.replace(tmp_fullpath, local_path)

    logger.info("Change delimiter finished. local_path: %s", local_path)


def check_gz_extension(filename):
    _, fileExtension = os.path.splitext(filename)

    return fileExtension == ".gz"

