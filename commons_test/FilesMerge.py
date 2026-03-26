import os.path
import re
import zlib
from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import List
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import copy
import gzip
from collections import namedtuple

from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend
from commons_test.InfodatManager import InfoDatManager
from commons_test.CompressionManager import CompressionManager



logger = logging.getLogger(__name__)


def start_merge(merge_file_list, encoding, compression, buffer_size=1024*1024*1024):
    logger.info("start merging file")
    current_size = 0
    current_file_index = 0
    buffered_data = []
    parquet_file_list = []
    try:
        merge_file_list = _decompress_gz_file(merge_file_list)
        for file in merge_file_list:
            file_size = os.path.getsize(file)
            if file_size <= buffer_size - current_size:
                with open(file, 'r', encoding=encoding) as file:
                    logger.info("reading file : %s", file)
                    while True:
                        chunk = file.read(16 * 1024 * 1024)
                        if not chunk:
                            break
                        buffered_data.append(chunk)
                current_size += file_size
            else:
                logger.info("file size is bigger than left buffer size, file size : %s, left buffer size : %s", file_size, buffer_size - current_size)
                for chunk in _file_chunk_reader(file, buffer_size - current_size, encoding):
                    buffered_data.append(chunk)
                    current_size += len(chunk)

                    if current_size >= buffer_size:
                        logger.info("buffer full")
                        ## need to write new parquet file at here.
                        parquet_file = file.replace("/idcube_out", "/idcube_out/converted")
                        tb_name = _extract_tb_name(file)
                        local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                                               f"{tb_name}_{current_file_index + 1}.{compression}.parquet")
                        local_parquet_dir = os.path.dirname(local_parquet_full_path)
                        if not os.path.exists(local_parquet_dir):
                            os.makedirs(local_parquet_dir)
                        with open(local_parquet_full_path, 'w', encoding='utf-8') as parquet_file:
                            parquet_file.writelines(buffered_data)
                        parquet_file_list.append(local_parquet_full_path)
                        current_size = 0
                        buffered_data = []
                        current_file_index += 1
        if buffered_data:
            parquet_file = merge_file_list[0].replace("/idcube_out", "/idcube_out/converted")
            tb_name = _extract_tb_name(parquet_file)
            local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                                   f"{tb_name}_{current_file_index + 1}.{compression}.parquet")
            local_parquet_dir = os.path.dirname(local_parquet_full_path)
            if not os.path.exists(local_parquet_dir):
                os.makedirs(local_parquet_dir)
            with open(local_parquet_full_path, 'w', encoding='utf-8') as parquet_file:
                parquet_file.writelines(buffered_data)
            parquet_file_list.append(local_parquet_full_path)

        logger.info("mergeed file list : %s", parquet_file_list)
        return parquet_file_list
    except Exception as e:
        raise Exception(e)


def _extract_tb_name(file_path):
    match = re.search(r'tb=(.*?)/', file_path)
    if match:
        return match.group(1)
    else:
        logger.error("no tb name...")
        raise Exception("no tb name")


def _file_chunk_reader(local_path, chunk_size, encoding):
    file_size = os.path.getsize(local_path)
    leftover = ""
    with open(local_path, 'r', encoding=encoding) as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            chunk = leftover + chunk
            lines = chunk.splitlines(keepends=True)
            if not chunk.endswith('\n'):
                logger.info("putting the left over")
                leftover = lines.pop()
            else:
                leftover = ""
            yield "".join(lines)


def _decompress_gz_file(merge_file_list):
    compression_manager = CompressionManager()
    decompress_files = []
    for file in merge_file_list:
        file_nm = file
        if ".gz" in file :
            file_nm = compression_manager.decompress_gz_with_zlib(file)
        logger.info("decompressed file_nm : %s", file_nm)
        decompress_files.append(file_nm)
    logger.info("decompressed file list : %s", decompress_files)
    return decompress_files