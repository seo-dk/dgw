import fnmatch
import ftplib
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import chardet
import paramiko
import ulid
from airflow.exceptions import AirflowException
from airflow.models import Variable


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class FileMergeCoordinator:

    def __init__(self, meta_info, kafka_send, collect_history):
        self.meta_info = meta_info
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.info_dat_path = None
        self.merge_info_dat_path = None
        self.src_path = None
        self.merge_src_path = None

    def start_merge_process(self, info_dat_path):
        self.info_dat_path = info_dat_path

        info_path_list = str(info_dat_path).split("/")
        last_info_path = info_path_list[-1]
        # /data/gw_meta/C-TPANI-TEST-SFTP-HH-0008/info_01HD5XR4Z0RD9FT5QE9CHNQWKN.dat -> /data/gw_meta/C-TPANI-TEST-SFTP-HH-0008/merge_info_01HD5XR4Z0RD9FT5QE9CHNQWKN.dat
        self.merge_info_dat_path = str(info_dat_path).replace(last_info_path, "merge_" + last_info_path)
        logger.info("merge_info_dat_path: %s", self.merge_info_dat_path)

        self.src_path = self.get_src_path(self.info_dat_path)
        logger.info("src_path: %s", self.src_path)

        self.merge_src_path = self.get_merge_src_path(self.src_path)
        logger.info("merge_src_path: %s", self.merge_src_path)

        self.write_merge_file()

        self.write_merge_info_dat()

        # info dat path return
        return self.merge_info_dat_path

    def write_merge_file(self):
        try:
            # read info.dat file
            os.makedirs(os.path.dirname(self.merge_src_path), exist_ok=True)

            with open(self.merge_src_path, 'w') as merge_file:
                with open(self.info_dat_path, 'r') as info_dat_file:
                    lines = info_dat_file.readlines()

                    for line in lines:
                        src = line.strip().split(",")[0]
                        logger.debug("write_merge_file src: %s", src)

                        with open(src, 'r') as target_file:
                            file_contents = target_file.read()
                            merge_file.write(file_contents + "\n")

        except FileNotFoundError:
            logger.error("merge_file - FileNotFoundError")
            self.kafka_send.sendErrorKafka(self.collect_history, 10, False, "merge_file - FileNotFoundError")

        except Exception as e:
            logger.exception("Error writing merge_file: %s", e)
            self.kafka_send.sendErrorKafka(self.collect_history, 10, False, "Error writing merge_file")

    def write_merge_info_dat(self):
        try:
            # info.dat file content
            dest_path = str(self.merge_src_path).replace("/data", "")
            new_ulid = ulid.new()

            jarInfo = ','.join([str(self.merge_src_path), str(dest_path), str(new_ulid)])
            # write local tmp file path & hdfs path to info.dat file
            logger.info("writing %s info.dat...", jarInfo)

            os.makedirs(os.path.dirname(self.merge_info_dat_path), exist_ok=True)
            with open(str(self.merge_info_dat_path), "w") as file:
                file.write(jarInfo)

            logger.info("finished writing at %s", self.merge_info_dat_path)

        except FileNotFoundError:
            logger.error("write_info_dat - FileNotFoundError")
            self.kafka_send.sendErrorKafka(self.collect_history, 10, False, "write_info_dat - FileNotFoundError")

        except Exception as e:
            logger.exception("Error writing merge_info.dat file: %s", e)
            self.kafka_send.sendErrorKafka(self.collect_history, 10, False, "Error writing merge_info.dat file")

    def get_src_path(self, info_dat_path):
        src_path = None

        try:
            with open(info_dat_path, 'r') as info_dat_file:
                lines = info_dat_file.readlines()
                line = lines[0].strip().split(",")
                src = line[0]
                src_path = src

        except FileNotFoundError:
            logger.error("get_src_path - self.info_dat_path: %s", info_dat_path)

        except Exception as e:
            logger.exception("Error read info_dat_path file: %s", e)
            self.kafka_send.sendErrorKafka(self.collect_history, 10, False, "Error reading info_dat_path file")

        return src_path

    def get_merge_src_path(self, src_path):
        merge_src_path = None

        if src_path is not None:
            src_path_list = str(src_path).split("/")
            tb_nm = None
            for part in src_path_list:
                if part.startswith("tb="):
                    tb_nm = part.replace("tb=", "")
                    break
            file_nm = src_path_list[-1]
            extention = file_nm.split(".")[-1]
            new_file_nm = tb_nm.upper() + "_MERGE." + extention

            last_slash_index = self.src_path.rfind('/')
            merge_src_path = self.src_path[:last_slash_index + 1] + new_file_nm

        return merge_src_path
