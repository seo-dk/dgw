import logging
import os
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class InfoDatManager:

    def write_info_dat(self, tmp_full_path, hdfs_path, filename, info_dat_path, ulid):
        try:
            full_hdfs_path = Path(hdfs_path, filename)
            # info.dat file content
            jar_info = ','.join([str(tmp_full_path), str(full_hdfs_path), ulid])
            # write local tmp file path & hdfs path to info.dat file
            logger.info("writing %s info.dat...", jar_info)
            infodat_dir, infodat_filenm = os.path.split(info_dat_path)
            os.makedirs(infodat_dir, exist_ok=True)

            with open(str(info_dat_path), 'a+') as file:
                if file.tell() > 0:
                    file.write('\n')
                file.write(jar_info)

            logger.info("finished writing at %s", info_dat_path)
        except Exception as e:
            logger.exception("Error Writing info.dat file: %s", e)
            raise AirflowException("Error writing Error Writing info.dat file", str(e))

    def delete_info_dat(self, info_dat_path):
        try:
            if os.path.exists(info_dat_path):
                os.remove(info_dat_path)
                logger.info("info.dat file deleted: %s", info_dat_path)
        except Exception as e:
            logger.exception("Error deleting info.data file: %s", e)
            raise AirflowException("Error Writing info.dat file", str(e))

    def read_infodat(self, info_dat_path):
        with open(info_dat_path, 'r') as f:
            lines = f.readlines()
        return lines

    def reset_infodat(self, info_dat_path):
        with open(info_dat_path, 'w') as f:
            pass

    def delete_non_parquet(self, info_dat_path):
        lines = self.read_infodat(info_dat_path)
        self.reset_infodat(info_dat_path)
        for local_path, dest, ulid in [line.strip().split(",") for line in lines]:
            if local_path.endswith(".parquet"):
                self.write_info_dat(local_path, os.path.dirname(dest), os.path.basename(local_path), info_dat_path, ulid)

