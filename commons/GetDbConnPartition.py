import logging
import re

from airflow.models import Variable

from commons.MetaInfoHook import InterfaceInfo
from commons.PathUtil import PathUtil

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class GetDbConnPartition:
    def __init__(self, interface_info: InterfaceInfo):
        self.interface_info = interface_info

    def get_partition(self, file_info, target_dt, interface_cycle):
        if self.interface_info.db_proto_info.tb_overwrite:
            return PathUtil.get_partition_info(file_info, target_dt, "DD", "", self.interface_info.target_time)
        else:
            return PathUtil.get_partition_info(file_info, target_dt, interface_cycle, "", self.interface_info.target_time)

    def get_overwritten_path(self, partition_dict):
        partitions = partition_dict["partitions"]
        dt_value = partitions[0]["dt"]
        new_dt_value = dt_value + "01"
        partitions[0]["dt"] = new_dt_value
        partition_dict["partitions"] = partitions

        hdfsPath = partition_dict["hdfsPath"]

        match = re.search(r'/dt=(\d+)', hdfsPath)

        if match:
            dt_value = match.group(1)
            new_dt_value = dt_value + "01"
            new_hdfsPath = re.sub(r'/dt=\d+', f'/dt={new_dt_value}', hdfsPath)
            partition_dict["hdfsPath"] = new_hdfsPath

        return partition_dict