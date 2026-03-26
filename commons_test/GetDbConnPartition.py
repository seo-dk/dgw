import logging
import re

from airflow.models import Variable

from commons_test.MetaInfoHook import InterfaceInfo
from commons_test.PathUtil import PathUtil

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class GetDbConnPartition:
    def create_partition_dict(self, interface_info, file_info, target_dt, file_name):
        partition_dict = self._get_partition(interface_info, file_info, target_dt)

        # YYYYMM01
        if file_info.db_proto_info.partitions:
            partition_dict = self._get_overwritten_path(partition_dict)

        if partition_dict["partitions"] is None:
            logger.error("partition_dict's partitions value is None, file_name: %s", file_name)
            raise

        elif partition_dict["hdfsPath"] is None:
            logger.error("partition_dict's hdfs_path value is None, file_name: %s", file_name)
            raise

        return partition_dict

    def _get_partition(self, interface_info, file_info, target_dt):
        if interface_info.db_proto_info.tb_overwrite:
            return PathUtil.get_partition_info(file_info, target_dt, "DD", "", interface_info.target_time)
        else:
            return PathUtil.get_partition_info(file_info, target_dt, interface_info.interface_cycle, "", interface_info.target_time)

    def _get_overwritten_path(self, partition_dict):
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
