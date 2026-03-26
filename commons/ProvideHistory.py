import logging
from dataclasses import dataclass, field
from typing import Dict, List
import re
import ulid
import os
from airflow.models import Variable

from commons.ProvideMetaInfoHook import ProvideMetaInfoHook

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

@dataclass
class ProvideHistoryData:
    id: str = ""
    version: str = ""
    type: str = ""
    period: str = ""
    source: str = ""
    target: List[str] = field(default_factory=list)
    filepath: str = ""
    filenm: str = ""
    source_ip: str = ""
    db: str = ""
    tb: str = ""
    retry: str = ""
    partitions: List[Dict[str, str]] = field(default_factory=list)

class ProvideHistory:
    def __init__(self, collect_interface, provide_interface, provide_target, dest_path, retry):
        self.provide_history_data = ProvideHistoryData()
        self.provide_history_data.version = "1"
        self.provide_history_data.source = "datagw"

        self.provide_history_data.id = self._get_ulid()
        self.provide_history_data.period = self._get_period(collect_interface)

        self._set_prop_using_interface(provide_interface, provide_target)
        self._set_path_using_dest_path(dest_path)

        self.provide_history_data.retry = "N"
        if retry:
            self.provide_history_data.retry = "Y"

    def _get_ulid(self):
        new_ulid = ulid.new()
        return str(new_ulid)

    def _get_period(self, collect_interface):
        period = ""

        if collect_interface:
            period = collect_interface.job_schedule_exp

        return period

    def _get_target(self, provide_interface_system_cd):
        system_code = ProvideMetaInfoHook().get_system_nm(provide_interface_system_cd)
        target = str(system_code.system_nm).lower()

        return target

    def _set_prop_using_interface(self, provide_interface, provide_target):
        self.provide_history_data.type = provide_interface.protocol_cd
        self.provide_history_data.target.append(self._get_target(provide_interface.system_cd))
        self.provide_history_data.db = provide_interface.db
        self.provide_history_data.tb = provide_interface.tb

        if provide_target:
            self.provide_history_data.source_ip = provide_target.target_info_json.ip

    def _set_path_using_dest_path(self, dest_path):
        self.provide_history_data.partitions = self._extract_partitions(dest_path)

        file_last_path = os.path.basename(dest_path)

        if '=' not in file_last_path:
            self.provide_history_data.filepath = os.path.dirname(dest_path)
            self.provide_history_data.filenm = file_last_path
        else:
            self.provide_history_data.filepath = dest_path
            self.provide_history_data.filenm= ""

    def _extract_partitions(self, hdfs_dest_path):
        partitions_list = []
        dt_match = re.search(r'dt=(\d+)', hdfs_dest_path)
        hh_match = re.search(r'hh=(\d+)', hdfs_dest_path)
        hm_match = re.search(r'hm=(\d+)', hdfs_dest_path)
        ym_match = re.search(r'ym=(\d{6})', hdfs_dest_path)

        if dt_match:
            partitions_list.append({'dt': dt_match.group(1)})
        if hh_match:
            partitions_list.append({'hh': hh_match.group(1)})
        if hm_match:
            partitions_list.append({'hm': hm_match.group(1)})
        if ym_match:
            partitions_list.append({'ym': ym_match.group(1)})

        return partitions_list

