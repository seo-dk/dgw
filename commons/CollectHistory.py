import copy
import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List
import re
import ulid
from airflow.models import Variable

from commons.MetaInfoHook import InterfaceInfo

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


@dataclass
class CollectHistoryData:
    version: str = ""
    collect_hist_id: str = ""
    retry_id_seq: int = 0
    protocol_cd: str = ""
    period: str = ""
    interface_id: str = ""
    system_cd: str = ""
    collect_source_seq: int = 0
    local_path: str = ""
    db: str = ""
    tb: str = ""
    dest_path: str = ""
    filenm: str = ""
    target_dt: str = ""
    partitions: List[Dict[str, str]] = field(default_factory=list)
    file_size: int = -1
    started_at: str = ""
    ended_at: str = ""
    task_log_url: str = ""
    err_msg: str = ""
    status_cd: str = ""
    retry: bool = False
    host_nm: str = ""


class CollectHistory:

    def __init__(self, interface_id, dag_id, task_id, host_nm, version, execution_date):
        self.server_info = CollectHistoryData()
        self.server_info.interface_id = interface_id
        self.server_info.task_log_url = self._get_url(dag_id, task_id, execution_date)
        self.server_info.host_nm = host_nm
        self.server_info.version = version
        self.chdDict = {}

    def _get_url(self, dag_id, task_id, execution_date):

        nine_hours = timedelta(hours=9)
        kst = execution_date + nine_hours
        execution_date_str = str(kst)
        execution_date_encode = urllib.parse.quote(execution_date_str[:execution_date_str.index('+')])
        base_url = "http://90.90.47.121:9090/log"
        logUrl = f'{base_url}?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date_encode}'
        return logUrl

    def set_server_info(self, interface_info: InterfaceInfo):
        logger.info("interface info : %s", interface_info)

        if interface_info.ftp_proto_info and interface_info.ftp_proto_info.schemaless:
            logger.info("server info : %s", interface_info.ftp_proto_info)
            self.server_info.protocol_cd = "SCHEMALESS_" + interface_info.protocol_cd
        else:
            self.server_info.protocol_cd = interface_info.protocol_cd

        self.server_info.period = interface_info.job_schedule_exp
        self.server_info.system_cd = interface_info.system_cd
        self.server_info.started_at = datetime.now().isoformat()

    def set_partition_info(self, ulid, filename, file_size, ended_at, src):
        # /O_TPANI_DW/5G_TRAFFIC_ANALYSIS_ENB_1H/20230605/11/2023060511_5G_COVERAGE_ENB_1H.dat
        # /user/test/sample/lte_call_kpi_01_20230418_1240.dat.lz4

        self.chdDict[ulid].filenm = filename
        self.chdDict[ulid].file_size = file_size
        self.chdDict[ulid].ended_at = ended_at
        self.chdDict[ulid].local_path = src

    def set_status_code(self, coll_hist_dict, code):
        coll_hist_dict.status_cd = self.status_code[code].status_cd

    def start_collect_h(self, collect_source_seq, local_path, dest_path, filename, target_dt, partitions,
                        collect_hist_id=None, started_at=None, retry_id_seq=None):
        current_time = datetime.now().isoformat()
        new_ulid = None
        chd = copy.copy(self.server_info)

        if collect_hist_id is None and started_at is None:
            new_ulid = ulid.new()
            chd.started_at = current_time
            chd.status_cd = self.status_code[0].status_cd
        else:
            new_ulid = collect_hist_id
            chd.started_at = started_at
            chd.retry_id_seq = int(retry_id_seq)
            chd.status_cd = self.status_code[6].status_cd
        logger.info("target_dt : %s", target_dt)
        chd.target_dt = target_dt
        pattern = re.compile(r'db=([^/]+)/tb=([^/]+)')
        match = pattern.search(dest_path)
        if match:
            chd.db = match.group(1)
            chd.tb = match.group(2)
        chd.collect_hist_id = str(new_ulid)
        chd.collect_source_seq = collect_source_seq
        chd.local_path = str(local_path)
        chd.dest_path = str(dest_path)
        chd.filenm = filename
        chd.partitions = partitions

        # self.chdDict[collect_source_seq] = copy.deepcopy(chd)
        self.chdDict[str(new_ulid)] = copy.deepcopy(chd)
        logger.info("new_ulid : %s", new_ulid)
        logger.info("ulidDict : %s", self.chdDict[str(new_ulid)])
        return str(new_ulid)

    def create_retry_collect_hist(self, collect_hist_id):
        current_time = datetime.now().isoformat()

        chd = copy.copy(self.server_info)
        new_ulid = collect_hist_id
        chd.started_at = current_time
        chd.status_cd = self.status_code[6].status_cd
        chd.collect_hist_id = str(new_ulid)

        self.chdDict[str(new_ulid)] = copy.deepcopy(chd)
        logger.info("new_ulid : %s", new_ulid)
        logger.info("ulidDict : %s", self.chdDict[str(new_ulid)])
        return str(new_ulid)

    def set_retry_true(self):
        self.server_info.retry = True

    def get_new_ulid(self):
        new_ulid = ulid.new()
        return str(new_ulid)

    def set_statuscode_info(self, status_code_info):
        self.status_code = status_code_info

    def set_noti_version(self, version, ulid):
        self.chdDict[ulid].version = version

    def set_protoco_cd(self, protocol, ulid):
        self.chdDict[ulid].protocol_cd = protocol
