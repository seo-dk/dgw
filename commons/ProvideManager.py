import logging
import os.path
from typing import List
import re

from airflow.models import Variable

from airflow.exceptions import AirflowException

from commons.MetaInfoHook import CollectProvideInfo, InterfaceInfo, SourceInfo, FtpSftpInterfaceProtoInfo
from commons.ProvideDistcpManager import ProvideDistcpManager
from commons.ProvideFTPManager import ProvideFTPManager
from commons.ProvideFTPManagerForDistcpDag import ProvideFTPManagerForDistcpDag
from commons.ProvideMetaInfoHook import ProvideMetaInfoHook, ProvideInterface
from commons.ProvideFileHandler import ProvideFileHandler
from commons.RemoteSparkJobManager import RemoteSparkJobManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class ProvideManager:
    def __init__(self, interface_info: InterfaceInfo, collect_provide_infos: List[CollectProvideInfo], retry=None):
        self.interface_info = interface_info
        self.collect_provide_infos = collect_provide_infos
        self.retry = False
        # self.context = context

        if retry:
            self.retry = True

    def provide_from_ftp(self, info_dat_path):
        if not self.collect_provide_infos:
            logger.warning("empty provide_list")
            return

        logger.info("provide logic start. info_dat_path: %s", info_dat_path)

        if not os.path.exists(info_dat_path):
            logger.warning("file not exist. info_dat_path: %s", info_dat_path)
            return

        src_path_list, unique_path_list = self._read_info_dat(info_dat_path)

        for collect_provide_info in self.collect_provide_infos:
            if collect_provide_info.provide_protocol_cd == 'DISTCP':
                # interface_cycle이 HH이고 unique_path_list에 hh=가 없으면 파일명까지 포함된 src_path_list 사용
                use_src_path_list = False
                if self.interface_info and self.interface_info.interface_cycle == 'HH':
                    # unique_path_list의 경로에 hh=가 포함되어 있는지 확인
                    has_hh_partition = any('hh=' in path for path in unique_path_list)
                    if not has_hh_partition:
                        use_src_path_list = True
                        logger.info("interface_cycle is HH and unique_path_list doesn't have hh= partition, using src_path_list with filenames")

                if use_src_path_list:
                    self._execute_provide_from_ftp(src_path_list, collect_provide_info)
                else:
                    self._execute_provide_from_ftp(unique_path_list, collect_provide_info)
            else:
                self._execute_provide_from_ftp(src_path_list, collect_provide_info)

    def provide_from_distcp(self, source_info: SourceInfo, hdfs_src_path, version, clear_path):
        def _get_dest_dict(dest_path):
            dest_path_dict = {}
            pattern = re.compile(r'db=([^/]+)/tb=([^/]+)')
            match = pattern.search(dest_path)

            if match:
                dest_path_dict["db"] = match.group(1).lower()
                dest_path_dict["tb"] = match.group(2).lower()
                logger.info("dest_dict[db]: %s, dest_dict[tb]: %s", dest_path_dict["db"], dest_path_dict["tb"])

            return dest_path_dict

        dest_path_dict = _get_dest_dict(hdfs_src_path)
        provide_interfaces: List[ProvideInterface] = ProvideMetaInfoHook().get_provide_interface_by_tb(dest_path_dict.get('db'), dest_path_dict.get('tb'))

        for provide_interface in provide_interfaces:

            provide_file_handler = ProvideFileHandler(self.interface_info, hdfs_src_path, provide_interface, version, clear_path)
            provide_file_handler.start(source_info)

            converted_hdfs_dest_path = provide_file_handler.get_hdfs_path()

            logger.info(f"coverted_hdfs_dest_path : {converted_hdfs_dest_path}")
            provide_dest_path = None
            provide_manager = ProvideManager(self.interface_info, None, False)

            if (
                    provide_interface.protocol_cd == 'FTP'
                    or provide_interface.protocol_cd == 'SFTP'
            ):
                provide_dest_path = provide_manager.provide_to_ftp(provide_interface, converted_hdfs_dest_path, version)
            elif provide_interface.protocol_cd == 'DISTCP':
                provide_dest_path = provide_manager.provide_to_distcp(provide_interface, converted_hdfs_dest_path, version)

            if provide_interface.proto_info_json.spark_server_info.is_external:
                RemoteSparkJobManager(self.interface_info, provide_interface, provide_dest_path, "distcp").start()

    def provide_from_distcp_batch(self, source_info, provide_interface, src_hdfs_path, version):
        provide_file_handler = ProvideFileHandler(self.interface_info, src_hdfs_path, provide_interface, version, source_info.distcp_proto_info.clear_path)
        provide_file_handler.start(source_info)

        src_path = provide_file_handler.get_hdfs_path()
        provide_dest_path = None
        if provide_interface.protocol_cd == 'FTP' or provide_interface.protocol_cd == 'SFTP':
            provide_dest_path = self.provide_to_ftp(provide_interface, src_path, version)
        elif provide_interface.protocol_cd == 'DISTCP':
            provide_dest_path = self.provide_to_distcp(provide_interface, src_path, version="1")

        if provide_interface.proto_info_json.spark_server_info.is_external:
            RemoteSparkJobManager(self.interface_info, provide_interface, provide_dest_path, "distcp").start()

    def provide_to_distcp(self, provide_info: ProvideInterface, hdfs_dest_path, version):
        logger.info("%s provide logic start", provide_info.interface_id)

        if not self._check_data_provision(provide_info, version):
            logger.warning("%s provide stop, options of allow_provide is false", provide_info.interface_id)
            return

        # this place is called when od triggered
        converted_hdfs_src_path = self._needs_parquet_conversion(provide_info, hdfs_dest_path)
        target_dest_path = ProvideDistcpManager(None, self.interface_info, None, converted_hdfs_src_path, self.retry, provide_info).execute()
        logger.info("%s provide logic successfully end", provide_info.interface_id)
        return target_dest_path

    def provide_to_ftp(self, provide_info: ProvideInterface, hdfs_dest_path, version):
        logger.info("%s provide logic start", provide_info.interface_id)

        if provide_info.proto_info_json.separate_path and version == "2":
            logger.info("separate_path true")

            for hdfs_dest_path in self._get_hdfs_subpaths(hdfs_dest_path):
                ProvideFTPManagerForDistcpDag(self.interface_info, provide_info, hdfs_dest_path, self.retry, version).execute()

            logger.info("%s provide logic successfully end", provide_info.interface_id)
            return

        formatted_target_dir = ProvideFTPManagerForDistcpDag(self.interface_info, provide_info, hdfs_dest_path, self.retry, version).execute()
        logger.info("%s provide logic successfully end", provide_info.interface_id)
        return formatted_target_dir

    def _read_info_dat(self, info_dat_path):
        src_path_list = []
        unique_path_list = []
        with open(info_dat_path, 'r') as f:
            for line in f.readlines():
                item_list = line.strip().split(",")
                dest = item_list[1]
                src_path = os.path.dirname(dest)

                if src_path not in unique_path_list:
                    logger.debug("unique_paths: %s", src_path)
                    # use dir_paht for distcp provide
                    unique_path_list.append(src_path)

                # use full path for ftp provide
                src_path_list.append(dest)
        return src_path_list, unique_path_list

    def _execute_provide_from_ftp(self, path_list, collect_provide_info: CollectProvideInfo):
        provided_src_path = []
        for src_path in path_list:
            if str(collect_provide_info.collect_hdfs_dir + "/") in src_path:
                logger.info("provide_info: %s", collect_provide_info)
                logger.info("%s provide logic start", collect_provide_info.provide_interface_id)
                logger.info("src_path %s", src_path)

                provide_interface = ProvideMetaInfoHook().get_provide_interface(collect_provide_info.provide_interface_id)

                provide_file_handler = ProvideFileHandler(self.interface_info, src_path, provide_interface, None, False)
                provide_file_handler.start()

                src_path = provide_file_handler.get_hdfs_path()

                if collect_provide_info.provide_protocol_cd == 'DISTCP':
                    provide_dest_path = ProvideDistcpManager(None, self.interface_info, collect_provide_info, src_path, self.retry).execute()
                else:
                    provide_dest_path = ProvideFTPManager(None, self.interface_info, collect_provide_info, src_path, self.retry).execute()

                logger.info("%s provide logic successfully end", collect_provide_info.provide_interface_id)

                if provide_interface.proto_info_json.spark_server_info.is_external:
                    logger.info("start spark job at remote server")
                    RemoteSparkJobManager(self.interface_info, provide_interface, provide_dest_path, "ftp/sftp").start()

    def _check_data_provision(self, provide_info, version):
        if 'DATALAKE-DISTCP' in provide_info.interface_id and version == "2":
            return provide_info.proto_info_json.allow_provide

        return True

    def _get_hdfs_subpaths(self, hdfs_path):
        hdfs_paths = []

        local_download_path = hdfs_path.replace('/staging', '/data')
        logger.info("local_download_path: %s", local_download_path)

        for fodler_name in os.listdir(local_download_path):
            local_fodler_path = os.path.join(local_download_path, fodler_name)
            if os.path.isdir(local_fodler_path):
                hdfs_sub_full_path = os.path.join(hdfs_path, fodler_name)
                logger.info("hdfs_sub_full_path: %s", hdfs_sub_full_path)
                hdfs_paths.append(hdfs_sub_full_path)

        return hdfs_paths

    def _needs_parquet_conversion(self, provide_info, hdfs_src_path):
        # distcp 수집 시 parquet 변환 된 경우: DataGWCluster staging 에 원본, 기본 경로에 변환 된 parquet 적재, 제공 시 hdfs_src_path는 staging 경로(원본) 전달
        # dbconn, sftp/ftp 수집 시 parquet 변환 된 경우: DataGWClsuter 기본 경로에 parquet 파일만 적재, 원본은 로컬에만 저장

        # 이미 ProvideFormatConverter에서 변환된 경로인 경우 스킵 (중복 변환 방지)
        if "/converted_parquet" in hdfs_src_path or "/converted_" in hdfs_src_path:
            logger.info("already converted path, skip _needs_parquet_conversion: %s", hdfs_src_path)
            return hdfs_src_path

        local_parquet_path = os.path.join("/data", hdfs_src_path.replace('/staging', '').lstrip("/")).replace("/idcube_out", "/idcube_out/converted_parquet")
        logger.info("check local_parquet_path exist %s", local_parquet_path)

        if os.path.exists(local_parquet_path):
            logger.info("collecting files by converting to parquet")
            ## format 변경으로 인해 convert_to -> format 변경
            if provide_info.proto_info_json.format == "parquet":
                # parquet 파일 저장된 하둡 경로 사용 path.replace(staging)
                return hdfs_src_path.replace('/staging', '')

        return hdfs_src_path


def _test_provide_from_ftp_spark():
    info_dat_path = "/data/gw_meta/test/C-PCELL-TEST-SFTP-DD-0001/info_01JY0Y9VM93PWH5V57DZSVHHY7.dat"
    collect_provide_infos = [CollectProvideInfo(collect_delimiter='|', collect_encode='EUC-KR', collect_interface_id='C-PCELL-TEST-SFTP-DD-0001', collect_hdfs_dir='/db=o_idcs/tb=lte_pcell_1d_test',
                       provide_interface_id='P-TAPD-TEST-DISTCP-OD-1068', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://90.90.43.21:8020/tap_d/tmp/db=o_idcs/tb=lte_pcell_1d/dt=${yyyyMMdd}', provide_protocol_cd='DISTCP', target_id='SVR-0038',
                       provide_use_yn='N')]
    interface_info = InterfaceInfo(interface_id='C-PCELL-TEST-SFTP-DD-0001', protocol_cd='SFTP', interface_cycle='DD', job_schedule_exp='0 12 * * *',
                                   ip_net='ip_net_150', system_cd='2012',
                                   proto_info='{"id": "ftpuser_idcs", "ip": "90.90.47.31", "pwd": "!godqhr2023!", "port": "22", "encode": "EUC-KR", "format": "gz", "file_nm": "lte_pcell_db_${yyyyMMdd-2d}.tar.gz.*", "noti_dir": "", "delimiter": "|", "local_dir": "/data", "source_dir": "/data/pcell/LTE/${yyyyMMdd-2d}", "ramdisk_dir": "/mnt/ramdisk/pcell", "noti_file_nm": "", "ftp_trans_mode": "Active", "external_process": true}',
                                   ftp_proto_info=FtpSftpInterfaceProtoInfo(ip='90.90.47.31', port='22', local_dir='/data', id='ftpuser_idcs', pwd='!godqhr2023!', source_dir='/data/pcell/LTE/${yyyyMMdd-2d}', source_file_nm='lte_pcell_db_20250616.tar.gz.*', ftp_trans_mode='Active', delimiter='|', noti_dir='', noti_file_nm='', encode='EUC-KR', format='gz', use_partitions=True, dir_extract_pattern=None, schemaless=False, external_process=True, ramdisk_dir='/mnt/ramdisk/pcell', bulk_upload=False, file_prefix=None, header_skip=False, use_ssh_key=False, distcp_as_file=False, proxy_setting=None),
                                   db_proto_info=None, distcp_proto_info=None, kafka_proto_info=None, target_time='202506161607', start_time='202506181607')

    ProvideManager(interface_info, collect_provide_infos).provide_from_ftp(info_dat_path)

def _test_provide_from_ftp_encrypt_and_convert():
    info_dat_path = "/data/gw_meta/test/C-RLAND-TEST-SFTP-HH-0001/info_01JY3AJN7Q8YWG1QZW86QNA3QB.dat"
    interface_info = InterfaceInfo(interface_id='C-RLAND-TEST-SFTP-HH-0001', protocol_cd='SFTP', interface_cycle='HH', job_schedule_exp='30 * * * *',
                                   ip_net='ip_net_90', system_cd='2025',
                                   proto_info='{"id": "inftangodw", "ip": "90.90.31.204", "pwd": "$dlsxj_tangodw", "port": "22", "encode": "cp949", "format": "text", "file_nm": ".*-${yyyyMMddHH-2H}.dat", "noti_dir": "/data1/INF/MDT/NOTI/", "delimiter": "\\\\036", "local_dir": "/data", "source_dir": "/data1/INF/MDT/DATA/${yyyyMMdd-2H}", "noti_file_nm": "${yyyyMMdd-2H}.noti"}', ftp_proto_info=FtpSftpInterfaceProtoInfo(ip='90.90.31.204', port='22', local_dir='/data', id='inftangodw', pwd='$dlsxj_tangodw', source_dir='/data1/INF/MDT/DATA/${yyyyMMdd-2H}', source_file_nm='.*-2025061912.dat', ftp_trans_mode=None, delimiter='\\036', noti_dir='/data1/INF/MDT/NOTI/', noti_file_nm='${yyyyMMdd-2H}.noti', encode='cp949', format='text', use_partitions=True, dir_extract_pattern=None, schemaless=False, external_process=False, ramdisk_dir=None, bulk_upload=False, file_prefix=None, header_skip=False, use_ssh_key=False, distcp_as_file=False, proxy_setting=None),
                                   db_proto_info=None, distcp_proto_info=None, kafka_proto_info=None, target_time='202506191228', start_time='202506191428')
    collect_provide_infos = [
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_nsn_raw_1d_pri',
                       provide_interface_id='P-IAM-TEST-FTP-OD-0030', provide_delimiter='|', provide_encode=None, provide_target_dir='/data03/TANGOD/v2',
                       provide_protocol_cd='FTP', target_id='SVR-0017', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_nsn_raw_1d_pri',
                       provide_interface_id='P-RLAND-TEST-DISTCP-OD-1019', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://dataGWCluster/test/provide_origin/db=o_rland/tb=mdt_nsn_raw_1d_test/dt=${yyyyMMdd}/hh=${HH}', provide_protocol_cd='DISTCP',
                       target_id='SVR-0038', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_elg_raw_1d_pri',
                       provide_interface_id='P-RLAND-TEST-DISTCP-OD-1020', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://dataGWCluster/test/provide_origin/db=o_rland/tb=mdt_elg_raw_1d_test/dt=${yyyyMMdd}/hh=${HH}', provide_protocol_cd='DISTCP',
                       target_id='SVR-0038', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_sam_raw_1d_pri',
                       provide_interface_id='P-RLAND-TEST-DISTCP-OD-1021', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://dataGWCluster/test/provide_origin/db=o_rland/tb=mdt_sam_raw_1d_test/dt=${yyyyMMdd}/hh=${HH}', provide_protocol_cd='DISTCP',
                       target_id='SVR-0038', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001',
                       collect_hdfs_dir='/db=o_rland/tb=mdt_sam_raw_fail_1d_pri', provide_interface_id='P-RLAND-TEST-DISTCP-OD-1022', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://dataGWCluster/test/provide/db=o_rland/tb=mdt_sam_raw_fail_1d_test/dt=${yyyyMMdd}/hh=${HH}', provide_protocol_cd='DISTCP',
                       target_id='SVR-0038', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_sam_raw_1d_pri',
                       provide_interface_id='P-RLAND-TEST-DISTCP-OD-1023', provide_delimiter=None, provide_encode=None,
                       provide_target_dir='hdfs://dataGWCluster/test/provide/db=o_rland/tb=mdt_sam_raw_1d_test/dt=${yyyyMMdd}/hh=${HH}', provide_protocol_cd='DISTCP',
                       target_id='SVR-0038', provide_use_yn='Y'),
    CollectProvideInfo(collect_delimiter='\\036', collect_encode='cp949', collect_interface_id='C-RLAND-TEST-SFTP-HH-0001', collect_hdfs_dir='/db=o_rland/tb=mdt_sam_raw_1d_pri',
                       provide_interface_id='P-RLAND-TEST-FTP-OD-1025', provide_delimiter='|', provide_encode=None, provide_target_dir='/data03/TANGOD/v2',
                       provide_protocol_cd='FTP', target_id='SVR-0017', provide_use_yn='Y')
    ]

    ProvideManager(interface_info, collect_provide_infos).provide_from_ftp(info_dat_path)



def _test_needs_parquet_conversion():
    provide_manager = ProvideManager(None, None)

    hdfs_src_path_1 = "/test/idcube_out/db=o_aim/tb=aim_fed_qos_1d/dt=20250420/"
    hdfs_src_path_2 = "/test/idcube_out/db=o_aim/tb=aim_fed_qos_1d/dt=20250421/"
    hdfs_src_path_3 = "/test/idcube_out/db=o_aimdd/tb=aim_fed_qos_1d/dt=20250420/"

    provide_manager._needs_parquet_conversion(None, hdfs_src_path_1)
    provide_manager._needs_parquet_conversion(None, hdfs_src_path_2)
    provide_manager._needs_parquet_conversion(None, hdfs_src_path_3)


if __name__ == '__main__':
    # _test_needs_parquet_conversion()
    _test_provide_from_ftp_spark()
    _test_provide_from_ftp_encrypt_and_convert()
