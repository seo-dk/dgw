import logging
import os
from datetime import datetime
from pathlib import Path
from ftplib import FTP
from paramiko import SFTPClient

from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.CollectHistory import CollectHistory
from commons.PathUtil import PathUtil, LocalPathInfos

logger = logging.getLogger(__name__)


class FtpSftpDownloader:

    def __init__(self, kafka_send):
        self.kafka_send = kafka_send

    def ftp_download(self, ftp: FTP, local_path_infos: LocalPathInfos, filename, coll_hist_dict, collect_history: CollectHistory, target_time, interface_info: InterfaceInfo, source_info: SourceInfo):
        logger.info('fullPathDict: %s, interface_info: %s', local_path_infos, interface_info)
        local_tmp = local_path_infos.local_tmp_dir
        logger.debug("local_tmp : %s", local_tmp)
        try:
            collect_file_dir_name = os.path.dirname(local_path_infos.local_tmp_full_path)
            if not os.path.exists(collect_file_dir_name):
                os.makedirs(os.path.dirname(local_path_infos.local_tmp_full_path), exist_ok=True)
                logger.info("mkdir %s", collect_file_dir_name)

            with open(local_path_infos.local_tmp_full_path, 'wb') as local_file:
                try:
                    startTime = datetime.now()
                    logger.info("FTP download started. target filenm : %s", filename)
                    ftp.retrbinary('RETR ' + filename, local_file.write)
                    endTime = datetime.now()
                    timeTaken = endTime - startTime
                    # get file from source -> local temp dir
                    logger.info("Download %s, Time taken : %s , Target time : %s", filename, timeTaken, target_time)
                    logger.info("local file size : %s", os.path.getsize(local_path_infos.local_tmp_full_path))

                    if source_info.file_proto_info.delete_source_after_download:
                        ftp.delete(filename)
                        logger.info(f"remove {filename} at source server {local_path_infos.source_file_path}")

                except Exception as e:
                    if local_path_infos.local_tmp_full_path is None:
                        raise Exception("Is not a Proper file...")
                    coll_hist_dict.err_msg = "filenm : {}, {}".format(filename, str(e)[:1024])
                    collect_history.set_status_code(coll_hist_dict, 2)
                    coll_hist_dict.ended_at = datetime.now().isoformat()
                    # send error message to kafka
                    self.kafka_send.send_to_kafka(coll_hist_dict)
                    raise Exception("Error FTP get, file : %s" % filename)

            logger.info("FTP download completed at %s", local_path_infos.local_tmp_full_path)
            return 1
        except Exception as e:
            raise Exception("Failed to download file: %s" % filename)

    def ftp_get_tar_gzip(self, ftp: FTP, local_full_tmp, filename, collect_history):
        logger.info("tar gzip local path : %s", local_full_tmp)
        if not os.path.exists(local_full_tmp):
            with open(local_full_tmp, 'wb') as local_file:
                try:
                    start_time = datetime.now()
                    logger.info("tar gzip file download start, target filenm : %s", filename)
                    ftp.retrbinary('RETR ' + filename, local_file.write)
                    end_time = datetime.now()
                    time_taken = end_time - start_time
                    logger.info("tar gzip file download success, Time taken %s", time_taken)
                    return local_full_tmp
                except Exception as e:
                    self.kafka_send.sendErrorKafka(collect_history, 2, True,
                                                   "Error while getting tar gzip file" + str(e)[:1024])
                    raise Exception("failed to download file : %s" % filename)
        else:
            logger.info("%s file exist...", filename)
            return local_full_tmp

    def sftp_download(self, sftp: SFTPClient, local_path_infos: LocalPathInfos, filename, coll_hist_dict, collect_history: CollectHistory,
                      interface_info: InterfaceInfo, source_info: SourceInfo):
        logger.info('fullPathDict: %s, meta_info: %s, meta_info.ftp_server_info.data_source_dir: %s', local_path_infos,
                    interface_info, type(interface_info.ftp_proto_info.source_dir))
        local_tmp = local_path_infos.local_tmp_dir
        logger.debug("local_tmp : %s", local_tmp)
        try:
            try:
                collect_file_dir_name = os.path.dirname(local_path_infos.local_tmp_full_path)
                if not os.path.exists(collect_file_dir_name):
                    os.makedirs(os.path.dirname(local_path_infos.local_tmp_full_path), exist_ok=True)
                    logger.info("mkdir %s", collect_file_dir_name)

                logger.info("Target filename : %s", filename)
                startTime = datetime.now()
                logger.info("SFTP download started. target filename : %s", filename)
                sftp.get(str(local_path_infos.source_file_path), str(local_path_infos.local_tmp_full_path))
                endTime = datetime.now()
                timeTaken = endTime - startTime
                logger.info("SFTP download success, filename : %s, Time taken : %s", filename, timeTaken)
                logger.info("local file size : %s", os.path.getsize(local_path_infos.local_tmp_full_path))

                if source_info.file_proto_info.delete_source_after_download:
                    sftp.remove(str(local_path_infos.source_file_path))
                    logger.info(f"remove {filename} at source server {local_path_infos.source_file_path}")

            except Exception as e:
                coll_hist_dict.err_msg = "filename : {}, {}".format(filename, str(e)[:1024])
                current_time = datetime.now().isoformat()
                coll_hist_dict.ended_at = current_time
                collect_history.set_status_code(coll_hist_dict, 2)
                # set Error info and send to kafka
                self.kafka_send.send_to_kafka(coll_hist_dict)
                raise Exception("Error SFTP get, file : %s" % filename)

            logger.info("FTP download completed at %s", local_tmp)
            return 1

        except Exception as e:
            raise Exception("Failed to download file: %s" % filename)

    def sftp_get_tar_gzip(self, sftp: SFTPClient, local_path_infos: LocalPathInfos, filename, collect_history):
        try:
            startTime = datetime.now()
            local_full_tmp = PathUtil.get_zip_path(local_path_infos.local_tmp_full_path, filename)
            source_full_dir = PathUtil.get_zip_path(local_path_infos.source_file_path, filename)
            logger.info("SFTP download started. target filename : %s", filename)
            sftp.get(str(source_full_dir), str(local_full_tmp))
            endTime = datetime.now()
            timeTaken = endTime - startTime
            logger.info("Download success : %s, Time Taken: %s", filename, timeTaken)
            logger.info("tar gzip file download success")
            return local_full_tmp
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 2, True, "Error while getting tar gzip file" + str(e)[:1024])
            raise Exception("failed to download zip file, file %s" % filename)


