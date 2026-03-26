import os
import sys
import socket
import struct
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Tuple, Dict
import pendulum
import pymysql
import random

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowNotFoundException

logger = logging.getLogger(__name__)

DEFAULT_SERVERS: Dict[str, Tuple[str, int]] = {
    'skt': ('90.90.100.34', 11001),
    'kt': ('90.90.100.34', 11001),
    'lg': ('90.90.100.34', 11001),
}

def get_db_connection():
    raw_conn_ids = Variable.get("mmonit_conn_ids", default_var=None)
    conn_ids = [cid.strip() for cid in raw_conn_ids.split(",") if cid.strip()]
    random.shuffle(conn_ids)

    for cid in conn_ids:
        try:
            conn = BaseHook.get_connection(cid)
        except AirflowNotFoundException:
            continue

        try:
            return pymysql.connect(
                host=conn.host,
                user=conn.login,
                password=conn.password,
                database=conn.schema,
                port=conn.port or 3306,
                charset="utf8mb4",
                connect_timeout=3,
                read_timeout=5,
                write_timeout=5,
                autocommit=False
            )
        except Exception:
            continue

    raise RuntimeError(f"All DB connections failed.")


def get_mobile_numbers():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mobile FROM users 
                WHERE uname in ('kykim') 
                AND mobile IS NOT NULL
                """)
            rows = cur.fetchall()
            return [r[0] for r in rows]
    finally:
        conn.close()

def send_sms_alert(context):
    local_tz = pendulum.timezone("Asia/Seoul")
    ti = context.get('task_instance')
    exec_date = context.get('execution_date')
    exception = context.get('exception')
    end_date_kst= ti.end_date.astimezone(local_tz).strftime("%-m/%-d %-H:%-M:%-S")

    msg=f"[{end_date_kst}] {ti.dag_id}, {ti.task_id}, {exception}, {ti.hostname}, {ti.try_number}"

    numbers = get_mobile_numbers()
    for num in numbers:
        simple_send(to_number=num, msg=msg)


# TPDO-656: 인코딩 에러 문자 알림 (Parquet 변환 중 발생 시)
SMS_MSG_MAX_BYTES = 200

# TPDO-656: 인코딩 에러 문자 알림 (Parquet 변환 중 발생 시)
def send_encoding_error_alert(csv_file: str, exception: Exception, interface_id: str = '', hdfs_dir: str = ''):
    """Parquet 변환 중 인코딩 에러 발생 시 문자 알림 (설정 확인용)."""
    try:
        import re
        db, tb = '', ''
        if hdfs_dir:
            db_m = re.search(r'db=([^/]+)', hdfs_dir)
            tb_m = re.search(r'tb=([^/]+)', hdfs_dir)
            db = db_m.group(1) if db_m else ''
            tb = tb_m.group(1) if tb_m else ''
        err_str = f"{exception!s}"
        prefix = f"[Encoding Error] interface_id={interface_id} db={db} tb={tb} file={csv_file} error="
        prefix_bytes = prefix.encode('utf-8')
        err_encoded = err_str.encode('utf-8')
        max_err_bytes = SMS_MSG_MAX_BYTES - len(prefix_bytes) - 3
        if len(err_encoded) > max_err_bytes:
            err_str = err_encoded[:max_err_bytes].decode('utf-8', errors='ignore') + '...'
        msg = prefix + err_str
        numbers = get_mobile_numbers()
        for num in numbers:
            simple_send(to_number=num, msg=msg)
    except Exception as e:
        logger.warning("send_encoding_error_alert failed: %s", e)


def set_logger(log_level: str, log_path: str = None):
    handler = TimedRotatingFileHandler(log_path, when="h", interval=1, backupCount=5)
    formatter = logging.Formatter('[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.addHandler(handler)

def simple_send(to_number: str, msg: str, from_number='0317105551', tsp='skt'):
    send(from_number=from_number, to_number=to_number, msg=msg, nms='none', tsp=tsp, server_info=DEFAULT_SERVERS.get(tsp, DEFAULT_SERVERS['skt']), socket_timeout=5.0)


def send(from_number: str, to_number: str, msg: str, nms: str, tsp: str, socket_timeout: float, server_info: Dict[str, Tuple[str, int]]) -> bool:
    sock = None
    try:
        logger.info('Sending SMS...')
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(socket_timeout)
        
        if tsp not in DEFAULT_SERVERS:
            logger.error(f'Undefined tsp: {tsp}')
            return False

        category = '기타'.encode('euc_kr')
        sms_data = struct.pack('12s12s100s40s40s200s', to_number.encode(), from_number.encode(), nms.encode(), category, category, msg.encode())
        sock.sendto(sms_data, server_info)
        logger.info(f'SMS successfully sent. to_number: {to_number}')
        return True
    except Exception:
        logger.exception(f'Failed to send sms. to_number: {to_number}')
        return False
    finally:
        if sock:
            sock.close()

def parse_arguments():
    parser = argparse.ArgumentParser(description='SMS Sender')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='detail level to log. (default: INFO)')
    parser.add_argument('--log-path', default='sms.log', help='Log path. (optional)')

    parser.add_argument('--socket-timeout', help='socket timeout (default: 5.0)', default=5.0)

    parser.add_argument('--nms', help='nms (default:none)', default='none')
    parser.add_argument('--tsp', default='skt', choices=['skt', 'kt', 'lg'], help='carrier type (default: skt)')

    parser.add_argument('--from_number', help='Sender phone number (default: 0317105551)', default='0317105551')
    parser.add_argument('--to_number', help='Receiver phone number', required=True)

    parser.add_argument('--skt_sms_server_ip', help='SKT SMS server ip (default: 90.90.100.34)', default='90.90.100.34')
    parser.add_argument('--skt_sms_server_port', help='SKT SMS server port (default: 11001)', default=11001)
    parser.add_argument('--none_skt_server_ip', help='SKT SMS server ip (default: 90.90.100.34)', default='90.90.100.34')
    parser.add_argument('--none_skt_server_port', help='SKT SMS server port (default: 11001)', default=11001)

    parser.add_argument('-v', '--verbose', action='store_true', help='turn on verbose (DEBUG) logging. Overrides --log-level.')
    parser.add_argument('-m', '--msg', help='Message to send', required=True)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    if args.verbose:
        args.log_level = 'DEBUG'

    set_logger(args.log_level, args.log_path)

    send(
        to_number=args.to_number,
        from_number=args.from_number,
        msg=args.msg,
        nms=args.nms,
        tsp=args.tsp,        
        server_info=DEFAULT_SERVERS.get(args.tsp, DEFAULT_SERVERS['skt']),
        socket_timeout=args.socket_timeout,
    )
