import logging
from datetime import datetime

import oracledb
import mysql.connector

from commons.DbConnWriter import DbConnWriter
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class DatabaseHook:
    def __init__(self, kafka_send, collect_history, data_class_info):
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.data_class_info = data_class_info
        self.dsn = None
        self.batch_size = 1000
        self.pool = None

        if self.data_class_info.type == 'ORACLE':
            self._initialize_dsn()
            self._connect_oracle()
        else:
            self._connect_mysql()

    def _initialize_dsn(self):
        dsn = self.data_class_info.rac
        if self.data_class_info.rac is None or self.data_class_info.rac == "":
            dsn = self.data_class_info.ip + ':' + str(self.data_class_info.port) + '/' + self.data_class_info.sid

        self.dsn = dsn

    def _get_connection(self):
        if self.data_class_info.type == 'ORACLE':
            return self.pool.acquire()
        else:
            return self.pool.get_connection()

    def _connect_oracle(self):
        try:
            logger.info("dsn : %s", self.dsn)
            logger.info("user : %s", self.data_class_info.id)
            oracledb.init_oracle_client()
            self.pool = oracledb.create_pool(user=self.data_class_info.id, password=self.data_class_info.pwd, dsn=self.dsn,
                                         min=2, max=5, increment=1, getmode=oracledb.POOL_GETMODE_WAIT)

        except Exception as e:
            self.kafka_send.sendErrorKafka(self.collect_history, 8, True, "connect to oracle failed")
            logger.exception("Oracle connection faild : %s", e)
            raise AirflowException("Error at Oracle connection : " + str(e)[:1024], self.collect_history.server_info)

    def _connect_mysql(self):
        try:
            self.pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_size=5,
                host=self.data_class_info.ip,
                user=self.data_class_info.id,
                password=self.data_class_info.pwd,
                port=3306
                # port=int(self.data_class_info.port)
            )

            print("_connect_mysql success")

        except Exception as e:
            self.kafka_send.sendErrorKafka(self.collect_history, 8, True, "connect to mysql failed")
            logger.exception("Mysql connection faild : %s", e)
            raise AirflowException("Error at Mysql connection : " + str(e)[:1024], self.collect_history.server_info)

    def _return_strings_as_bytes(self, cursor, name, default_type, size, precision, scale):
        if default_type == oracledb.DB_TYPE_VARCHAR:
            return cursor.var(str, arraysize=cursor.arraysize, bypass_decode=True)

    def executeDataSql(self, collect_history_dict, query, sql_params, local_full_path, file_name):
        formatted_sql = query.replace('#', '{}').format(*sql_params)

        logger.debug("executeDataSql setting, file_name: %s, collect_history_dict: %s, executeDataSql sql: %s",
                     file_name, collect_history_dict.collect_hist_id, formatted_sql)

        connection = self._get_connection()
        try:
            start = datetime.now()
            total_batch = self._process_query(connection, formatted_sql, local_full_path, file_name, collect_history_dict)
            self._log_execution_result(collect_history_dict, file_name, local_full_path, start, total_batch)
        except Exception as e:
            logger.exception("Error at executeDataSql file_name: %s, collect_history_dict.id: %s, query: %s, error: %s",
                             file_name, collect_history_dict.collect_hist_id, formatted_sql, e)
            self.kafka_send.sendErrorKafka(collect_history_dict, 8, False, "executeDataSql error")
            raise AirflowException("Error at executeDataSql query: %s, error: %s", formatted_sql, e)

        finally:
            if self.data_class_info.type == 'ORACLE':
                self.pool.release(connection)
            else:
                connection.close()

    def _log_execution_result(self, collect_history_dict, file_name, local_full_path, start, total_batch):
        diff = datetime.now() - start
        logger.info("query process finished, file_name: %s, hist_id: %s, elapsed time: %s, total_batch: %s, local_full_path: %s",
                    file_name, collect_history_dict.collect_hist_id, diff, total_batch, local_full_path)
        if total_batch == 0:
            logger.warning("query result is empty, file_name: %s, hist_id: %s",file_name, collect_history_dict.collect_hist_id)

    def _process_query(self, connection, sql, local_full_path, file_name, collect_history_dict):
        db_conn_writer = DbConnWriter()
        db_conn_writer.remove_local_file(local_full_path)
        with connection.cursor() as cursor:
            cursor.outputtypehandler = self._return_strings_as_bytes
            cursor.execute(sql)

            columns = [col[0] for col in cursor.description]
            total_batch = 0

            while True:
                batch_result = cursor.fetchmany(self.batch_size)
                if not batch_result:
                    break

                results = self._process_batch(batch_result, columns)
                total_batch += len(results)
                db_conn_writer.write_to_local_file(self.kafka_send, collect_history_dict, local_full_path, file_name, results, total_batch)

        if total_batch == 0:
            db_conn_writer.write_to_local_file(self.kafka_send, collect_history_dict, local_full_path, file_name, None, total_batch)

        return total_batch

    def _process_batch(self, batch_result, columns):
        results = []
        for row in batch_result:
            decoded_row = self._decode_row(row, columns)
            results.append(decoded_row)
        return results

    def _decode_row(self, row, columns):
        decoded_row = []
        for idx, value in enumerate(row):
            column_name = columns[idx]
            decoded_row.append(self._decode_value(column_name, value))
        return decoded_row

    def _decode_value(self, column_name, value):
        if column_name.upper().endswith("DECODE_TO_EUC_KR") and value is not None:
            return bytes.fromhex(value.decode('ascii')).decode('euc-kr', errors='replace')
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return value

    def executeCountSql(self, query, params):
        formatted_sql = query.replace('#', '{}').format(*params)
        logger.info("executeCountSql sql: %s", formatted_sql)
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(formatted_sql)
                result = cursor.fetchone()

                row_count = int(result[0]) if result else None

            logger.info("executeCountSql result's row count: %s", row_count)

            return row_count

        except Exception as e:
            logger.exception("Error at executeCountSql : %s", e)
            self.kafka_send.sendErrorKafka(self.collect_history, 8, True, "executeCountSql error")
            raise AirflowException("Error at executeCountSql : " + str(e)[:1024], self.collect_history.server_info)

        finally:
            if self.data_class_info.type == 'ORACLE':
                self.pool.release(connection)
            else:
                connection.close()


    def closeConnection(self):
        if self.data_class_info.type == 'ORACLE':
            self.pool.close()
        else:
            self._get_connection().close()
