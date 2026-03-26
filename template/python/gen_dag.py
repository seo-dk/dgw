import os
import glob
import shutil
import mysql.connector
import time
from datetime import datetime, timedelta
from configparser import ConfigParser
import paramiko
import sys
import json

import logging_config
import logging

logging_config.setup_logging(__file__)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

DEFAULTS = {
    "MAX_ACTIVE_RUNS" : 8,
    "MAX_ACTIVE_TASKS" : 24,
    "RETRIES" : 2,
    "IGNORE_SMS_ALERT" : False,
}

class SqlExecutor:
    def __init__(self, conf, section):
        self.conf = conf
        self.section = section

    def _execute_query(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute(self.conf[self.section]['query'])
        return cursor.fetchall()

    def execute(self):
        try:
            with mysql.connector.connect(**self.conf['common']) as conn:
                return self._execute_query(conn)
        except Exception as e:
            logger.exception(f"Failed to execute SQL: {e}")
            return []

class DagGenerator:
    def __init__(self, sql_executor, template_dict, output_dir, conf, section):
        self.sql_executor = sql_executor
        self.template_dict = template_dict
        self.output_dir = output_dir
        self.conf = conf
        self.section = section

    def _create_new_file(self, row):
        filetype = self._get_filetype(row)
        interface_id = row['INTERFACE_ID']
        new_file_name = f"{self.output_dir}/{interface_id}.py"
        
        try:
            shutil.copyfile(self.template_dict[filetype], new_file_name)
            logger.info('Created new file: %s', new_file_name)
        except Exception as e:
            raise Exception(f"Failed to create new file: {new_file_name}") from e

        return new_file_name

    def _get_replacements(self, row):
        keys = ['PROTOCOL_CD', 'INTERFACE_CYCLE', 'IP_NET', 'SYSTEM_NM']
        filtered_tags = list(filter(None, [row.get(key) for key in keys]))
        filtered_tags.append(self.conf[self.section]['tags'])

        # recipient_emails = ['hankgood95@mobigen.com', 'kyunghyunkim22@mobigen.com']

        recipient_emails = []
        interface_id = row.get('INTERFACE_ID', '')
        
        base = {
            "$DAG_ID": row['INTERFACE_ID'],
            "$TAGS": f"[{','.join(map(repr, filtered_tags))}]",
            "$IP_NET": row['IP_NET'],
            "$JOB_SCHEDULE_EXP": row.get('JOB_SCHEDULE_EXP'),
            "$DESCRIPTION": row['DESCRIPTION'],
            "$EMAIL": f"[{','.join(map(repr, recipient_emails))}]",
        }

        dag_args = row.get("dag_args") or {}
        if isinstance(dag_args, str):
            dag_args = json.loads(dag_args) if dag_args else {}

        merged = {**DEFAULTS, **{k.upper(): v for k, v in dag_args.items()}}

        for k, v in merged.items():
            base[f"${k.upper()}"] = v

        # print(f'base: {base}')

        return base

    def _get_filetype(self, row):
        # SFTP는 FTP와 TEMPLATE을 공유하기위해 PROTOCOL을 동일하게 설정
        filetype = 'FTP' if row['PROTOCOL_CD'] == 'SFTP' else row['PROTOCOL_CD']

        # FTP와 FTP-OD를 구분하기 위해 INTEFACE_CYCLE를 추가
        if row['INTERFACE_CYCLE'] == 'OD':
            filetype = f"{filetype}-{row['INTERFACE_CYCLE']}"
        return filetype

    def _generate(self, row):
        file_path = f"{self.output_dir}/{row['INTERFACE_ID']}.py"
        self._create_new_file(row)
        replacements = self._get_replacements(row)
        with open(file_path, 'r+') as file:
            content = file.read()
            for key, value in replacements.items():
                if isinstance(value, (int, float, bool)):
                    content = content.replace(key, str(value))
                else:
                    content = content.replace(key, value if key in ["$TAGS", "$EMAIL"] else f"\"{value}\"")
            file.seek(0)
            file.truncate()
            file.write(content)
        logger.info(f"File generated/updated: {file_path}")

    def generate(self):
        result_set = self.sql_executor.execute()
        existing_files = [f"{row['INTERFACE_ID']}.py" for row in result_set]

        for row in result_set:
            try:
                self._generate(row)
            except Exception as e:
                logger.exception(f"Failed to generate dag: {e}")            


class DagDeployer:
    def __init__(self, config_file, section):
        self.conf = ConfigParser()
        self.conf.read(config_file)
        self.section = section
        self.input_dir = self.conf[self.section]['input_dir']
        self.output_dir = self.conf[self.section]['output_dir']

        template_dict = self._load_template_dict()
        logger.info('Template files found: %s', template_dict)
        executor = SqlExecutor(self.conf, section)
        self.generator = DagGenerator(executor, template_dict, self.output_dir, self.conf, section)

    def _load_template_dict(self):
        return {
            protocol: sorted(files, reverse=True)[0]
            for protocol in ['SFTP', 'FTP', 'DBCONN', 'DISTCP', 'FTP-OD', 'DISTCP-OD', 'KAFKA']
            if (files := glob.glob(f"{self.input_dir}/dag_{protocol.lower()}.py"))
        }

    def _clear_output_directory(self):
        for f in glob.glob(f"{self.output_dir}/*"):
            os.remove(f)

    def run(self):
        try:
            self._clear_output_directory()
            self.generator.generate()
        except Exception:
            logger.exception('Error occurred while deploying dag')

if __name__ == '__main__':
    config_file = sys.argv[1]
    section = sys.argv[2]
    DagDeployer(config_file, section).run()

