import os
import json
import re
from pyarrow.fs import HadoopFileSystem
import glob
import pyarrow as pa
import pandas as pd
from decimal import Decimal

hadoop_home = '/local/HADOOP'

pattern = [hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

os.environ['CLASSPATH'] = ':'.join([hadoop_home + '/etc/hadoop:', hdfs_cp])
os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + hadoop_home + '/lib/native:'


class SchemaInfoManager:
    def __init__(self, hdfs_path, port=8020, user='hadoop'):
        print('SchemaInfoManager.__init__')
        host = "default"
        try:
            self.json_file_path = self._make_schema_hdfs_path(hdfs_path)
            self._hdfs = HadoopFileSystem(host, port=port, user=user)

            try:
                with self._hdfs.open_input_file(self.json_file_path) as file:
                    content = file.read().decode('utf-8')
                    self.schema_dict = json.loads(content)
            except Exception as e:
                raise Exception(f"Error reading hdfs file : {e}")

        except Exception as e:
            raise Exception(f"Error connecting hdfs : {e}")

    def get_schema_dict(self):
        return self.schema_dict

    def get_converters(self):
        converters = {}
        for field in self.schema_dict['fields']:
            if field['type'] == 'string':
                converters[field['name']] = self._safe_convert(str)
            elif field['type'] in ['int', 'long']:
                converters[field['name']] = self._safe_convert(lambda x: int(x.strip()))
            elif field['type'] in ['float', 'double']:
                converters[field['name']] = self._safe_convert(lambda x: float(x.strip()))
            elif field['type'] == 'boolean':
                converters[field['name']] = self._safe_convert(lambda x: x.lower() in ['true', '1', 't', 'y', 'yes'])
            elif field['type'] == 'timestamp':
                converters[field['name']] = self._safe_convert(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce'))
            elif field['type'] == 'decimal':
                converters[field['name']] = self._safe_convert(lambda x: Decimal(x.strip()))
            else:
                raise ValueError(f"Unsupported JSON type: {field['type']}")
        return converters

    def _safe_convert(self, convert_func):
        def wrapper(x):
            if x is None:
                return None
            try:
                return convert_func(x)
            except ValueError:
                return None

        return wrapper        

    def _json_to_pyarrow_type(self, json_type):
        if json_type == 'string':
            return pa.string()
        elif json_type == 'int':
            return pa.int32()
        elif json_type == 'long':
            return pa.int64()
        elif json_type == 'float':
            return pa.float32()
        elif json_type == 'double':
            return pa.float64()
        elif json_type == 'boolean':
            return pa.bool_()
        elif json_type == 'timestamp':
            return pa.timestamp('ns')
        elif json_type == 'decimal':
            return pa.decimal128(30, 0)
        else:
            raise ValueError(f"Unsupported JSON type: {json_type}")

    def get_field_names(self):
        return [field['name'] for field in self.schema_dict['fields']]

    def get_schema(self):
        return pa.schema([pa.field(field['name'], self._json_to_pyarrow_type(field['type'])) for field in self.schema_dict['fields']])

    def _make_schema_hdfs_path(self, hdfs_dir):
        match = re.search(r"/db=(\w+)/tb=(\w+)", hdfs_dir)
        if match:
            return f"hdfs://dataGWCluster/schema/{match.group(1)}.{match.group(2)}.json"
        else:
            raise Exception("no db name and table name found in hdfs_dir")
