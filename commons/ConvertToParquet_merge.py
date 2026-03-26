import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import glob
import os
import time
from io import StringIO
from decimal import Decimal
import fastparquet as fp
import warnings

from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.SchemaInfoManager import SchemaInfoManager

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)

class ConvertToParquet:

    def __init__(self, source_info: SourceInfo, interface_info: InterfaceInfo):
        self.source_info = source_info
        self.interface_info = interface_info
        self.schema_info_manager = SchemaInfoManager(self.source_info.hdfs_dir)

        self.json_fields = None
        self.compression = None
        self.delimiter = None
        self.encode = None

        if self.interface_info.protocol_cd == 'FTP' or self.interface_info.protocol_cd == 'SFTP':
            self.json_fields = self.schema_info_manager.get_schema_dict()
            self.compression = self.source_info.file_proto_info.target_compression
            self.delimiter = self._check_delimiter(self.interface_info.ftp_proto_info.delimiter)
            self.encode = self.interface_info.ftp_proto_info.encode


    def start_convert(self, local_file_path, parquet_local_full_path):
        try:
            logger.info("start convert to parquet")
            field_names = [field['name'] for field in self.json_fields['fields']]
            fields = [pa.field(field['name'], self._json_to_pyarrow_type(field['type'])) for field in self.json_fields['fields']]
            schema_arrow = pa.schema(fields)

            # pandas_dtype = {field['name']: self._json_to_pyarrow_type(field['type']).to_pandas_dtype() for field in schema_fields}
            parquet_local_dir = os.path.dirname(parquet_local_full_path)

            if self.compression not in parquet_local_full_path:
                parquet_file_base, parquet_extension = os.path.splitext(parquet_local_full_path)
                parquet_extension = "."+self.compression + parquet_extension
                parquet_local_full_path = parquet_file_base + parquet_extension

            if os.path.exists(parquet_local_dir):
                start_time = time.time()

                # logger.info("schemas : %s", fields)

                converters = self._get_converters(self.json_fields)

                df_iter = pd.read_csv(local_file_path, chunksize=1e5, header=None, names=field_names, converters=converters,
                                      sep=self.delimiter, encoding=self.encode, na_values=['', ' '], engine='python')

                for i, chunk in enumerate(df_iter):
                    chunk.to_parquet(parquet_local_full_path, engine='fastparquet', compression=self.compression, index=False, append=False if i == 0 else True)

                end_time = time.time()
                execution_time = end_time - start_time

                logger.info("%s is created", parquet_local_full_path)
                logger.info("execution time: %s secs", execution_time)
            else:
                logger.warning("%s does not exist", parquet_local_dir)
                raise Exception("Error during convert, %s does not exist", parquet_local_dir)
        except Exception as e:
            logger.error("Error converting %s : %s", parquet_local_full_path, str(e))
            raise Exception(str(e))

    def _json_to_pyarrow_type(self, json_type):
        if json_type == 'string':
            return pa.string()
        elif json_type == 'int':
            return pa.int32()
        elif json_type == 'bigint':
            return pa.float64()
        elif json_type == 'float':
            return pa.float64()
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

    def _safe_convert(self, convert_func):
        def wrapper(x):
            if x is None:
                return None
            try:
                return convert_func(x)
            except ValueError:
                return None
        return wrapper

    def _get_converters(self, schema_dict):
        converters = {}
        for field in schema_dict['fields']:
            if field['type'] == 'string':
                converters[field['name']] = self._safe_convert(str)
            elif field['type'] == 'int':
                converters[field['name']] = self._safe_convert(lambda x: int(x.strip()))
            elif field['type'] in ['float', 'double', 'bigint']:
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

    def _check_delimiter(self, delimiter):
        logger.info("checking delimiter")
        delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")
        to_byte = delimiter.encode("utf-8")
        byte_size = len(to_byte)

        if byte_size > 1:
            logger.debug("delimter should be changed")
            if delimiter == "|^|" or delimiter == ",|'" or delimiter == "\",\"" :
                changed_delimiter = bytes("\\u001e", "utf-8").decode("unicode_escape")
                logger.info("delimiter changed to %s -> %s", delimiter, changed_delimiter)
                return changed_delimiter
        else:
            return delimiter