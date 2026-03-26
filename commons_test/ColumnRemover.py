import logging
import os

from typing import List

logger = logging.getLogger(__name__)

class ColumnRemover:
    def __init__(self, file_full_path, delete_columns: List[int], delimiter):
        self.file_full_path = file_full_path
        self.delete_columns = delete_columns
        self.delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")

    def _delete(self, file_lines, write_file, file_delimiter):
        delete_columns = self.delete_columns
        sorted(delete_columns, reverse=True)
        logger.info("delete_columns value : %s", delete_columns)
        with open(write_file, 'w') as tmp_file:
            for line_arr in file_lines:
                for idx in delete_columns:
                    del line_arr[idx - 1]
                write_line = file_delimiter.join(line_arr)
                logger.info("write line : %s", write_line)
                tmp_file.write(write_line)

    def start(self):
        if self.delete_columns:
            logger.info("delete column in particular index")
            tmp_file_path = str(self.file_full_path) + ".tmp"
            lines = []
            with open(self.file_full_path) as local_file:
                while True:
                    line = local_file.readline()
                    if not line:
                        break
                    line_arr = line.split(self.delimiter)
                    if len(line_arr) > 1:
                        lines.append(line_arr)
            self._delete(lines, tmp_file_path, self.delimiter)
            logger.info("completed to delete columns")
            if os.path.exists(tmp_file_path):
                os.rename(tmp_file_path, self.file_full_path)