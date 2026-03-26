import socket
import base64
import logging
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from airflow.models import Variable
from commons_test.SecurityClient import SecurityClient

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))
class CryptoClient:
    def __init__(self, cipher_keys):
        self.cipher_keys = cipher_keys

    def enc(self, enc_type, value):
        try:
            cipher_key = self.cipher_keys[enc_type]
            if enc_type == 'imsi' and len(value) < 15:
                return ""
            else:
                prefix = ""
                if enc_type == 'imsi':
                    prefix = "T" + value[5] if value.startswith("45005") else "O" + value[5]
                iv = cipher_key[:16].encode('utf-8')
                key = cipher_key.encode('utf-8')
                enc_ciphers = AES.new(key, AES.MODE_CBC, iv)
                encrypted = enc_ciphers.encrypt(pad(value.encode('utf-8'), AES.block_size))
                return prefix + base64.b64encode(encrypted).decode('utf-8')
        except Exception as e:
            if value is None:
                logger.error("value is None")
            raise Exception(e)

    def dec(self, enc_type, value):
        try:
            cipher_key = self.cipher_keys[enc_type]
            if len(value) > 0:
                iv = cipher_key[:16].encode('utf-8')
                key = cipher_key.encode('utf-8')
                cipher = AES.new(key, AES.MODE_CBC, iv)
                if enc_type == 'imsi':
                    bytes_value = base64.b64decode(value[2:])
                else:
                    bytes_value = base64.b64decode(value)
                decrypted = unpad(cipher.decrypt(bytes_value), AES.block_size).decode('utf-8')
                return decrypted
            else:
                return ""
        except Exception as e:
            raise Exception(f"Decryption exception: type[{enc_type}], value:[{value}], {str(e)}")

