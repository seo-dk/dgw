import socket
import base64
import logging
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from SecurityClient import SecurityClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("encrypt.log", mode='a')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)
class CryptoClient:
    def __init__(self, cipher_keys):
        self.cipher_keys = cipher_keys
        logger.info("CryptoClient created.")

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
        return None

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
            logger.error(f"Decryption exception: type[{enc_type}], value:[{value}], {str(e)}")
        return None


if __name__ == "__main__":
    cipher_keys = {'imsi': '636c64686b64686b', 'mdn': '676b73666b746b73', 'enb': '603f20dd57cd4747', 'svrcd': '776c73656874726f'}
    cryptoClient = CryptoClient(cipher_keys)

    # dec_imsi = cryptoClient.dec('imsi', 'T01nb0pi6qi4D8vL5xSe1YSw==')
    # print(f'dec_imsi: {dec_imsi}')
    cipher_key = cryptoClient.cipher_keys
    enc_values = ["9rxnMb59zxDKJyTk84BSpw=="]
    dec_imsi1 = cryptoClient.dec('mdn', enc_values[0])
    print(f'decrypt result: {dec_imsi1}')

