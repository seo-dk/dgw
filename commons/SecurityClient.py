import requests
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import time
from airflow.models import Variable

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class SecurityClient:

    def __init__(self):
        # api_key/prefix_url을 코드에 하드코딩하지 않고 Airflow Variable에서 주입받습니다.
        prefix_url = Variable.get("xdr_security_prefix_url", default_var=None)
        api_key = Variable.get("xdr_security_api_key", default_var=None)

        if not prefix_url:
            raise ValueError(
                "Airflow Variable 'xdr_security_prefix_url' is missing or empty. (XDR prefix_url)"
            )

        if not api_key:
            raise ValueError(
                "Airflow Variable 'xdr_security_api_key' is missing or empty. (XDR api_key)"
            )

        self.api_key = api_key
        self.prefix_url = prefix_url
        self.user_id = "DATAGW"

    def send_request(self, url, method, data=None):
        headers = {
            'Content-Type': 'application/json',
            'X-Api-Key': self.api_key
        }
        request_method = getattr(requests, method.lower(), None)

        if not request_method:
            raise ValueError(f"Unsupported HTTP method: {method}")

        try:
            response = request_method(url, headers=headers, json=data, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def request_enc_dec(self, type, value, action):
        for attempt in range(5):
            try:
                url = f"https://{self.prefix_url}/security/xdr/{action}/{type}"
                data = {'datas': value, 'userId': self.user_id} if value is not None else None
                return self.send_request(url, "POST", data).get('datas')
            except Exception as e:
                logger.error(f"Exception on attempt {attempt + 1}: {e}", exc_info=True)
                time.sleep(1)
        raise RuntimeError(f"Failed after 5 attempts: {e}")

    def encrypt(self, type, values):
        return self.request_enc_dec(type, values, "enc")

    def decrypt(self, type, values):
        return self.request_enc_dec(type, values, "dec")

    def request_key(self, type):
        result = None
        last_ex_msg = None
        for i in range(5):
            try:
                url = self.get_url("key", type)
                response = self.send_request(url, "GET")
                if response and 'datas' in response:
                    result = response['datas'][0]
                    if result:
                        break
            except Exception as e:
                last_ex_msg = str(e)
                logger.error(f"Exception on attempt {i + 1}: {e}", exc_info=True)
                time.sleep(1)

        if last_ex_msg:
            raise RuntimeError(last_ex_msg)
        return result

    def get_url(self, type, column):
        return f"https://{self.prefix_url}/security/xdr/{type}/{column}"

    def get_keys(self):
        cipher_keys = {}
        for ent_type in ['imsi', 'mdn', 'enb', 'svrcd']:
            try:
                cipher_key = self.request_key(ent_type)
                if len(cipher_key) != 16:
                    raise ValueError(f"Key length for {ent_type} must be 16 bytes.")
                cipher_keys[ent_type] = cipher_key
            except Exception as e:
                logger.error(f"Failed to create cipher. type: {ent_type}, {str(e)}")
                raise
        return cipher_keys

if __name__ == '__main__':
    client = SecurityClient()
    value = "bca94c334022eaa53b5a96fdf5d75a91"
    print(f"encrypt value : {value}")
    encrypted_values = client.encrypt('imsi', [value, value])
    print(encrypted_values)
    decrypted_values = client.decrypt('imsi', encrypted_values)
    print(decrypted_values)
    key_value = client.request_key('enb')
    print(key_value)
