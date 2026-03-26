import requests
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class XdrSecurityClient:

    def __init__(self, prefix_url, api_key, user_id="MFAS"):
        self.api_key = api_key
        self.prefix_url = prefix_url
        self.user_id = user_id

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

    def request_enc_dec(self, column, value, action):
        for attempt in range(5):
            try:
                url = f"https://{self.prefix_url}/security/xdr/{action}/{column}"
                data = {'datas': value, 'userId': self.user_id} if value is not None else None
                return self.send_request(url, "POST", data).get('datas')
            except Exception as e:
                logger.error(f"Exception on attempt {attempt + 1}: {e}", exc_info=True)
                time.sleep(1)
        raise RuntimeError(f"Failed after 5 attempts: {e}")

    def encrypt(self, column, values):
        return self.request_enc_dec(column, values, "enc")

    def decrypt(self, column, values):
        return self.request_enc_dec(column, values, "dec")

if __name__ == '__main__':
    client = XdrSecurityClient("104.104.105.209:8443", "1MflcTFE54")
    encrypted_values = client.encrypt('mdn', ["1234", "5678"])
    print(encrypted_values)
    decrypted_values = client.decrypt('mdn', ['Qqof0qGJPazkMShuPW1gvA==', 'jJQT9j2wMHf7Fp+r6kJRQw=='])
    print(decrypted_values)
