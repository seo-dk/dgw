import time
import json
import random
import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from airflow.hooks.base import BaseHook

class VaultCrypto:

    VAULT_CONNECTIONS = [
        {"conn_id": "vault_airflow_1", "weight": 70},
        {"conn_id": "vault_airflow_2", "weight": 30},
    ]

    def __init__(self, max_retries=3, retry_delay=2):
        self.aes_key       = None
        self.aes_iv        = None
        self.dgw_kafka_pwd = None
        self.max_retries   = max_retries
        self.retry_delay   = retry_delay

        requests.packages.urllib3.disable_warnings()
        self._load_all_secrets()

    def _select_connection(self):
        """Select connection based on weight"""
        conn_ids = [c["conn_id"] for c in self.VAULT_CONNECTIONS]
        weights  = [c["weight"]  for c in self.VAULT_CONNECTIONS]
        selected = random.choices(conn_ids, weights=weights, k=1)[0]
        print(f"[Vault] Selected connection : {selected}")
        return selected

    def _get_secret(self, conn_id, tag):
        """Fetch secret from Vault"""
        conn     = BaseHook.get_connection(conn_id)
        extra    = json.loads(conn.extra) if conn.extra else {}
        url      = conn.host + tag
        headers  = {'X-Vault-Namespace': extra.get("namespace", "")}
        print(f"[Vault] Connecting to : {url}")
        resp = requests.get(url, headers=headers, timeout=(2, 5), verify=False)
        resp.raise_for_status()
        return resp.json()["data"]

    def _execute_with_retry(self, tag):
        """Common retry and fallback handler"""
        primary    = self._select_connection()
        conn_ids   = [c["conn_id"] for c in self.VAULT_CONNECTIONS]
        fallbacks  = [c for c in conn_ids if c != primary]
        ordered    = [primary] + fallbacks
        last_error = None

        for conn_id in ordered:
            for attempt in range(1, self.max_retries + 1):
                try:
                    return self._get_secret(conn_id, tag)
                except Exception as e:
                    last_error = e
                    print(f"[Vault] Failed (conn: {conn_id}, attempt: {attempt}/{self.max_retries}): {e}")
                    if attempt < self.max_retries:
                        print(f"[Vault] Retrying in {self.retry_delay} second(s)...")
                        time.sleep(self.retry_delay)

            print(f"[Vault] Switching to fallback connection...")

        raise RuntimeError(f"[Vault] All connections failed. Last error: {last_error}")

    def _load_all_secrets(self):
        """Load all secrets from Vault for all tags"""

        # 1. airflow tag -> load aes_key / aes_iv
        secret       = self._execute_with_retry("airflow")
        self.aes_key = secret['encrytkey'].encode('utf-8')
        self.aes_iv  = secret['encrytiv'].encode('utf-8')

        if len(self.aes_key) != 32:
            raise ValueError(f"Invalid AES KEY length: {len(self.aes_key)}")
        if len(self.aes_iv) != 16:
            raise ValueError(f"Invalid AES IV length: {len(self.aes_iv)}")

        print(f"[Vault] airflow KEY/IV loaded successfully")

        # 2. kafka tag -> load dgw_kafka_pwd
        secret             = self._execute_with_retry("kafka")
        self.dgw_kafka_pwd = secret['dgw_pwd']

        print(f"[Vault] kafka PWD loaded successfully")

    def decrypt(self, hex_str: str) -> str:
        """Decrypt HEX string using AES-256-CBC"""
        try:
            encrypted = bytes.fromhex(hex_str)
            cipher    = Cipher(algorithms.AES(self.aes_key), modes.CBC(self.aes_iv))
            decryptor = cipher.decryptor()
            padded    = decryptor.update(encrypted) + decryptor.finalize()
            unpadder  = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
            return plaintext.decode('utf-8')
        except Exception as e:
            raise ValueError(f"[Decrypt] Decryption failed: {e}")


if __name__ == "__main__":
    client = VaultCrypto()

    # Print kafka password
    print(f"Kafka PWD : {client.dgw_kafka_pwd}")

    # Decrypt encrypted value
    enc_text = "4082E0E5F112EC6BB0CDDF4DCA0D35E4"
    dec_text = client.decrypt(enc_text)
    print(f"Decrypted : {dec_text}")
