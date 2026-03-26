import json
import urllib3
from airflow.models import Variable

VAULT_IP = "90.90.42.228"
VAULT_ADDR = "https://vault.sktelecom.com:8200"
VAULT_NAMESPACE = "prod_Data-gateway"
VAULT_SECRET_PATH = "secret/data/encrypt-keys"

_addr_no_scheme = VAULT_ADDR.split("://", 1)[-1]
_VAULT_HOST, _VAULT_PORT_STR = _addr_no_scheme.rsplit(":", 1)
_VAULT_PORT = int(_VAULT_PORT_STR)

_pool = urllib3.HTTPSConnectionPool(
    host=VAULT_IP,
    port=_VAULT_PORT,
    assert_hostname=_VAULT_HOST,
    server_hostname=_VAULT_HOST,
    cert_reqs="CERT_NONE",
)


def _request(method, path, headers=None, body=None):
    hdrs = {"Host": _VAULT_HOST}
    if headers:
        hdrs.update(headers)
    resp = _pool.urlopen(
        method,
        path,
        headers=hdrs,
        body=json.dumps(body).encode() if body else None,
    )
    if resp.status >= 400:
        raise Exception(f"Vault API error: {resp.status} {resp.data.decode()}")
    return json.loads(resp.data.decode())


def get_vault_token():
    """AppRole 로그인으로 Vault 토큰 발급 (2분 TTL, 1회용 - Role 설정)"""
    role_id = Variable.get("vault_role_id")
    secret_id = Variable.get("vault_secret_id")
    data = _request(
        "POST",
        "/v1/auth/approle/login",
        headers={"X-Vault-Namespace": VAULT_NAMESPACE},
        body={"role_id": role_id, "secret_id": secret_id},
    )
    return data["auth"]["client_token"]


def get_encrypt_keys():
    token = get_vault_token()
    data = _request(
        "GET",
        f"/v1/{VAULT_SECRET_PATH}",
        headers={
            "X-Vault-Token": token,
            "X-Vault-Namespace": VAULT_NAMESPACE,
        },
    )
    return data["data"]["data"]


if __name__ == "__main__":
    keys = get_encrypt_keys()
    print(keys)

