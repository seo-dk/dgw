import logging
from airflow.models import Variable
import socks
import socket
import subprocess
import psutil

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

class ProxyManager:
    def __init__(self, proxy_setting):
        self.proxy_setting = proxy_setting
        self.ssh_process = None

    def start_proxy(self):
        try:

            ips = self._get_all_ips()
            proxy_ip = self.proxy_setting["ip"]
            proxy_id = self.proxy_setting["id"]
            proxy_port = self.proxy_setting["port"]

            logger.info(f"all_ips : {ips}")
            logger.debug("proxy_ip : %s", proxy_ip)
            logger.debug("local ips : %s", ips)

            if not any(proxy_ip == host_ip for host_ip in ips):
                logger.info(f"Setting up proxy on {socket.gethostname()} to route traffic through {proxy_ip} server")
                self.ssh_process = subprocess.Popen(
                    ## -D를 이용해서 1080 포트를 동적 포트 포워딩 시켜준다. -N 을통해서 명령어 실행 안하고 프록시역할만 수행하도록 하고 -f 로 해당 세션 백그라운드 실행
                    ["ssh", "-D", proxy_port, "-N", "-f", f"{proxy_id}@{proxy_ip}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

                socks.set_default_proxy(socks.SOCKS5, "localhost", int(proxy_port))
                socket.socket = socks.socksocket

                logger.info("success...")
            else:
                logger.info(f"local host is same with proxy_ip, local_host:{ips}, proxy_ip : {proxy_ip}")
        except Exception as e:
            logger.exception("Error during setting up proxy server")
            raise Exception(f"Error setting proxy, {e}")

    def _get_all_ips(self):
        ips = []
        for iface_name, iface_addresses in psutil.net_if_addrs().items():
            for address in iface_addresses:
                if address.family == socket.AF_INET:
                    ips.append(address.address)
        return ips

    def stop_proxy(self):
        if self.ssh_process:
            self.ssh_process.terminate()
