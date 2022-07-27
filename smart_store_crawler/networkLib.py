import os
import random
import string
import time
import platform
import traceback
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from smart_store_crawler.proxyLib import proxyManager


def retry_req_get(url, header, proxy_server):
    res = False

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    with requests.Session() as s:
        try:
            retries = 2
            backoff_factor = 0.3
            status_forcelist = (500, 502, 504)

            retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                          status_forcelist=status_forcelist)

            adapter = HTTPAdapter(max_retries=retry)
            s.mount('http://', adapter)
            s.mount('https://', adapter)
            res = s.get(url, headers=header, proxies=proxy_server, verify=False, timeout=5)
        except:
            traceback.print_exc()

    return res


def retry_req_post(url, header, data, proxy_server):
    res = False

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    with requests.Session() as s:
        try:
            retries = 2
            backoff_factor = 0.3
            status_forcelist = (500, 502, 504)

            retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                          status_forcelist=status_forcelist)

            adapter = HTTPAdapter(max_retries=retry)
            s.mount('http://', adapter)
            s.mount('https://', adapter)
            res = s.post(url, data, headers=header, proxies=proxy_server, verify=False, timeout=5)
        except:
            traceback.print_exc()

    return res


def retry_req_json(url, header, data, proxy_server):
    res = False

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    header['content-type'] = 'application/json'

    with requests.Session() as s:
        try:
            retries = 2
            backoff_factor = 0.3
            status_forcelist = (500, 502, 504)

            retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                          status_forcelist=status_forcelist)

            adapter = HTTPAdapter(max_retries=retry)
            s.mount('http://', adapter)
            s.mount('https://', adapter)
            res = s.post(url, data=json.dumps(data), headers=header, proxies=proxy_server, verify=False, timeout=5)
        except:
            traceback.print_exc()

    return res


def req_get_on_proxy(url, header, proxy_manager: proxyManager):
    html = ''
    final_url = None

    for i in range(0, 999):
        res = retry_req_get(url, header, proxy_manager.getCurrentProxy())

        if not res:
            if not proxy_manager.isLoadedProxy():
                break

            proxy_manager.changeProxy()
            time.sleep(3)
            continue

        html = res.text
        final_url = res.url

        break

    return html, final_url


def req_json_on_proxy(url, header, data, proxy_manager: proxyManager):
    html = ''
    final_url = None

    for i in range(0, 999):
        res = retry_req_json(url, header, data, proxy_manager.getCurrentProxy())

        if not res:
            if not proxy_manager.isLoadedProxy():
                break

            proxy_manager.changeProxy()
            time.sleep(3)
            continue

        html = res.text
        final_url = res.url

        break

    return html, final_url
