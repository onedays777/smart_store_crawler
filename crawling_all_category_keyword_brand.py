import time
import os
import sys
import os.path
import subprocess

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import naverShopping
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler.config import DB_JOB_TYPE
from smart_store_crawler.config import LOGLEVEL


commonLib.print_log(LOGLEVEL.I, "Program Begin")

args = sys.argv

proxy_manager = proxyManager(True)

db_job_type = DB_JOB_TYPE.INSERT_CATEGORY_HOT_KEYWORD_BRAND.name

cmd = f"python.exe main.py {db_job_type}"

if os.path.exists('main.exe'):
    cmd = f"main.exe {db_job_type}"

commonLib.print_log(LOGLEVEL.D, "cmd = %s" % cmd)

subprocess.Popen(cmd, close_fds=True)
