import time
import os
import sys
import os.path
from bs4 import BeautifulSoup
from urllib import parse
from threading import Thread
from datetime import datetime
import logging
import subprocess

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import DBLib
from smart_store_crawler import naverShopping
from smart_store_crawler import jobStoreProfile
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler.config import DB_JOB_TYPE
from smart_store_crawler.config import LOGLEVEL


commonLib.print_log(LOGLEVEL.I, "Program Begin")

main_category = None

args = sys.argv

if len(args) > 1:
	main_category = args[1]

if len(args) > 2:
	config.CFG_PROXY_FILENAME = args[2]


proxy_manager = proxyManager(True)

db_job_type = DB_JOB_TYPE.INSERT_STORE_PROFILE.name

if main_category is not None:
	proxy_manager.changeProxy()

	sub_category_list, is_end_category = naverShopping.get_sub_category_list(proxy_manager, main_category)

	for sub_category in sub_category_list:
		sub_category_code = sub_category['code']

		cmd = f"python.exe main.py {db_job_type} {config.CFG_PROXY_FILENAME} {main_category} {sub_category_code}"

		if os.path.exists('main.exe'):
			cmd = f"main.exe {db_job_type} {config.CFG_PROXY_FILENAME} {main_category} {sub_category_code}"

		commonLib.print_log(LOGLEVEL.D, "cmd = %s" % cmd)

		subprocess.Popen(cmd, close_fds=True)
else:
	for main_category in config.CFG_NAVER_SHOPPING_CATEGORY_CODE_LIST:
		proxy_manager.changeProxy()

		sub_category_list, is_end_category = naverShopping.get_sub_category_list(proxy_manager, main_category)

		for sub_category in sub_category_list:
			sub_category_code = sub_category['code']

			cmd = f"python.exe main.py {db_job_type} {config.CFG_PROXY_FILENAME} {main_category} {sub_category_code}"

			if os.path.exists('main.exe'):
				cmd = f"main.exe {db_job_type} {config.CFG_PROXY_FILENAME} {main_category} {sub_category_code}"

			commonLib.print_log(LOGLEVEL.D, "cmd = %s" % cmd)

			subprocess.Popen(cmd, close_fds=True)
