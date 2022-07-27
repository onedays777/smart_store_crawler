import time
import os
import sys
import os.path
import logging
from bs4 import BeautifulSoup
from urllib import parse
from threading import Thread
from datetime import datetime
from urllib.parse import urlparse, parse_qs, parse_qsl
import gc
import json
import numpy as np

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import networkLib
from smart_store_crawler import DBLib
from smart_store_crawler import naverShopping
from smart_store_crawler import jobStoreProfile
from smart_store_crawler import jobProductDetail
from smart_store_crawler import jobCategory
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler.config import DB_JOB_TYPE
from smart_store_crawler.config import LOGLEVEL

"""
proxy_manager = proxyManager(True)
brand_list = naverShopping.get_sub_category_list(proxy_manager, "100000001")
pass
"""
# Test Code Begin
"""
category_code = '100000383'
db_job_type = DB_JOB_TYPE.INSERT_STORE_PROFILE
"""
# Test Code End

# Default Config
"""
config.CFG_LOGLEVEL = commonLib.readConfigInt('log_level', int(config.CFG_LOGLEVEL))
commonLib.writeConfig('log_level', config.CFG_LOGLEVEL)

config.CFG_PROXY_FILENAME = commonLib.readConfigString('proxy_filename', config.CFG_PROXY_FILENAME)
commonLib.writeConfig('proxy_filename', config.CFG_PROXY_FILENAME)
"""


# dynamodb is not thread safe
# dbPoolCount = commonLib.readConfigInt('db_pool_count', 40) 
# commonLib.writeConfig('db_pool_count', dbPoolCount)
# dbPooler = DBPooler(dbPoolCount)
# item = DBLib.select_naver_shopping_outlink()
# naverShopping.get_product_list(proxy_manager, "100000022", 1000)

"""
items = DBLib.select_store_profile_list()
emails = []
for item in items:
	emails.append(item.get('chrgrEmail'))

print("\n".join(emails))
"""

"""
count = DBLib.select_store_profile_count()
print(count)
time.sleep(1000)

#item = DBLib.select_store_profile_list() # 500289369
item = DBLib.select_store_visitor_list()
"""

"""
while (True):
	count = DBLib.select_store_profile_count()
	print("select_store_profile_count : " + str(count))
	count = DBLib.select_naver_shopping_outlink_count()
	print("select_naver_shopping_outlink_count : " + str(count))

	time.sleep(3)
"""

commonLib.print_log(LOGLEVEL.I, "Main : Begin")

args = sys.argv

main_category = None
sub_category = None

if len(args) > 1:
	db_job_type = DB_JOB_TYPE(args[1])
if len(args) > 2:
	config.CFG_PROXY_FILENAME = args[2]
if len(args) > 3:
	main_category = args[3]
if len(args) > 4:
	sub_category = args[4]

commonLib.print_log(LOGLEVEL.I, "Main : Load Proxy File = " + config.CFG_PROXY_FILENAME)

proxy_manager = proxyManager(True)
proxy_manager.changeProxy()


if db_job_type in [DB_JOB_TYPE.INSERT_CATEGORY_HOT_KEYWORD_BRAND]:
	dynamodb = DBLib.create_connection()

	jobCategory.category_keyword_brand_to_db(proxy_manager, dynamodb)

	commonLib.print_log(LOGLEVEL.I, "Main : Job Is Done")

	sys.exit()


sub_category_list = []

if sub_category:
	if sub_category in config.CFG_NAVER_SHOPPING_CATEGORY_CODE_LIST:
		sub_category_list, is_end_category = naverShopping.get_sub_category_list(proxy_manager, sub_category)
	else:
		sub_category_list.append(sub_category)
else:
	for sub_category in config.CFG_NAVER_SHOPPING_CATEGORY_CODE_LIST:
		list, is_end_category = naverShopping.get_sub_category_list(proxy_manager, sub_category)
		sub_category_list = np.concatenate((sub_category_list, list))


for sub_category in sub_category_list:
	sub_category_code = sub_category['code']

	pageIndex = 1

	while True:
		# TODO : 가격비교 탭만 돌리기 위해서
		product_list, referer_url, is_last_page = naverShopping.get_product_list(proxy_manager,
												sub_category_code, pageIndex, config.CFG_NAVER_SHOPPING_TAB_CATALOG)

		if product_list is None:
			break

		pageIndex += 1

		tHandles = []

		for product in product_list:
			proxy_manager.changeProxy()

			if db_job_type in [DB_JOB_TYPE.INSERT_STORE_PROFILE, DB_JOB_TYPE.INSERT_STORE_PROFILE_FORCE]:
				is_force = (db_job_type == DB_JOB_TYPE.INSERT_STORE_PROFILE_FORCE)
				dynamodb = DBLib.create_connection()

				th = Thread(target=jobStoreProfile.store_profile_to_db, args=(proxy_manager, product, is_force, referer_url, dynamodb))
				th.start()
				tHandles.append(th)
				time.sleep(3)

			elif db_job_type in [DB_JOB_TYPE.INSERT_PRODUCT, DB_JOB_TYPE.INSERT_PRODUCT_FORCE]:
				is_force = (db_job_type == DB_JOB_TYPE.INSERT_PRODUCT_FORCE)
				dynamodb = DBLib.create_connection()

				#jobProductDetail.product_detail_to_db(proxy_manager, product, is_force, referer_url, dynamodb)

				th = Thread(target=jobProductDetail.product_detail_to_db, args=(proxy_manager, product, is_force, referer_url, dynamodb))
				th.start()
				tHandles.append(th)
				time.sleep(3)


		del product_list

		for th in tHandles:
			th.join()

		del tHandles
		gc.collect()

		if is_last_page:
			break



