import time
import os
import sys
import os.path
from threading import Thread
from datetime import datetime
import logging
import gc

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import DBLib
from smart_store_crawler import naverShopping
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler.config import DB_JOB_TYPE
from smart_store_crawler.config import LOGLEVEL


def category_keyword_brand_to_db_recursion(proxy_manager: proxyManager, p_category_code, p_category_name,
                                           dynamodb=None):
    proxy_manager.changeProxy()

    product_count = naverShopping.get_category_product_count(proxy_manager, p_category_code)

    sub_category_list, is_end_category = naverShopping.get_sub_category_list(proxy_manager, p_category_code)
    category_brand_list = naverShopping.get_category_brand_list(proxy_manager, p_category_code)

    DBLib.insert_category_sub_category(p_category_code, p_category_name, product_count, sub_category_list, dynamodb)
    DBLib.insert_category_brand(p_category_code, p_category_name, category_brand_list, dynamodb)

    # category_code 와 categoryId 는 다름 : url 에 보이는게 category_code 이고 Naver html 내부에서 사용하는게 categoryId
    # 눈에 보이는 category_code 를 DB 에 저장한다.
    categoryId = naverShopping.get_category_id_by_category_code(proxy_manager, p_category_code)

    if categoryId is not None:
        hot_keyword = naverShopping.get_category_hot_keyword(proxy_manager, p_category_code, categoryId)
        hot_brand = naverShopping.get_category_hot_brand(proxy_manager, p_category_code, categoryId)

        DBLib.insert_category_hot_keyword(p_category_code, p_category_name, hot_keyword, dynamodb)
        DBLib.insert_category_hot_brand(p_category_code, p_category_name, hot_brand, dynamodb)

    for category in sub_category_list:
        category_code = category['code']
        category_name = category['name']

        if not is_end_category:
            category_keyword_brand_to_db_recursion(proxy_manager, category_code, category_name, dynamodb)

    del sub_category_list
    del category_brand_list
    del hot_keyword
    del hot_brand

    gc.collect()


def category_keyword_brand_to_db(proxy_manager: proxyManager, dynamodb=None):
    code_count = len(config.CFG_NAVER_SHOPPING_CATEGORY_CODE_LIST)

    for i in range(0, code_count):
        category_code = config.CFG_NAVER_SHOPPING_CATEGORY_CODE_LIST[i]
        category_name = config.CFG_NAVER_SHOPPING_CATEGORY_NAME_LIST[i]

        category_keyword_brand_to_db_recursion(proxy_manager, category_code, category_name, dynamodb)

        #th = Thread(target=category_keyword_brand_to_db_recursion,
        #            args=(proxy_manager, category_code, category_name, dynamodb))
        #th.start()

