import time
import os
import sys
import os.path
from threading import Thread
from datetime import datetime
import logging

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import DBLib
from smart_store_crawler import naverShopping
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler.config import DB_JOB_TYPE
from smart_store_crawler.config import LOGLEVEL


# 상품상세가 가격비교인 경우 각각의 상품에 대해 다시 확인
def store_profile_to_db_by_catalog(proxy_manager: proxyManager, product_list, is_force, referer_url, dynamodb=None):
    for product in product_list:
        url = product.get('mobileProductUrl', None)

        if url is None:
            url = product.get('pcProductUrl', None)

        if url is None:
            continue

        item = DBLib.select_naver_shopping_outlink(url, dynamodb)

        if item is not None:
            del item
            commonLib.print_log(LOGLEVEL.D, "store_profile_to_db_by_catalog: outlink url = " + url)
            continue

        proxy_manager.changeProxy()
        parsed_data, is_catalog_product, final_url = naverShopping.get_product_detail(proxy_manager, url, referer_url)

        if final_url is not None:
            if parsed_data is None and not commonLib.is_naver_url(final_url):
                DBLib.insert_naver_shopping_outlink(url, dynamodb)
                commonLib.print_log(LOGLEVEL.D, "store_profile_to_db_by_catalog: unknown url")
                continue

        channelNo = parsed_data.get('channelNo', None)
        channelSiteProfileUrl = parsed_data.get('channelSiteProfileUrl', None)

        if channelNo is None or channelSiteProfileUrl is None:
            continue

        if not is_force:
            item = DBLib.select_store_profile_list(channelNo, dynamodb)

            if item is not None:
                del item
                commonLib.print_log(LOGLEVEL.D,
                                    "store_profile_to_db_by_catalog: exists store profile = " + str(channelNo))

                continue

        del parsed_data

        parsed_data = naverShopping.get_store_profile(proxy_manager, channelSiteProfileUrl)

        commonLib.print_log(LOGLEVEL.D,
                            "store_profile_to_db_by_catalog: insert_store_profile = " + parsed_data.get('channelName',
                                                                                                        ''))
        DBLib.insert_store_profile(parsed_data, dynamodb)

        del parsed_data


def store_profile_to_db(proxy_manager: proxyManager, product, is_force, referer_url, dynamodb=None):
    url = product.get('crUrl', '')

    if url == '':
        url = product.get('adcrUrl', '')

    item = DBLib.select_naver_shopping_outlink(url, dynamodb)

    if item is not None:
        # commonLib.print_log(LOGLEVEL.D, "store_profile_to_db: outlink url = " + url)
        return

    parsed_data, is_catalog_product, final_url = naverShopping.get_product_detail(proxy_manager, url, referer_url)

    if final_url is not None:
        if parsed_data is None and not commonLib.is_naver_url(final_url):
            DBLib.insert_naver_shopping_outlink(url, dynamodb)
            return

    if is_catalog_product:
        time.sleep(3)

        product_list = naverShopping.get_product_catalog_list(proxy_manager, parsed_data)
        store_profile_to_db_by_catalog(proxy_manager, product_list, is_force, parsed_data.get('catalog_url', ''),
                                       dynamodb)

        del product_list

        return

    channelNo = parsed_data.get('channelNo', None)
    channelSiteProfileUrl = parsed_data.get('channelSiteProfileUrl', None)

    if channelNo is None or channelSiteProfileUrl is None:
        return

    if not is_force:
        item = DBLib.select_store_profile_list(channelNo, dynamodb)

        if item is not None:
            del item

            #commonLib.print_log(LOGLEVEL.D, "store_profile_to_db: exists store profile = " + str(channelNo))

            return

    del parsed_data

    parsed_data = naverShopping.get_store_profile(proxy_manager, channelSiteProfileUrl)

    if parsed_data is None:
        return

    commonLib.print_log(LOGLEVEL.D, "store_profile_to_db: insert_store_profile = " + parsed_data.get('channelName', ''))
    DBLib.insert_store_profile(parsed_data, dynamodb)

    channelNo = parsed_data.get('channelNo', None)

    del parsed_data

    """
	# 방문자 정보 입력
	if channelNo != None:
		parsed_data = naverShopping.get_store_visitor(proxy_manager, channelNo, channelSiteProfileUrl)

		if parsed_data != None:
			parsed_data['channelNo'] = channelNo
			DBLib.insert_store_visitor(parsed_data, dynamodb)

			del parsed_data
	"""
