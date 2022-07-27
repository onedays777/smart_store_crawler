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


def product_detail_to_db_by_catalog(proxy_manager: proxyManager, product_list, is_force, referer_url, dynamodb=None):
    for product in product_list:
        DBLib.insert_product_catalog(product, dynamodb)

        commonLib.print_log(LOGLEVEL.D,
                        "product_detail_to_db_by_catalog: insert_product_catalog = " + str(product.get("nvMid", "")))

        # TODO : 일단 가격 비교 상품만 수집 한다.
        """
        url = product.get('pcProductUrl', None)

        if url is None:
            url = product.get('mobileProductUrl', None)

        if url is None:
            continue

        item = DBLib.select_naver_shopping_outlink(url, dynamodb)

        if item is not None:
            del item
            commonLib.print_log(LOGLEVEL.D, "product_detail_to_db_by_catalog: outlink url = " + url)
            continue

        proxy_manager.changeProxy()
        parsed_data, is_catalog_product, final_url = naverShopping.get_product_detail(proxy_manager, url, referer_url)

        if final_url is not None:
            if parsed_data is None and not commonLib.is_naver_url(final_url):
                DBLib.insert_naver_shopping_outlink(url, dynamodb)
                commonLib.print_log(LOGLEVEL.D, "product_detail_to_db_by_catalog: outlink url")
                continue

        if parsed_data is None:
            return

        productNo = parsed_data.get('productNo', None)

        if productNo is None:
            return

        if not is_force:
            item = DBLib.select_product_list(productNo, dynamodb)

            if item is not None:
                del item
                commonLib.print_log(LOGLEVEL.D, "product_detail_to_db_by_catalog: exists product = " + str(productNo))

                return

        review_list = naverShopping.get_product_review_list(proxy_manager, parsed_data)
        review_evaluations = naverShopping.get_product_review_evaluations(proxy_manager, parsed_data)

        parsed_data['review_evaluations'] = review_evaluations

        for review in review_list:
            DBLib.insert_review(review, dynamodb)

        productNo = parsed_data.get('productNo', None)

        commonLib.print_log(LOGLEVEL.D,
                            "product_detail_to_db_by_catalog: insert_product = " + str(productNo))
        DBLib.insert_product(parsed_data, dynamodb)

        del review_list
        del review_evaluations
        del parsed_data
        """


def product_detail_to_db(proxy_manager: proxyManager, product, is_force, referer_url, dynamodb=None):
    url = product.get('crUrl', '')

    if url == '':
        url = product.get('adcrUrl', '')

    item = DBLib.select_naver_shopping_outlink(url, dynamodb)

    if item is not None:
        commonLib.print_log(LOGLEVEL.D, "product_detail_to_db: outlink url = " + url)
        return

    parsed_data, is_catalog_product, final_url = naverShopping.get_product_detail(proxy_manager, url, referer_url)

    if final_url is not None:
        if parsed_data is None and not commonLib.is_naver_url(final_url):
            DBLib.insert_naver_shopping_outlink(url, dynamodb)
            return

    if parsed_data is None:
        return

    if is_catalog_product:
        product_list = naverShopping.get_product_catalog_list(proxy_manager, parsed_data)
        product_detail_to_db_by_catalog(proxy_manager, product_list, is_force, parsed_data.get('catalog_url', ''),
                                        dynamodb)

        del product_list

        return
    else:  # TODO : 일단 가격 비교 상품만 수집 한다.
        del parsed_data
        return

    """
    productNo = parsed_data.get('productNo', None)

    if productNo is None:
        return

    if not is_force:
        item = DBLib.select_product_list(productNo, dynamodb)

        if item is not None:
            del item

            return

    review_list = naverShopping.get_product_review_list(proxy_manager, parsed_data)
    review_evaluations = naverShopping.get_product_review_evaluations(proxy_manager, parsed_data)

    parsed_data['review_evaluations'] = review_evaluations

    for review in review_list:
        DBLib.insert_review(review, dynamodb)

    commonLib.print_log(LOGLEVEL.D, "store_product_detail_to_db: insert_product = " + str(productNo))
    DBLib.insert_product(parsed_data, dynamodb)

    del review_list
    del review_evaluations

    del parsed_data
    """
