import os
import random
import string
import time
import json
from decimal import Decimal
from bs4 import BeautifulSoup
import traceback

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler import DBLib
from smart_store_crawler import networkLib
from smart_store_crawler import proxyLib
from smart_store_crawler.proxyLib import proxyManager
from smart_store_crawler import NSparser
from smart_store_crawler.config import LOGLEVEL


def get_sub_category_list(proxy_manager: proxyManager, category_code, referer=''):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_MOBILE_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': referer}

    url = f"https://msearch.shopping.naver.com/search/category/{category_code}"

    commonLib.print_log(LOGLEVEL.D, "get_sub_category_list: url = %s" % url)

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    sub_category_list = []

    soup = BeautifulSoup(html, 'html.parser')
    el_list = soup.select('body ul li a[class^="navigation_category"]')

    is_end_category = False

    if len(el_list) > 0:
        for el in el_list:
            category = {}
            href = el.get('href', '')
            category['name'] = el.get_text()
            category['code'] = href.replace('/search/category/', '')
            sub_category_list.append(category)

    if len(el_list) == 0:
        el_list = soup.select('body ul li a[class^="smallCategory_category"]')

        if len(el_list) > 0:
            is_end_category = True

            for el in el_list:
                category = {}
                href = el.get('href', '')
                category['name'] = el.get_text()
                category['code'] = href.replace('/search/category/', '')
                sub_category_list.append(category)

    if len(el_list) == 0:
        is_end_category = True

    return sub_category_list, is_end_category


def get_category_id_by_category_code(proxy_manager: proxyManager, category_code):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_MOBILE_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = f"https://msearch.shopping.naver.com/search/category/{category_code}"

    commonLib.print_log(LOGLEVEL.D, "get_category_id_by_category_code: url = %s" % url)

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    if html == '':
        return None

    soup = BeautifulSoup(html, 'html.parser')

    el_list = soup.select('#__NEXT_DATA__')
    json_str = str(el_list[0])
    json_str = json_str.replace('<script id="__NEXT_DATA__" type="application/json">{', '{')
    json_str = json_str.replace('}</script>', '}')

    json_data = json.loads(json_str)
    initialState = json_data['props']['pageProps']['initialState']
    json_data = json.loads(initialState)
    catId = str(json_data['searchParam']['catId'])
    catId = catId.split(' ')[0]

    return catId


def get_product_list(proxy_manager: proxyManager, category_code, page, nvshop_tab='', referer=''):
    is_last_page = False

    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': referer}

    url = f"https://search.shopping.naver.com/search/category/{category_code}?pagingIndex={page}&pagingSize=40"

    if nvshop_tab != '':
        url += f"&productSet={nvshop_tab}"

    commonLib.print_log(LOGLEVEL.I, "get_product_list: url = %s" % url)

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    soup = BeautifulSoup(html, 'html.parser')

    el_list = soup.select('span[class^="pagination_btn_page"].active')

    if len(el_list) == 0:
        is_last_page = True

    el_list = soup.select('#__NEXT_DATA__')
    json_str = str(el_list[0])
    json_str = json_str.replace('<script id="__NEXT_DATA__" type="application/json">{', '{')
    json_str = json_str.replace('}</script>', '}')

    json_data = json.loads(json_str)
    parsed_data = NSparser.parse_product_list(json_data, page)
    del json_data

    return parsed_data, url, is_last_page


def get_product_detail(proxy_manager: proxyManager, url, referer=''):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Cookie': 'wcs_bt=s_515732974119507972:1652339350; NNB=JW73AFMXWJ6GE'
        , 'Referer': referer}

    commonLib.print_log(LOGLEVEL.D, "get_product_detail: url = %s" % url)

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    if html == '':
        return None, False, None

    if "<p>해당 쇼핑몰로 이동중입니다.</p>" in html:
        return None, False, "https://this.is.outlink"

    splits = html.split('<script>window.__PRELOADED_STATE__=', 2)

    json_str = ''

    if len(splits) > 1:
        json_str = splits[1]

        splits = json_str.split('}}}</script>', 2)
        json_str = splits[0]
        json_str += '}}}'

    if json_str == '':
        soup = BeautifulSoup(html, 'html.parser')
        el_list = soup.select('#__NEXT_DATA__')

        if len(el_list) > 0:
            json_str = str(el_list[0])
            json_str = json_str.replace('<script id="__NEXT_DATA__" type="application/json">{', '{')
            json_str = json_str.replace('}</script>', '}')

    if json_str == '':
        return None, False, final_url

    try:
        json_data = json.loads(json_str, parse_float=Decimal)
        parsed_data, is_catalog_product = NSparser.parse_product_detail(json_data)
        del json_data

        return parsed_data, is_catalog_product, final_url
    except:
        pass

    return None, False, final_url


def get_store_profile(proxy_manager: proxyManager, url, referer=''):
    commonLib.print_log(LOGLEVEL.D, "get_store_profile: url = %s" % url)

    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive', 'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': referer}

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    splits = html.split('<script>window.__PRELOADED_STATE__=', 2)
    json_str = ''

    if len(splits) > 1:
        json_str = splits[1]

        splits = json_str.split('}}}</script>', 2)
        json_str = splits[0]
        json_str += '}}}'

    if json_str == '':
        return None

    json_data = json.loads(html, parse_float=Decimal)

    parsed_data = NSparser.parse_store_profile(json_data)
    del json_data

    return parsed_data


def get_store_visitor(proxy_manager: proxyManager, channelNo, referer=''):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_MOBILE_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': referer}

    url = f"https://smartstore.naver.com/i/v1/visit/{channelNo}"

    commonLib.print_log(LOGLEVEL.D, "get_store_profile: url = %s" % url)

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    if html == '':
        return None

    json_data = json.loads(html, parse_float=Decimal)  # {"total":5576,"today":14}

    return json_data


def get_product_catalog_list(proxy_manager: proxyManager, product):
    catalog_url = product['catalog_url']
    referer_url = product['referer_url']
    NaPm = product['NaPm']
    nvMid = product['nvMid']

    product_list = []

    splits = catalog_url.split("?", 2)
    url_prefix = splits[0]
    url_prefix = url_prefix.replace("/catalog/", "/api/catalog/")

    for page in range(1, 1000):
        url = f"{url_prefix}?arrivingTomorrow=false&cardPrice=false&deliveryToday=false&isNPayPlus=false&lowestPrice&nvMid={nvMid}&onlyBeautyWindow=false&page={page}&pageSize=20&pr=PC&sort=LOW_PRICE&withFee=false&isManual=false&catalogProviderTypeCode=&exposeAreaName=SELLER_BY_PRICE&catalogType=BRAND&inflow=brc"

        header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
                  'User-Agent': config.CFG_PC_HTTP_UA
            , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
            , 'Referer': referer_url}

        json_data = None

        html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

        if html != '':
            json_data = json.loads(html, parse_float=Decimal)

        if json_data is None:
            break

        totalCount = json_data['result']['totalCount']

        if totalCount == 0:
            break

        products = json_data['result']['products']

        if len(products) == 0:
            break

        for catalog_prd in products:
            product_list.append(catalog_prd)

        time.sleep(3)

    parsed_data = NSparser.parse_product_catalog_list(product_list)
    del product_list

    return parsed_data


def get_product_review_evaluations(proxy_manager: proxyManager, product):
    originProductNo = product['productNo']
    leafCategoryId = product['categoryId']
    merchantNo = product['channelNo']

    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_MOBILE_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = f"https://m.smartstore.naver.com/i/v1/reviews/evaluations-result?originProductNo={originProductNo}&leafCategoryId={leafCategoryId}&merchantNo={merchantNo}"

    json_data = None

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    if html != '':
        json_data = json.loads(html, parse_float=Decimal)

    if json_data is None:
        return None

    parsed_data = NSparser.parse_product_review_evaluations(json_data)
    del json_data

    return parsed_data


def get_product_review_list(proxy_manager: proxyManager, product):
    originProductNo = product['productNo']
    merchantNo = product['channelNo']

    total_review_list = []

    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_MOBILE_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    for page in range(1, 99999):
        url = f"https://m.smartstore.naver.com/i/v1/reviews/paged-reviews?page={page}&pageSize=30&merchantNo={merchantNo}&originProductNo={originProductNo}&sortType=REVIEW_RANKING"

        html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

        if html == 'OK':  # no more review
            break

        json_data = json.loads(html, parse_float=Decimal)

        parsed_data = NSparser.parse_product_review_list(json_data)
        del json_data

        if parsed_data is None:
            break

        if len(parsed_data) == 0:
            break

        for review in parsed_data:
            total_review_list.append(review)

        time.sleep(3)

    return total_review_list


def get_category_hot_keyword(proxy_manager: proxyManager, category_code, categoryId):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = "https://search.shopping.naver.com/api/graphql"

    categoryId = str(categoryId)

    param = {}
    param['query'] = "\n      query ($params: PopularQueriesInput) {\n        BestPopularQueries(params: $params) {\n          rankedDate\n          charts {\n            keyword\n            rank\n            change\n            rankedReason\n          }\n        }\n      }\n    ";
    param['variables'] = {'params': {'categoryId': categoryId}}

    json_data = None

    html, final_url = networkLib.req_json_on_proxy(url, header, param, proxy_manager)

    if html != '':
        json_data = json.loads(html, parse_float=Decimal)

    if json_data is None:
        return None

    parsed_data = NSparser.parse_category_hot_keyword(json_data)

    return parsed_data


def get_category_hot_brand(proxy_manager: proxyManager, category_code, categoryId):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = "https://search.shopping.naver.com/api/graphql"

    categoryId = str(categoryId)

    param = {}
    param['query'] = "\n      query ($params: PopularBrandsInput) {\n        BestPopularBrands(params: $params) {\n          rankedDate\n          brands {\n            weekly {\n              rank\n              exposeBrandName\n              brandName\n              change\n            }\n            daily {\n              rank\n              exposeBrandName\n              brandName\n              change\n            }\n          }\n        }\n      }\n    ";
    param['variables'] = {'params': {'categoryId': categoryId}}

    json_data = None

    html, final_url = networkLib.req_json_on_proxy(url, header, param, proxy_manager)

    if html != '':
        json_data = json.loads(html, parse_float=Decimal)

    if json_data is None:
        return None

    parsed_data = NSparser.parse_category_hot_brand(json_data)

    return parsed_data


def get_category_brand_list(proxy_manager: proxyManager, category_code):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = f"https://search.shopping.naver.com/search/category/{category_code}"

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    soup = BeautifulSoup(html, 'html.parser')

    brand_list = []

    el_list = soup.select('#__NEXT_DATA__')

    if len(el_list) == 0:
        return brand_list

    json_str = str(el_list[0])
    json_str = json_str.replace('<script id="__NEXT_DATA__" type="application/json">{', '{')
    json_str = json_str.replace('}</script>', '}')

    json_data = json.loads(json_str)

    try:
        filterValues = json_data['props']['pageProps']['initialState']['mainFilters'][1]['filterValues']

        for row in filterValues:
            new_brand = {}

            new_brand['title'] = row['title']
            new_brand['value'] = row['value']
            new_brand['productCount'] = row['productCount']

            brand_list.append(new_brand)
    except:
        pass

    return brand_list

def get_category_product_count(proxy_manager: proxyManager, category_code):
    header = {'Accept-Encoding': 'gzip, deflate', 'Proxy-Connection': 'Keep-Alive',
              'User-Agent': config.CFG_PC_HTTP_UA
        , 'Accept': '*/*', 'Accept-Language': 'ko,en;q=0.9,en-US;q=0.8'
        , 'Referer': ''}

    url = f"https://search.shopping.naver.com/search/category/{category_code}"

    html, final_url = networkLib.req_get_on_proxy(url, header, proxy_manager)

    soup = BeautifulSoup(html, 'html.parser')

    product_count = 0

    el = soup.select_one('div.seller_filter_area > ul > li:nth-child(1) > a > span:nth-child(1)')

    if el is not None:
        product_count = el.getText()
        product_count = product_count.replace(",", "")
        product_count = int(product_count)

    return product_count
