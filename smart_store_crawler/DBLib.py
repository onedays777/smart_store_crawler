import sys

import boto3
import time
import traceback
from datetime import datetime

from urllib.parse import urlparse, parse_qs, parse_qsl
from botocore.exceptions import ClientError
from botocore.config import Config

from smart_store_crawler import commonLib
from smart_store_crawler.config import LOGLEVEL

"""
access-key-id : AKIA425PBCI74MEE5QYB
access-key : VbVcWM+9jVaC5hnn6yWUlHhDH/x525XlN7oIlIs4
table : smartstore-channel-info

https://docs.aws.amazon.com/ko_kr/amazondynamodb/latest/developerguide/Tools.CLI.html

"""


def create_connection():
    """
	client = boto3.client(
		'dynamodb',
		aws_access_key_id = 'AKIA425PBCI74MEE5QYB',
		aws_secret_access_key = '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',
		)

	db_exceptions = client.exception
	"""

    config = Config(
        connect_timeout=30, read_timeout=2,
        retries={'max_attempts': 3})

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id='AKIA425PBCI74MEE5QYB',
        aws_secret_access_key='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',
        region_name='ap-northeast-2',
        config=config
    )

    return dynamodb


# ---------------------------- Select

def select_store_profile_count(dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-channel-info')

    return table.item_count


def select_store_profile_list(channelNo=None, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-channel-info')

    if channelNo is None:
        response = table.scan(Limit=1000)
        return response['Items']

    try:
        response = table.get_item(Key={'channelNo': channelNo})
    except ClientError as e:
        traceback.print_exc()
    else:
        return response.get('Item', None)

    return None


def select_store_visitor_list(channelNo=None, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-channel-visitor')

    if channelNo is None:
        response = table.scan()
        return response['Items']

    try:
        response = table.get_item(Key={'channelNo': channelNo})
    except ClientError as e:
        traceback.print_exc()
    else:
        return response.get('Item', None)

    return None


def select_naver_shopping_outlink(link_url=None, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver_shopping_outlink')

    if link_url is None:
        response = table.scan()
        return response['Items']

    parts = urlparse(link_url)
    querys = parse_qs(parts.query)

    nvMid = querys.get('nvMid', None)

    if nvMid is None:
        nvMid = querys.get('nv_mid', None)

    if nvMid is None:
        return None

    valid_nvMid = int(nvMid[0])

    try:
        response = table.get_item(Key={'nv_mid': valid_nvMid})
    except ClientError as e:
        traceback.print_exc()
    else:
        return response.get('Item', None)

    return None


def select_naver_shopping_outlink_count(dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver_shopping_outlink')

    return table.item_count


def select_product_list(productNo=None, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-product')

    if productNo is None:
        response = table.scan()
        return response['Items']

    try:
        response = table.get_item(Key={'productNo': productNo})
    except ClientError as e:
        traceback.print_exc()
    else:
        return response.get('Item', None)

    return None


# ---------------------------- Insert

def insert_store_profile(store_profile, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-channel-info')

    try:
        table.put_item(Item=store_profile)
    except:
        traceback.print_exc()


def insert_store_visitor(store_visit, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    store_visit['updated_at'] = int(time.time())

    table = dynamodb.Table('smartstore-channel-visitor')

    try:
        table.put_item(Item=store_visit)
    except:
        traceback.print_exc()


def insert_product(product, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-product')

    try:
        table.put_item(Item=product)
    except:
        traceback.print_exc()
    # print(product)
    # sys.exit()


def insert_review(review, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('smartstore-product-review')

    try:
        table.put_item(Item=review)
    except:
        traceback.print_exc()


def insert_product_catalog(product, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver-shopping-price-comparison')

    try:
        table.put_item(Item=product)
    except:
        traceback.print_exc()


def insert_category_sub_category(category_code, category_name, product_count, sub_category_list, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver-shopping-sub-category')

    regist_date = datetime.now().strftime('%Y%m%d')

    item = {}
    item['categoryId'] = category_code
    item['categoryName'] = category_name
    item['product_count'] = product_count
    item['regist_date'] = int(regist_date)
    item['category_list'] = sub_category_list

    try:
        table.put_item(Item=item)
    except:
        traceback.print_exc()


def insert_category_brand(category_code, category_name, category_brand_list, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver-shopping-category-brand')

    regist_date = datetime.now().strftime('%Y%m%d')

    item = {}
    item['categoryId'] = category_code
    item['categoryName'] = category_name
    item['regist_date'] = int(regist_date)
    item['brand_list'] = category_brand_list

    try:
        table.put_item(Item=item)
    except:
        traceback.print_exc()

def insert_category_hot_brand(category_code, category_name, hot_brand, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver-shopping-hot-brand')

    regist_date = datetime.now().strftime('%Y%m%d')

    item = {}
    item['categoryId'] = category_code
    item['categoryName'] = category_name
    item['regist_date'] = int(regist_date)
    item['hot_brand'] = hot_brand

    try:
        table.put_item(Item=item)
    except:
        traceback.print_exc()


def insert_category_hot_keyword(category_code, category_name, hot_keyword, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    table = dynamodb.Table('naver-shopping-search-keyword')

    regist_date = datetime.now().strftime('%Y%m%d')

    item = {}
    item['categoryId'] = category_code
    item['categoryName'] = category_name
    item['regist_date'] = int(regist_date)
    item['hot_brand'] = hot_keyword

    try:
        table.put_item(Item=item)
    except:
        traceback.print_exc()


def insert_naver_shopping_outlink(link_url, dynamodb=None):
    if not dynamodb:
        dynamodb = create_connection()

    parts = urlparse(link_url)
    querys = parse_qs(parts.query)

    nvMid = querys.get('nvMid', None)

    if nvMid is None:
        nvMid = querys.get('nv_mid', None)

    if nvMid is None:
        return

    valid_nvMid = int(nvMid[0])

    table = dynamodb.Table('naver_shopping_outlink')

    try:
        table.put_item(Item={'nv_mid': valid_nvMid})
    except:
        traceback.print_exc()
