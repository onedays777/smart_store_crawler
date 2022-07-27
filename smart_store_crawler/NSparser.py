import os
import random
import string
import time
import json
import traceback
from bs4 import BeautifulSoup
from datetime import datetime

def parse_product_list(json_data, page):
    json_data = json_data['props']['pageProps']['initialState']
    #json_data = json.loads(json_str)

    item_name_list = ['mallProductId', 'shoppingResult', 'searchAdResult', 'productName', 'productTitle', 'brand',
                      'brandNo', 'scoreInfo', 'category1Id', 'category2Id', 'category3Id', 'category4Id'
        , 'category1Name', 'category2Name', 'category3Name', 'category4Name', 'categoryLevel', 'openDate', 'lowPrice',
                      'price', 'lowestCardPrice'
        , 'reviewCount', 'checkOutReviewCount', 'imageUrl', 'additionalImageCount', 'mallCount', 'keepCnt', 'crUrl',
                      'naverPayAdAccumulatedDisplayValue'
        , 'deliveryFeeContent', 'rank', 'adcrUrl']

    product_list = json_data['products']['list']
    #product_list = json_data['compositeProducts']['list']

    new_product_list = []

    rankNum = 0

    for product in product_list:
        item = product.get('item', None)

        if item is None:
            continue

        new_product = {}

        for item_name in item_name_list:
            new_product[item_name] = item.get(item_name, '')

        adcrUrl = new_product.get("adcrUrl", "")

        if len(adcrUrl) > 10:
            new_product['rankNum'] = "광고"
        else:
            rankNum += 1
            new_product['rankNum'] = str(rankNum)

        new_product['page'] = page
        new_product['rankDate'] = datetime.now().strftime('%Y-%m-%d')

        mallInfoCache = item.get('mallInfoCache', {})
        new_product['mallGrade'] = mallInfoCache.get('mallGrade', '')
        new_product['eventScheduledCont'] = mallInfoCache.get('eventScheduledCont', '')

        lowMallList = item.get('lowMallList', None)

        if lowMallList is not None:
            new_product['lowMallList'] = []

            for lowMall in lowMallList:
                new_rowMall = {}

                new_rowMall['price'] = lowMall.get('price', '')
                new_rowMall['chnlType'] = lowMall.get('chnlType', '')
                new_rowMall['chnlName'] = lowMall.get('chnlName', '')
                new_rowMall['naverPay'] = lowMall.get('naverPay', '')
                new_rowMall['naverPayType'] = lowMall.get('naverPayType', '')

                new_product['lowMallList'].append(new_rowMall)

        new_product_list.append(new_product)

    return new_product_list


def parse_product_detail(json_data):
    new_product = {}

    try:
        url = json_data['props']['pageProps']['ogTag']['url']

        if "shopping.naver.com/catalog/" in url:
            catalog_url = url.replace('?', '/products?')

            #queryHash = json_data['props']['pageProps']['dehydratedState']['queries'][3]['queryHash']
            NaPm = json_data['query']['NaPm']
            nvMid = json_data['query']['nvMid']  # PKey

            new_product['nvMid'] = nvMid
            new_product['catalog_url'] = catalog_url
            new_product['referer_url'] = url
            #new_product['queryHash'] = queryHash
            new_product['NaPm'] = NaPm

            return new_product, True
    except:
        pass

    product = json_data.get('product', None)

    if product is None:
        return None, False

    channelNo = json_data['product']['A']['channel']['channelNo']
    new_product['channelNo'] = int(channelNo)

    channelSiteFullUrl = json_data['product']['A']['channel']['channelSiteFullUrl']

    if "https://shopping.naver.com/outlink/" not in channelSiteFullUrl:
        new_product['channelSiteFullUrl'] = channelSiteFullUrl
        new_product['channelSiteProfileUrl'] = channelSiteFullUrl + '/profile'

    item_name_list = ['productNo', 'name', 'channelProductType', 'channelProductStatusType', 'channelProductDisplayStatusType',
                      'name', 'saleType', 'productStatusType']

    for item_name in item_name_list:
        new_product[item_name] = json_data['product']['A'].get(item_name, '')

    new_product['wholeCategoryName'] = json_data['product']['A']['category']['wholeCategoryName']
    new_product['categoryId'] = json_data['product']['A']['category']['categoryId']  # Review 정보를 위해서 필요 string

    new_product['salePrice'] = json_data['storeKeep']['A'].get('salePrice', 0)
    new_product['stockQuantity'] = json_data['storeKeep']['A'].get('stockQuantity', 0)

    new_product['zzimCount'] = json_data['storeKeep']['A'].get('zzimCount', 0)
    new_product['channelName'] = json_data['product']['A']['channel']['channelName']
    new_product['danawaRegistration'] = json_data['product']['A']['epInfo'].get('danawaRegistration', False)
    new_product['discountedRatio'] = json_data['product']['A']['benefitsView'].get('discountedRatio', 0)
    new_product['discountedSalePrice'] = json_data['product']['A']['benefitsView'].get('discountedSalePrice', 0)
    new_product['productImages'] = json_data['product']['A'][
        'productImages']  # [] => {order, url, width, height, imageType}
    new_product['optionCombinations'] = json_data['product']['A'].get('optionCombinations',
                                                                      {})  # [] => {id, optionName1, optionName2, optionName3, stockQuantity, price}
    new_product['representImage'] = json_data['product']['A'].get('representImage',
                                                                  {})  # {order, url, width, height, imageType}
    new_product['detailContents'] = json_data['product']['A'].get('detailContents',
                                                                  {})  # {excessDetailContentText, editorType, detailContentText}

    productDeliveryInfo = json_data['product']['A'].get('productDeliveryInfo', None)
    benefitsView = json_data['product']['A'].get('benefitsView', None)
    reviewAmount = json_data['product']['A'].get('reviewAmount', None)
    saleAmount = json_data['product']['A'].get('saleAmount', None)

    new_product['saleAmount'] = {}

    if productDeliveryInfo is not None:
        new_product['baseFee'] = productDeliveryInfo.get('baseFee', 0)
        new_product['deliveryFeeType'] = productDeliveryInfo.get('deliveryFeeType', '')
        new_product['todayDelivery'] = productDeliveryInfo.get('todayDelivery', False)

    if benefitsView is not None:
        new_product['textReviewPoint'] = benefitsView.get('textReviewPoint', 0)
        new_product['photoVideoReviewPoint'] = benefitsView.get('photoVideoReviewPoint', 0)

    if reviewAmount is not None:
        new_product['totalReviewCount'] = reviewAmount.get('totalReviewCount', 0)
        new_product['averageReviewScore'] = reviewAmount.get('averageReviewScore', 0)  # float -> string
        new_product['averageReviewScore'] = str(new_product['averageReviewScore'])

        new_product['score1ReviewCount'] = reviewAmount.get('score1ReviewCount', 0)
        new_product['score2ReviewCount'] = reviewAmount.get('score2ReviewCount', 0)
        new_product['score3ReviewCount'] = reviewAmount.get('score3ReviewCount', 0)
        new_product['score4ReviewCount'] = reviewAmount.get('score4ReviewCount', 0)
        new_product['score5ReviewCount'] = reviewAmount.get('score5ReviewCount', 0)

    if saleAmount is not None:
        new_product['saleAmount']['cumulationSaleCount'] = saleAmount.get('cumulationSaleCount', 0)
        new_product['saleAmount']['recentSaleCount'] = saleAmount.get('recentSaleCount', 0)
        new_product['saleAmount']['regDate'] = datetime.now().strftime('%Y-%m-%d')


    viewAttributes = json_data['product']['A'].get('viewAttributes', None)
    detailAttributes = json_data['product']['A'].get('detailAttributes', None)
    additionalAttributes = json_data['product']['A'].get('additionalAttributes', None)
    productDeliveryLeadTimes = json_data['product']['A'].get('productDeliveryLeadTimes', None)
    averageDeliveryLeadTime = json_data['product']['A'].get('averageDeliveryLeadTime', None)

    sellerTags = None
    seoInfo = json_data['product']['A'].get('seoInfo', None)

    new_product['productDeliveryLeadTimes'] = []

    if seoInfo is not None:
        sellerTags = seoInfo.get('sellerTags', None)

    if viewAttributes is not None:
        new_product['viewAttributes'] = viewAttributes  # {}

    if detailAttributes is not None:
        new_product['detailAttributes'] = detailAttributes  # {}

    if additionalAttributes is not None:
        new_product['additionalAttributes'] = additionalAttributes  # {{}}

    if productDeliveryLeadTimes is not None:
        for el in productDeliveryLeadTimes:
            el['date'] = datetime.now().strftime('%Y-%m-%d')
            new_product['productDeliveryLeadTimes'].append(el) # [] => {rangeNumberText, rangeText, leadTimeCount, leadTimePercent, date}

    if averageDeliveryLeadTime is not None:
        new_product['averageDeliveryLeadTime'] = averageDeliveryLeadTime  # [] => {productAverageDeliveryLeadTime, sellerAverageDeliveryLeadTime}

    new_product['area2ExtraFee'] = ''

    productDeliveryInfo = json_data['product']['A'].get('productDeliveryInfo', None)

    if productDeliveryInfo is not None:
        deliveryBundleGroup = productDeliveryInfo.get('deliveryBundleGroup', None)

        if deliveryBundleGroup is not None:
            area2ExtraFee = deliveryBundleGroup.get('area2ExtraFee', None)

            if area2ExtraFee is not None:
                new_product['area2ExtraFee'] = area2ExtraFee

    new_product['sellerTags'] = []

    if sellerTags is not None:
        for tag in sellerTags:
            text = tag.get('text', '')
            new_product['sellerTags'].append(text)

    new_product['productNo'] = int(new_product['productNo'])

    smartStoreV2 = json_data.get('smartStoreV2', None)

    if smartStoreV2 is not None:
        new_product['representativeImageUrl'] = smartStoreV2['channel'].get('representativeImageUrl', '')

    return new_product, False


def parse_store_profile(json_data):
    new_store_profile = {}

    if json_data.get('smartStoreV2', None) is not None:
        channelNo = json_data['smartStoreV2']['channelNo']  # 스토어의 기본키
        chrgrEmail = json_data['smartStoreV2']['channel'].get('chrgrEmail', '')  # e-mail
        channelName = json_data['smartStoreV2']['channel'].get('channelName', '')  # 쇼핑몰 이름
        description = json_data['smartStoreV2']['channel'].get('description', '')  # 쇼핑몰 설명
        representName = json_data['smartStoreV2']['channel'].get('representName', '')  # 상호명
        representativeName = json_data['smartStoreV2']['channel'].get('representativeName', '')  # 대표자
        identity = json_data['smartStoreV2']['channel'].get('identity', '')  # 사업자등록번호
        declaredToOnlineMarkettingNumber = json_data['smartStoreV2']['channel'].get('declaredToOnlineMarkettingNumber',
                                                                                    '')  # 통신판매업번호

        formattedNumber = json_data['smartStoreV2']['channel']['contactInfo'].get('telNo', {}).get('formattedNumber',
                                                                                                   '')  # 고객센터
        fullAddressInfo = json_data['smartStoreV2']['channel']['businessAddressInfo'].get('fullAddressInfo',
                                                                                          '')  # 사업장 소재지

    elif json_data.get('channel', None) is not None:
        channelNo = json_data['channel']['A']['id']  # 스토어의 기본키
        chrgrEmail = json_data['channel']['A'].get('chrgrEmail', '')
        channelName = json_data['channel']['A'].get('channelName', '')  # 쇼핑몰 이름
        description = json_data['channel']['A'].get('description', '')  # 쇼핑몰 설명
        representName = json_data['channel']['A'].get('representName', '')  # 상호명
        representativeName = json_data['channel']['A'].get('representativeName', '')  # 대표자
        identity = json_data['channel']['A'].get('identity', '')  # 사업자등록번호
        declaredToOnlineMarkettingNumber = json_data['channel']['A'].get('declaredToOnlineMarkettingNumber',
                                                                         '')  # 통신판매업번호

        formattedNumber = json_data['channel']['A']['contactInfo'].get('telNo', {}).get('formattedNumber', '')  # 고객센터
        fullAddressInfo = json_data['channel']['A']['businessAddressInfo'].get('fullAddressInfo', '')  # 사업장 소재지

    # 성별 검색 인기도 남자/여자
    ratioFemale = json_data['datalab']['A']['ratioFemale']
    ratioMale = json_data['datalab']['A']['ratioMale']

    # 연령별 검색 인기도
    ratings = json_data['datalab']['A']['ratings']

    new_store_profile['channelNo'] = channelNo
    new_store_profile['chrgrEmail'] = chrgrEmail
    new_store_profile['channelName'] = channelName
    new_store_profile['description'] = description
    new_store_profile['representName'] = representName
    new_store_profile['representativeName'] = representativeName
    new_store_profile['formattedNumber'] = formattedNumber
    new_store_profile['identity'] = identity
    new_store_profile['fullAddressInfo'] = fullAddressInfo
    new_store_profile['declaredToOnlineMarkettingNumber'] = declaredToOnlineMarkettingNumber
    new_store_profile['ratioFemale'] = ratioFemale
    new_store_profile['ratioMale'] = ratioMale
    new_store_profile['ratings'] = ratings

    return new_store_profile


def parse_product_catalog_list(product_list):
    """
    item_name_list = ['nvMid', 'fullCatNm', 'fullCatId', 'catLvl', 'registrationYearMonth', 'priceHistory',
                      'catalogName', 'productName', 'pcPrice', 'mobilePrice'
        , 'pcProductUrl', 'mobileProductUrl', 'savingPoint', 'savingStoreCoupon', 'deliveryFee', 'pcNaverPay',
                      'mobileNaverPay', 'totalCount']
    """

    item_name_list = ['nvMid', 'matchNvMid', 'categoryId', 'mallName', 'productName', 'pcPrice', 'pcProductUrl', 'mobileProductUrl'
                      , 'cardPrice', 'mobilePrice', 'cardName', 'mallBusinessNumber', 'mallRegisteredNumber'
                      , 'mallBusinessBaseAddress', 'mallUrl', 'deliveryFee', 'eventContent', 'savingContent'
                      , 'pcNaverPay', 'mobileNaverPay', 'deliveryContent', 'couponContent']

    new_product_list = []

    for product in product_list:
        new_product = {}

        for item_name in item_name_list:
            new_product[item_name] = product.get(item_name, '')

        new_product_list.append(new_product)

    return new_product_list


def parse_product_review_evaluations(json_data):
    review_evaluations = {}
    review_evaluations['productReviewEvaluationVOs'] = []
    review_evaluations['reviewTopicVOs'] = []

    productReviewEvaluationVOs = json_data.get('productReviewEvaluationVOs', None)

    if productReviewEvaluationVOs is None:
        return None

    for review_evaluation in productReviewEvaluationVOs:
        new_review_evaluation = {}

        new_review_evaluation['reviewEvaluationSeq'] = review_evaluation.get('reviewEvaluationSeq')
        new_review_evaluation['reviewEvaluationName'] = review_evaluation.get('reviewEvaluationName')
        new_review_evaluation['reviewEvaluationPhrase'] = review_evaluation.get('reviewEvaluationPhrase')

        reviewEvaluationValues = review_evaluation.get('reviewEvaluationValues')

        if reviewEvaluationValues is not None:
            new_review_evaluation['reviewEvaluationValues'] = []

            for evalue in reviewEvaluationValues:
                new_value = {}

                new_value['reviewEvaluationValueName'] = evalue['reviewEvaluationValueName']
                new_value['count'] = evalue['count']
                new_value['percent'] = evalue['percent']

                new_review_evaluation['reviewEvaluationValues'].append(new_value)

        review_evaluations['productReviewEvaluationVOs'].append(new_review_evaluation)

    reviewTopicVOs = json_data.get('reviewTopicVOs', None)

    if reviewTopicVOs is None:
        return review_evaluations

    for topic in reviewTopicVOs:
        review_evaluations['reviewTopicVOs'].append(topic['topicCodeName'])

    return review_evaluations


def parse_product_review_list(json_data):
    new_review_list = []

    review_list = json_data.get('contents', None)

    if review_list is None:
        return None

    for review in review_list:
        new_review = {}

        createDate = review.get('createDate', None)

        if createDate is None:
            continue

        #date_example = "2022-04-27T01:54:29.230+0000"
        createDate = str(createDate).split(".", 2)[0]
        date_format = datetime.strptime(createDate, "%Y-%m-%dT%H:%M:%S")
        unix_time = datetime.timestamp(date_format)
        comp_time = time.time() - (86400*365)

        if unix_time < comp_time:
            break

        new_review['id'] = str(review.get('id', "0"))  # Pkey
        new_review['reviewContentClassType'] = review.get('reviewContentClassType', '')
        new_review['reviewScore'] = review.get('reviewScore', 0)
        new_review['helpCount'] = review.get('helpCount', 0)
        new_review['reviewRankingScore'] = review.get('reviewRankingScore', 0)
        new_review['writerMemberId'] = review.get('writerMemberId', '')
        new_review['writerMemberProfileImageUrl'] = review.get('writerMemberProfileImageUrl', '')
        new_review['productName'] = review.get('productName', '')
        new_review['productOptionContent'] = review.get('productOptionContent', '')
        new_review['productNo'] = review.get('productNo', '')
        new_review['commentContent'] = review.get('commentContent', '')
        new_review['createDate'] = review.get('createDate', '') # 2022-04-27T01:54:29.230+0000

        reviewTopics = review.get('reviewTopics')

        new_review['topicCodeName'] = []

        if reviewTopics is not None:
            for topic in reviewTopics:
                topicCodeName = topic.get('topicCodeName', '')

                new_review['topicCodeName'].append(topicCodeName)

        reviewAttaches = review.get('reviewAttaches')

        new_review['attachUrl'] = []

        if reviewAttaches is not None:
            for attache in reviewAttaches:
                attachUrl = attache.get('attachUrl', '')

                new_review['attachUrl'].append(attachUrl)

        new_review_list.append(new_review)

    return new_review_list


def parse_category_hot_keyword(json_data):
    data = json_data.get('data', None)

    if data is None:
        return None

    bestPopularQueries = data.get('BestPopularQueries', None)

    if bestPopularQueries is None:
        return None

    new_chart_list = []

    #rankedDate = bestPopularQueries.get('rankedDate', '')
    charts = bestPopularQueries.get('charts')

    if charts is None:
        return None

    for chart in charts:
        chart['keyword'] = chart['keyword']
        chart['rank'] = int(chart['rank'])
        new_chart_list.append(chart)

    return new_chart_list


def parse_category_hot_brand(json_data):
    data = json_data.get('data', None)

    if data is None:
        return None

    bestPopularBrands = data.get('BestPopularBrands', None)

    if bestPopularBrands is None:
        return None

    new_brand_list = []

    #rankedDate = bestPopularBrands.get('rankedDate', '')
    brands = bestPopularBrands.get('brands')

    if brands is None:
        return None

    for brand in brands['daily']:
        brand['brandName'] = brand['brandName']
        brand['exposeBrandName'] = brand['exposeBrandName']
        brand['rank'] = int(brand['rank'])
        new_brand_list.append(brand)

    return new_brand_list
