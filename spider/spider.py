import json
import logging
from urllib.parse import urlencode

import pymongo
import requests
from lxml.etree import XMLSyntaxError
from pyquery import PyQuery as pq
from requests.exceptions import ConnectionError

from .setting import *

logging.basicConfig(level=logging.INFO)

# 当前代理地址
proxy = None


def get_proxy():
    '''
    获取代理
    :return:
    '''
    try:
        response = requests.get(proxy_pool_url)  # 禁用自动重定向
        if response.status_code == 200:
            return json.loads(response.text)
        return None
    except ConnectionError:
        return None


def get_html(url, count=1):
    '''
    爬取指定网页
    :param url:
    :param count:
    :return:
    '''
    global proxy

    logging.info('当前第%s次爬取：%s 代理地址：%s' % (count, url, proxy))

    # 判断是否最大请求次数
    if count >= max_count:
        logging.error('请求已超过最大次数，请求失败，地址：%s' % (url,))
        return None

    try:
        if proxy:
            proxies = {
                'http': 'http://' + proxy
            }
            response = requests.get(url, allow_redirects=False, headers=headers, proxies=proxies)  # 禁用自动重定向
        else:
            response = requests.get(url, allow_redirects=False, headers=headers)  # 禁用自动重定向
        if response.status_code == 200:
            return response.text
        elif response.status_code == 302:  # IP被封，更换代理
            count += 1
            logging.warning('当前IP已被检测，请更换代理。')
            logging.info('正在获取新代理...')
            proxy = get_proxy()
            if proxy:
                logging.info('获取新代理成功：{}'.format(proxy))
                return get_html(url, count)
            else:
                logging.warning('获取新代理失败，当前地址：%s' % (url,))
                return None
    except ConnectionError:
        count += 1
        logging.warning('请求异常，请更换代理')
        logging.info('正在获取新代理...')
        proxy = get_proxy()
        if proxy:
            logging.info('获取新代理成功：{}'.format(proxy))
            return get_html(url, count)
        else:
            logging.warning('获取新代理失败，当前地址：%s' % (url,))
            return None


def get_index(keyword, page):
    '''
    获取搜素索引页
    :param keyword:
    :param page:
    :return:
    '''
    raw_data = {
        'query': keyword,
        'type': 2,
        'page': page
    }
    # 将请求参数urlencode
    data = urlencode(raw_data)
    url = base_url + data
    html = get_html(url, 1)
    return html


def parse_index(html):
    '''
    使用pyquery解析页面内容获取文章url
    :param html:
    :return:
    '''
    # 可以首先用 lxml 的 etree 处理一下代码，这样如果你的 HTML 代码出现一些不完整或者疏漏，都会自动转化为完整清晰结构的 HTML代码。
    logging.info('开始解析查询页文章地址...')
    if html:
        doc = pq(html)
        # 结果页有两种格式，先匹配最常见的格式
        items = doc('div.news-box ul.news-list li[id^="sogou_vr_"] div.img-box a[data-z="art"]').items()
        for item in items:
            yield item.attr('href')
        # 匹配第二种格式
        items = doc('div.news-box ul.news-list li.js-li div.txt-box h3 a').items()
        for item in items:
            yield item.attr('href')
    else:
        logging.warning('未获取到查询页html...')
        pass


def get_detail(url):
    '''
    请求微信文章正文，不需要使用代理，由于没有使用反爬虫策略
    :param url:
    :return:
    '''
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        return None
    except ConnectionError:
        return None


def parse_detail(html, url):
    '''
    解析微信文章信息
    :param html:
    :return:
    '''
    try:
        doc = pq(html)
        # 标题
        title = doc('h2.rich_media_title').text()
        # 发布日期
        post_date = doc('em#post-date').text()
        # 发布人
        post_user = doc('a#post-user').text()
        # 文本内容
        content_text = doc('div.rich_media_content').text()
        # 图片内容
        content_img_items = doc('div.rich_media_content p img').items()
        content_imgs = []
        for item in content_img_items:
            content_imgs.append(item.attr('data-src'))
        # 公众号
        wechat = doc('p.profile_meta span.profile_meta_value').text()
        return {
            'url': url,
            'title': title,
            'post_date': post_date,
            'post_user': post_user,
            'content_text': content_text,
            'content_imgs': content_imgs,
            'wechat': wechat
        }
    except XMLSyntaxError:
        return None


client = pymongo.MongoClient(mongo_url)
db = client[dbname]  # 库名


def save_mongo(data):
    '''
    保存到mongo中
    :param data:
    :return:
    '''
    # 向article表中更新数据，update表示查询url是否已存在，不存在则插入，存在则更新
    if db['article'].update({'url': data['url']}, {'$set': data}, True):
        logging.info('保存至mongo中成功：%s' % (data,))
    else:
        logging.warning('保存至mongo中失败：%s' % (data,))


def run():
    '''
    循环调度器
    :return:
    '''
    for page in range(1, 101):
        html = get_index(keyword, page)
        if html:
            for url in parse_index(html):
                # 获取文章html内容
                article_html = get_detail(url)
                if article_html:
                    article_data = parse_detail(article_html, url)
                    if article_data:
                        save_mongo(article_data)


if __name__ == '__main__':
    run()
