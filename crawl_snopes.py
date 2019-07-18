'''爬虫要先执行该段代码，获取所有股票代码，再根据代码获取所有股票评论'''

import requests
from UA import agents
import time
import random
from multiprocessing import Pool
import threading
import sys
from db import StockMongo
import numpy as np
from proxies import proxies
from bs4 import BeautifulSoup as Soup
import urllib


'''爬虫的第一步，要先爬取的股票代码'''

headers={
    'User-Agent':random.choice(agents),
}

snopes_url='https://www.snopes.com/'
factcheck_url='https://www.snopes.com/whats-new/page/{page_num}/'

def write_url_to_txt(urls):
    with open("url_list_night.txt", 'a') as f:
        f.write('\n'.join(urls))

def write_failed_url_to_txt(url):
    with open("failed_url_list.txt", 'a') as f:
        f.write(url+'\n')
def write_source_to_txt(keywords, source):
    with open("source_list.txt", 'a') as fw:
        fw.write(keywords+':'+source+'\n')

def write_event_to_txt(keywords, label):
    with open("events_list_night.txt", 'a') as fw:
        fw.write(keywords+':'+label+'\n')

def get_url(num):#默认是不使用dialing
    # evnets_database=StockMongo('snopes','url_list')
    url=factcheck_url.format(page_num=str(num))#股票列表URL
    print("start the thread %d to get url"%num)
    cnt = 0
    while True:
        session = requests.session()
        idx = np.random.randint(len(proxies))
        proxy = '%s:%s'%(proxies[idx]['ip'], proxies[idx]['port'])

        if proxies[idx]["type"] == "https":
            thisproxies = {'https': 'https://{}'.format(proxy)}
        else:
            thisproxies = {'http': 'http://{}'.format(proxy)}

        session.proxies = thisproxies  # 携带代理
        try:
            html = session.get(url=snopes_url, headers=headers)
            print(html)
            html = session.get(url, headers=headers)
            if html.status_code == 200:
                contents = Soup(html.text, 'html.parser').select('article[class=media-wrapper]')
                urls = list( map(lambda div: div.a['href'], contents) )
                global lock
                lock.acquire()
                write_url_to_txt(urls)
                lock.release()
                print("--page (%d) write_finished--"%num)
                break
            elif html.status_code ==404:
                print(url)
                break
        except:
            print('获取失败，准备重新获取')#失败后要
            cnt += 1
            if cnt == 20:
                break
            time.sleep(2)
            continue

def get_data(url):
    print("start the thread to get url:%s" % url)
    while True:
        session = requests.session()
        # proxy = requests.get('http://localhost:5000/get').text  # 获取本地代理池代理
        idx = np.random.randint(len(proxies))
        proxy = '%s:%s'%(proxies[idx]['ip'], proxies[idx]['port'])

        if proxies[idx]["type"] == "https":
            thisproxies = {'https': 'https://{}'.format(proxy)}
        else:
            thisproxies = {'http': 'http://{}'.format(proxy)}

        session.proxies = thisproxies  # 携带代理
        try:
            html = session.get(url, headers=headers)
            print(html)
            if html.status_code == 200:
                label = Soup(html.text, 'html.parser').select('div[class=media-body]')[0].h5.text
                keywords = url.split('/')[-2]
                global lock
                lock.acquire()
                write_event_to_txt(keywords, label)
                lock.release()
                print("--url (%s) write_finished--"%url)
                break
            elif html.status_code ==404:
                print(url)
                break
        except:
            print('获取失败，准备重新获取')#失败后要
            time.sleep(2)
            continue

def TwitterSource(blockquotes):
    print("--------def TwitterSource(blockquotes)--------")
    flag = False
    source = ''
    for quote in blockquotes:
        try:
            classType = quote['class']
        except KeyError:
            continue
        else:
            if classType[0] == 'twitter-tweet':
                flag = True
                source = quote.select('p')[-1].a['href']
                break
    return flag, source

def FacebookSource(content):
    print("-------def FacebookSource(content)-------")
    flag = False
    source = ''
    if content.iframe is not None and content.iframe['src'].find("facebook.com") != -1:
        print("------ If Loop --------:", content.iframe['src'])
        flag = True
        src = content.iframe['src'].split('href=')[1].split('&width')[0]
        print("----- src in Facebook---:", src)
        source = urllib.parse.unquote(src)
        print("----- source in Facebook -----:", source)
    print("------- before return in FacebookSource------")
    return flag, source

def ExtractThePage(html):
    print("------ ExtractThePage-----------")
    soup = Soup(html.text, 'html.parser')
    content = soup.select('div[class=content]')[0]
    flag, source = TwitterSource(content.select('blockquote'))
    if not flag:
        flag, source = FacebookSource(content)
    print("return flag and source")
    return flag, source

def ConstructSession():
    session = requests.session()
    # proxy = requests.get('http://localhost:5000/get').text  # 获取本地代理池代理
    idx = np.random.randint(len(proxies))
    proxy = '%s:%s' % (proxies[idx]['ip'], proxies[idx]['port'])

    if proxies[idx]["type"] == "https":
        thisproxies = {'https': 'https://{}'.format(proxy)}
    else:
        thisproxies = {'http': 'http://{}'.format(proxy)}
    session.proxies = thisproxies  # 携带代理
    return session

def WriteFile(flag, keywords, source, url):
    global lock
    lock.acquire()
    if flag:
        write_source_to_txt(keywords, source)
    else:
        write_failed_url_to_txt(url)
    lock.release()
    print("--url (%s) write_finished--" % url)

def get_source(url):
    print("start the thread to get source:%s" % url)
    while True:
        session = ConstructSession()
        try:
            html = session.get(url, headers=headers)
            print(html)
        except:
            print('获取失败，准备重新获取(%s)'%url)  # 失败后要
            time.sleep(2)
            continue

        if html.status_code == 200:
            keywords = url.split('/')[-2]
            print("-----200 type handler-----")
            flag, source = ExtractThePage(html)
            # print("-----200 type handler-----\n" + flag + ':' + source)
            WriteFile(flag, keywords, source, url)
            break
        elif html.status_code ==404:
            print('------404 type handler -------'+url)
            break


if __name__ =='__main__':
    lock = threading.Lock()
    # pool =Pool(6)
    #
    # for i in range(100, 1000):
    #     pool.apply_async(func=get_url,args=(i,))
    #
    # pool.close()
    # pool.join()  # 必须等待所有子进程结束



    source_kw = set( map(lambda line:line.split(':')[0], open("source_list.txt").readlines() ) )
    faied_kw = set( map(lambda line: line.split('/')[-2], open('failed_url_list.txt').readlines() ) )
    all_kw = set( map(lambda  line: line.split(':')[0], open('events_list_night.txt').readlines() ) )
    rest_kw = all_kw - source_kw - faied_kw

    pool1 = Pool(6)

    for kw in rest_kw:
        url = 'https://www.snopes.com/fact-check/%s/'%kw
        pool1.apply_async(func=get_source, args=(url,))

    pool1.close()
    pool1.join()#必须等待所有子进程结束



