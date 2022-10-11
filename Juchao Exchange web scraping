import requests

from bs4 import BeautifulSoup

import time

from urllib.parse import urlencode

import re

import json

import csv





csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','publishDate0','publishDate1','publishDate2','publishDate3','actualDate','Quanter','pop']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''





url = 'http://www.cninfo.com.cn/new/information/getPrbookInfo'

headers = {

    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=data/yypl',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}










def post_params(page):


    data = {

        'sectionTime': '2020-06-30',
        'firstTime': '',
        'lastTime': '',
        'market': 'szsh',
        'stockCode':'', 
        'orderClos': '',
        'isDesc': '',
        'pagesize': 20,
        'pagenum': page,

    }
    #fromData = urlencode(data).encode('utf-8')
    #param = urlencode(data)
    return data




rows=[]


for page in range(1,198):
    r = requests.post(url,data=post_params(page))
    rt=r.text
    ret=re.findall('\"prbookinfos\":\[.*\]',rt)[0][15:-1]
    diclist=re.findall('\{(.*?)\}',ret)
    for d in diclist:

        data=json.loads('{'+d+'}')

        row=[data['seccode'],data['secname'],data['f002d_0102'],data['f006d_0102'],data['f003d_0102'],data['f004d_0102'],data['f005d_0102'],data['f001d_0102'],data['orgId']]

        rows.append(row)


with open('D:/szsh2020-06-30.csv','w')as f:

    f_csv = csv.writer(f)

    f_csv.writerow(csvheaders)

    f_csv.writerows(rows)

import requests

from bs4 import BeautifulSoup

import time

from urllib.parse import urlencode

import re

import json

import csv





csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','publishDate0','publishDate1','publishDate2','publishDate3','actualDate','Quanter','pop']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''





url = 'http://www.cninfo.com.cn/new/information/getPrbookInfo'

headers = {

    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=data/yypl',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}










def post_params(page):


    data = {

        'sectionTime': '2020-03-31',
        'firstTime': '',
        'lastTime': '',
        'market': 'szsh',
        'stockCode':'', 
        'orderClos': '',
        'isDesc': '',
        'pagesize': 20,
        'pagenum': page,

    }
    #fromData = urlencode(data).encode('utf-8')
    #param = urlencode(data)
    return data




rows=[]


for page in range(1,193):
    r = requests.post(url,data=post_params(page))
    rt=r.text
    ret=re.findall('\"prbookinfos\":\[.*\]',rt)[0][15:-1]
    diclist=re.findall('\{(.*?)\}',ret)
    for d in diclist:

        data=json.loads('{'+d+'}')

        row=[data['seccode'],data['secname'],data['f002d_0102'],data['f006d_0102'],data['f003d_0102'],data['f004d_0102'],data['f005d_0102'],data['f001d_0102'],data['orgId']]

        rows.append(row)



with open('D:/szsh2020-03-31.csv','w')as f:

    f_csv = csv.writer(f)

    f_csv.writerow(csvheaders)

    f_csv.writerows(rows)



import requests

from bs4 import BeautifulSoup

import time

from urllib.parse import urlencode

import re

import json

import csv





csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','publishDate0','publishDate1','publishDate2','publishDate3','actualDate','Quanter','pop']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''





url = 'http://www.cninfo.com.cn/new/information/getPrbookInfo'

headers = {

    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=data/yypl',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}










def post_params(page):


    data = {

        'sectionTime': '2019-12-31',
        'firstTime': '',
        'lastTime': '',
        'market': 'szsh',
        'stockCode':'', 
        'orderClos': '',
        'isDesc': '',
        'pagesize': 20,
        'pagenum': page,

    }
    #fromData = urlencode(data).encode('utf-8')
    #param = urlencode(data)
    return data




rows=[]


for page in range(1,192):
    r = requests.post(url,data=post_params(page))
    rt=r.text
    ret=re.findall('\"prbookinfos\":\[.*\]',rt)[0][15:-1]
    diclist=re.findall('\{(.*?)\}',ret)
    for d in diclist:

        data=json.loads('{'+d+'}')

        row=[data['seccode'],data['secname'],data['f002d_0102'],data['f006d_0102'],data['f003d_0102'],data['f004d_0102'],data['f005d_0102'],data['f001d_0102'],data['orgId']]

        rows.append(row)



with open('D:/szsh2019-12-31.csv','w')as f:

    f_csv = csv.writer(f)

    f_csv.writerow(csvheaders)

    f_csv.writerows(rows)



import requests

from bs4 import BeautifulSoup

import time

from urllib.parse import urlencode

import re

import json

import csv





csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','publishDate0','publishDate1','publishDate2','publishDate3','actualDate','Quanter','pop']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''





url = 'http://www.cninfo.com.cn/new/information/getPrbookInfo'

headers = {

    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=data/yypl',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}










def post_params(page):


    data = {

        'sectionTime': '2019-09-30',
        'firstTime': '',
        'lastTime': '',
        'market': 'szsh',
        'stockCode':'', 
        'orderClos': '',
        'isDesc': '',
        'pagesize': 20,
        'pagenum': page,

    }
    #fromData = urlencode(data).encode('utf-8')
    #param = urlencode(data)
    return data




rows=[]


for page in range(1,186):
    r = requests.post(url,data=post_params(page))
    rt=r.text
    ret=re.findall('\"prbookinfos\":\[.*\]',rt)[0][15:-1]
    diclist=re.findall('\{(.*?)\}',ret)
    for d in diclist:

        data=json.loads('{'+d+'}')

        row=[data['seccode'],data['secname'],data['f002d_0102'],data['f006d_0102'],data['f003d_0102'],data['f004d_0102'],data['f005d_0102'],data['f001d_0102'],data['orgId']]

        rows.append(row)



with open('D:/szsh2019-09-30.csv','w')as f:

    f_csv = csv.writer(f)

    f_csv.writerow(csvheaders)

    f_csv.writerows(rows)
