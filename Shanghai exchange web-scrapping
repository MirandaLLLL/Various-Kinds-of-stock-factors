import requests

from bs4 import BeautifulSoup

import time

from urllib.parse import urlencode

import re

import json

import csv




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback29146',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2020',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback29146',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2020',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2020_Q2.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback40273',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2020',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback40273',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2020',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2020_Q1.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback15845',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L011',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback15845',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L011',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2019_Q4.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback76678',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L014',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback76678',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L014',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2019_Q3.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback74853',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback74853',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2019_Q2.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback4111',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback4111',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2019',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2019_Q1.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback1676',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L011',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback1676',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L011',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2018_Q4.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback86156',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L014',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback86156',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L014',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2018_Q3.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback75895',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback75895',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L012',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2018_Q2.csv','w')as f:

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




csvheaders = ['S_INFO_WINDCODE','COMPANY_NAME','STATEMENT_TYPE','REPORT_YEAR','publishDate0','publishDate1','publishDate2','publishDate3','actualDate']

'''

rows = [

        [1,'xiaoming','male',168,23],

        [1,'xiaohong','female',162,22],

        [2,'xiaozhang','female',163,21],

        [2,'xiaoli','male',158,21]

    ]

'''




url = 'http://query.sse.com.cn/infodisplay/queryBltnBookInfo.do?'

headers = {

    'Referer': 'http://www.sse.com.cn/disclosure/listedinfo/periodic/',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

}







def get_params(pagenum):

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback14990',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': pagenum,

        'pageHelp.beginPage': pagenum,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 10*pagenum+1,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param

def get_params1():

    _timestamp = int(time.time() * 1000)

    data = {

        'jsonCallBack': 'jsonpCallback14990',

        'isPagination': 'false',

        'isNew': '1',

        'bulletintype': 'L013',

        'publishYear': '2018',

        'cmpCode': '',

        'startTime': '',

        'sortName': 'companyCode',

        'direction': 'asc',

        'pageHelp.pageSize': 25,

        'pageHelp.pageCount': 50,

        'pageHelp.pageNo': 1,

        'pageHelp.beginPage': 1,

        'pageHelp.cacheSize': 1,

        'pageHelp.endPage': 5,

        "_": _timestamp,  # 当前的一个时间戳

    }

    param = urlencode(data)

    return param



rows=[]

r = requests.get(url + get_params1(), headers=headers)

rt=r.text

ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

diclist=re.findall('\{(.*?)\}',ret)

for d in diclist:

        #print(d)

    data=json.loads('{'+d+'}')

        #print(data)

    row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

    rows.append(row)


for page in range(2,68):

    r = requests.get(url + get_params(page), headers=headers)

    rt=r.text

    ret=re.findall('\"data\":\[.*\]',rt)[0][8:-1]

    diclist=re.findall('\{(.*?)\}',ret)

    for d in diclist:

        #print(d)

        data=json.loads('{'+d+'}')

        #print(data)

        row=[data['companyCode'],data['companyAbbr'],data['bulletinType'],data['publishYear'],data['publishDate0'],data['publishDate1'],data['publishDate2'],data['publishDate3'],data['actualDate']]

        rows.append(row)

    #print(len(diclist))




with open('D:/Reservation2018_Q1.csv','w')as f:

    f_csv = csv.writer(f)

    f_csv.writerow(csvheaders)

    f_csv.writerows(rows)
