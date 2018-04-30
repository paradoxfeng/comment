from pymongo import errors,MongoClient
from datetime import datetime,timedelta


class Mongoqueue(object):#用于实现分布式进程的mongodb队列，可以实现对爬取失败的url进行重新抓取
    OUTSTANDING = 1#等待状态
    PROCESSING = 2#处理状态
    COMPLETE = 3#完成状态
    ERROR = 4#失败状态

    def __init__(self,db,table,timeout=300):
        self.client = MongoClient('localhost')
        self.db = self.client[db]
        self.table = self.db[table]
        self.timeout = timeout

    def __bool__(self):#整个类的bool值，为真则整个类为真
        result = self.table.find_one({'status':{'$ne':self.COMPLETE}})
        if result:
            return True
        else:
            return False


    def push(self,url):
        try:
            self.table.insert({'url':url,'status':self.OUTSTANDING})
            print('插入到队列成功',url)
        except errors.DuplicateKeyError as e:
            print('url已存在',url)

    def push_comment(self,comment):
        try:
            self.table.insert({'comment':comment})
            print('存储评论成功',comment)
        except Exception as e:
            print(e)

    def pop(self):
        result = self.table.find_and_modify(query={'status':self.OUTSTANDING},update={'$set':{'status':self.PROCESSING,'timestamp':datetime.now()}})
        if result:
            return result['url']
        else:
            raise KeyError

    def repair(self):#对请求超时的url进行状态重置
        result = self.table.find_and_modify(query={'status':{'$ne':self.COMPLETE},'timestamp':{'$lt':datetime.now()-timedelta(seconds=self.timeout)}},
                                   update={'$set':{'status':self.OUTSTANDING}})
        if result:
            print('重置url为等待状态',result['url'])

    def complete(self,url):
        self.table.update({'url':url},{'$set':{'status':self.COMPLETE}})

    def set_error(self,url):
        self.table.update({'url':url},{'$set':{'status':self.ERROR}})

    def set(self):
        cursor = self.table.find()
        for x in cursor:
            url = x['url']
            print(url)
            self.table.update({'url':url},{'$set':{'status':self.OUTSTANDING}})
















